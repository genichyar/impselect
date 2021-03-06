from impala.util import as_pandas
from impala.dbapi import connect
from impala.error import RPCError
import json
import pandas as pd
import tempfile
import errno
import os
from time import sleep


def read_config():
    config = None
    for loc in os.curdir, os.path.expanduser('~'):
        try:
            with open(os.path.join(loc, '.impselect.txt')) as source:
                config = json.load(source)
        except IOError:
            pass

    return config


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python > 2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def prepare_filename(filename):
    return filename.replace(' ', '_').replace(':', '_')


class Impala(object):

    def __init__(self, name, connection=None, dir=None, verbose=None, try_except=None):
        self.__name = name
        config = read_config()
        if connection is not None:
            self.connection = connection
        elif config and ('connection' in config):
            self.connection = config['connection']
        else:
            raise ValueError('Connection is not defined.')

        if dir is not None:
            self.__dir = dir
        elif config and ('tmpdir' in config):
            self.__dir = os.path.join(config['tmpdir'], self.__name)
        else:
            self.__dir = os.path.join(tempfile.gettempdir(), 'impselect', self.__name)

        mkdir_p(self.__dir)

        if verbose is not None:
            self.verbose = verbose
        elif config and ('verbose' in config):
            self.verbose = config['verbose']
        else:
            self.verbose = 1

        if try_except is not None:
            self.try_except = try_except
        elif config and ('try_except' in config):
            self.try_except = config['try_except']
        else:
            self.try_except = {'timeout': 10, 'count': 10}

        self.__tables = []

    @property
    def name(self):
        return self.__name

    @property
    def dir(self):
        return self.__dir

    def __execute(self, sql, ret='pandas'):
        ret_val = None
        with connect(**self.connection) as con:
            cur = con.cursor()
            cur.execute(sql)
            if ret == 'pandas':
                ret_val = as_pandas(cur)
            elif ret == 'status':
                ret_val = cur.status()

        return ret_val

    def create_table(self, sql, table_name):
        sql = 'CREATE TABLE {table_name} AS ({sql})'.format(table_name=table_name, sql=sql)
        result = self.__execute(sql)
        if self.verbose >= 1:
            print(result)
        return result

    def drop_table(self, table_name, purge=True):
        sql = 'DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name)
        if purge:
            sql += ' PURGE'
        result = self.__execute(sql, 'status')
        if self.verbose >= 1:
            print(result)
        return result

    def describe_table(self, table_name):
        sql = 'DESCRIBE {table_name}'.format(table_name=table_name)
        result = self.__execute(sql)
        return result

    def get_file_path(self, name):
        return os.path.join(self.__dir, name)

    def get_csv_path(self, name):
        return os.path.join(self.__dir, prepare_filename(name) + '.csv.gz')

    def get_batch_csv_path(self, name, itervar):
        return os.path.join(self.__dir, prepare_filename(name + '_' + str(itervar)) + '.csv.gz')

    def load(self, name, csv_options=None):
        if csv_options is None:
            csv_options = {}
        options = {'compression': 'gzip', 'encoding': 'utf-8'}
        options.update(csv_options)
        return pd.read_csv(self.get_csv_path(name), **options)

    def save(self, df, name, csv_options=None):
        if csv_options is None:
            csv_options = {}
        options = {'compression': 'gzip', 'index': False, 'encoding': 'utf-8'}
        options.update(csv_options)
        df.to_csv(self.get_csv_path(name), **options)

    def select(self, sql, name=None, table_name=None, csv_options=None):
        if name:
            if os.path.isfile(self.get_csv_path(name)):
                if self.verbose >= 1:
                    print('Data for task "' + name + '" is exists. Passed.')
                return self.load(name)

        if table_name:
            self.drop_table(table_name)
            self.create_table(sql, table_name)

            sql_select = 'SELECT * FROM {table_name}'.format(table_name=table_name)
            df = self.__execute(sql_select)
        else:
            df = self.__execute(sql)

        if name:
            self.save(df, name, csv_options)
            if self.verbose >= 1:
                print('Data for task "' + name + '" has written.')

        return df

    def prepare(self, sql, name):
        self.select(sql=sql, name=name)

    def prepare_batch(self, sql, itervars, name, csv_options=None):
        for itervar in itervars:
            batch_file_path = self.get_batch_csv_path(name, itervar)
            if os.path.isfile(batch_file_path):
                if self.verbose >= 1:
                    print('Data for task "' + name + '" and itervar "' + str(itervar) + '" is exists. Passed.')
                continue
            df = None
            if self.try_except:
                for i in range(0, self.try_except['count']):
                    try:
                        df = self.__execute(sql.format(itervar=itervar))
                        break
                    except RPCError as e:
                        print(str(e))
                        print('Exception was raised during selection. Retry.')
                        if i + 1 == self.try_except['count']:
                            raise
                        sleep(self.try_except['timeout'])
            else:
                df = self.__execute(sql.format(itervar=itervar))

            if csv_options is None:
                csv_options = {}
            options = {'compression': 'gzip', 'index': False, 'encoding': 'utf-8'}
            options.update(csv_options)

            df.to_csv(batch_file_path, **options)
            if self.verbose >= 1:
                print('Data for task "' + name + '" and itervar "' + str(itervar) + '" has written.')

    def load_batch(self, itervars, name, itervar_column=None, transform=None, csv_options=None):
        if csv_options is None:
            csv_options = {}
        options = {'compression': 'gzip', 'encoding': 'utf-8'}
        options.update(csv_options)

        tmp_df = []
        for itervar in itervars:
            batch_file_path = self.get_batch_csv_path(name, itervar)
            tmp_df.append(pd.read_csv(batch_file_path, **options))
            if itervar_column:
                tmp_df[-1][itervar_column] = str(itervar)
            if callable(transform):
                tmp_df[-1] = transform(tmp_df[-1])
        df = pd.concat(tmp_df, ignore_index=True, copy=False)

        return df

    def select_batch(self, sql, itervars, name, itervar_column=None, csv_options=None):
        self.prepare_batch(sql, itervars, name, csv_options)
        return self.load_batch(itervars, name, itervar_column, csv_options=csv_options)
