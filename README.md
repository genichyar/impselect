# Impselect

Impselect is simple module for data selection from Impala. It supports caching, batch select and creating temporary tables.

## Installation

```
pip install git+https://github.com/genichyar/impselect
```

For convenience you can create settings file `.impselect.txt` in user home directory. This file has JSON format.
```json
{
    "connection": {"host": "localhost", "database": "db_name"},
    "tmpdir": "C:\\impselect",
    "verbose": 1,
    "try_except": {"timeout": 10, "count": 10}
}
```

## Examples

```python
# First of all, we import library and create Impala object.
from impselect import Impala
i = Impala('your project name')

# Simple select.
df = i.select('SELECT * FROM users')

# Select with caching result to file.
df = i.select('SELECT * FROM users', 'task_name_1')

# Select with creating temporary table.
df = i.select('SELECT * FROM users', table_name='table_name') # Also you can use caching to file.

# Batch select.
df = i.select_batch('SELECT * FROM users WHERE day = unix_timestamp("{itervar}")',
                    ['2017-06-04', '2017-06-05'], 'task_name_2')
```