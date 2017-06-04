#!/usr/bin/env python
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='impselect',
    version='0.1.0',

    description='Batch data selection tool from Impala.',
    long_description=long_description,

    url='https://github.com/genichyar/impselect',

    author='genichyar',
    author_email='genichyar@genichyar.com',

    license='MIT',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
    ],

    keywords='impala',
    packages=find_packages(),
    install_requires=['pandas', 'impyla'],
)
