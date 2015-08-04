# CQLSL

[![cqlsl build status](https://travis-ci.org/drudim/cqlsl.svg?branch=master)](https://travis-ci.org/drudim/cqlsl)
[![cqlsl pypi version](http://img.shields.io/pypi/v/cqlsl.svg)](https://pypi.python.org/pypi/cqlsl)


CQL for Python without any additional abstraction layers. Designed to be simple and fast tool for communication with Cassandra. CQLSL is something between native Cassandra driver (which is really awesome!) and ORM.

## Why SQLSL?
First of all: not all applications need to use ORM. Second reason: I hate ORMs that use Active Record Pattern. Especially, when they try to be similar to Django ORM syntax. Usually, we lost a lot of useful features of chosen storage, when we try to build own abstraction layer on top of it.

## Installation
```
pip install cqlsl
```

## Getting Started
First of all you need to define connection settings to your Cassandra cluster (For advanced configuration, please, read [official documentation for python cassandra driver](https://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster)):
```python
from cassandra.cluster import Cluster
cluster = Cluster(['localhost'])
```
Then you need to define session with this cluster:
```python
from cqlsl.sessions import Session
session = Session(cluster, keyspace='myapp')
```
And you ready to perform queries:
```python
from cqlsl.statements import *
session.execute(
  insert('books').values(id=1, title='Domain-driven Design', author='Eric Evans', year=2004)
)
session.execute(
  update('books').set(title='Domain-driven Design: Tackling Complexity...').where(id=1)
)
book_title = session.execute(
  select('books').fields('title').where(id=1)
)[0]['title']
session.execute(
  delete('books').fields('year').where(id=1)
)
```

## Documentation
### Sessions
Under construction...
### Statements
Under construction...

## Change Log

### v0.2.0 (2015/08/05 00:15 +00:00)
- [#5](https://github.com/drudim/cqlsl/pull/3) Add Python 3.4.x support (@mantzouratos @drudim)

### v0.1.2 (2014/12/18 20:56 +00:00)
- [#3](https://github.com/drudim/cqlsl/pull/3) Add ORDER BY and ALLOW FILTERING options (@drudim)
- [#2](https://github.com/drudim/cqlsl/pull/2) Add support for comparisons in WHERE clause (@drudim)
- [#1](https://github.com/drudim/cqlsl/pull/1) Add support for several conditions in WHERE clause (@drudim)
