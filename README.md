# CQLSL
[![Build Status](https://travis-ci.org/drudim/cqlsl.svg?branch=master)](https://travis-ci.org/drudim/cqlsl)

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
