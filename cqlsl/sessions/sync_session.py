from cassandra.cluster import Cluster
from cassandra.query import dict_factory


__all__ = ['SyncSession']


class SyncSession(object):
    def __init__(self, keyspace='default'):
        self.session = Cluster(['localhost']).connect(keyspace)
        self.session.row_factory = dict_factory

    def execute(self, statement):
        return self.session.execute(statement.query, statement.context)

    def execute_raw(self, raw_query):
        return self.session.execute(raw_query)
