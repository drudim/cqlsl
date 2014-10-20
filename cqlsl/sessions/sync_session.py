from cassandra.cluster import Cluster


__all__ = ['SyncSession']


class SyncSession(object):
    def __init__(self, keyspace='default'):
        self.session = Cluster(['localhost']).connect(keyspace)

    def execute(self, statement):
        return self.session.execute(statement.query, statement.context)

    def execute_raw(self, raw_query):
        return self.session.execute(raw_query)
