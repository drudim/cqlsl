import logging
from cassandra.query import dict_factory
from cqlsl.statements.base import BaseStatement


__all__ = ['Session']

log = logging.getLogger('cqlsl.queries')


class Session(object):
    def __init__(self, cluster, keyspace=None):
        self.cluster = cluster
        self.keyspace = keyspace
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = self.cluster.connect(self.keyspace)
            self._session.row_factory = dict_factory
        return self._session

    def execute(self, query, context=None):
        if isinstance(query, BaseStatement):
            query, context = query.query, query.context
        return self.session.execute(query, context)


class AsyncSession(Session):
    def execute(self, query, context=None):
        if isinstance(query, BaseStatement):
            query, context = query.query, query.context
        return self.session.execute_async(query, context)
