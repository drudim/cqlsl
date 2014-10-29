import logging
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cqlsl.statements.base import BaseStatement


__all__ = ['SyncSession']

log = logging.getLogger('cqlsl.queries')


class SyncSession(object):
    def __init__(self, keyspace='default'):
        self.session = Cluster(['localhost']).connect(keyspace)
        self.session.row_factory = dict_factory

    def execute(self, query, context=None):
        if isinstance(query, BaseStatement):
            query, context = query.query, query.context

        log.debug('Executing {} with next context: {}'.format(query, context))

        return self.session.execute(query, context)
