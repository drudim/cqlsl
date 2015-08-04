import os
from cassandra.cluster import Cluster
from cqlsl.sessions import Session
from ..base import BaseTestCase


__all__ = ['BaseIntegrationTestCase']


class BaseIntegrationTestCase(BaseTestCase):
    session = Session(
        Cluster([os.environ.get('CQLSL_TEST_CLUSTER', 'localhost')], protocol_version=2)
    )

    @classmethod
    def setUpClass(cls):
        cls.session.execute(
            '''
            CREATE KEYSPACE IF NOT EXISTS cqlsl_test WITH REPLICATION={
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
            '''
        )
        cls.session.execute('USE cqlsl_test')

    @classmethod
    def tearDownClass(cls):
        cls.session.execute('DROP KEYSPACE cqlsl_test')
