from cqlsl.sessions import SyncSession
from tests.base import BaseTestCase


__all__ = ['BaseIntegrationTestCase']


class BaseIntegrationTestCase(BaseTestCase):
    session = SyncSession()

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
