from unittest import TestCase
from sessions import SyncSession
from statements import delete


class DeletesTest(TestCase):
    def setUp(self):
        self.session = SyncSession(keyspace='cqlsl')
        self.session.execute_raw(
            '''
            CREATE TABLE delete_stmt_test (
                test_id int,
                test_text text,
                test_map map<text,int>,
                test_list list<text>,
                PRIMARY KEY (test_id)
            )
            '''
        )

    def tearDown(self):
        self.session.execute_raw('DROP TABLE delete_stmt_test')

    def test_delete_fields(self):
        self.session.execute_raw(
            "INSERT INTO delete_stmt_test (test_id, test_text, test_map) VALUES (1, 'test_1', {'a': 1})"
        )

        self.assertEqual({'a': 1}, self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_map'))
        self.assertEqual('test_1', self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_text'))

        self.session.execute(delete('delete_stmt_test').fields('test_map').where(test_id=1))

        self.assertIsNone(self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_map'))
        self.assertEqual('test_1', self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_text'))

    def test_delete_where(self):
        self.session.execute_raw("INSERT INTO delete_stmt_test (test_id, test_text) VALUES (1, 'test_1')")
        self.session.execute_raw("INSERT INTO delete_stmt_test (test_id, test_text) VALUES (2, 'test_2')")

        self.assertEqual(2, len(self.session.execute_raw('SELECT * FROM delete_stmt_test')))

        self.session.execute(delete('delete_stmt_test').where(test_id=1))

        self.assertEqual(1, len(self.session.execute_raw('SELECT * FROM delete_stmt_test')))
        self.assertEqual(2, self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_id'))

    def test_delete_map_items(self):
        self.session.execute_raw(
            "INSERT INTO delete_stmt_test (test_id, test_map) VALUES (1, {'a': 1, 'b': 2, 'c': 3})"
        )

        self.assertEqual(
            {'a': 1, 'b': 2, 'c': 3},
            self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_map')
        )

        self.session.execute(delete('delete_stmt_test').fields(test_map__keys=('b', 'c')).where(test_id=1))

        self.assertEqual(
            {'a': 1},
            self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_map')
        )

    def test_delete_list_item(self):
        self.session.execute_raw(
            "INSERT INTO delete_stmt_test (test_id, test_list) VALUES (1, ['a', 'b', 'c', 'd'])"
        )

        self.assertEqual(
            ['a', 'b', 'c', 'd'],
            self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_list')
        )

        self.session.execute(delete('delete_stmt_test').fields(test_list__indexes=(1, 3)).where(test_id=1))

        self.assertEqual(
            ['a', 'c'],
            self.session.execute_raw('SELECT * FROM delete_stmt_test')[0].get('test_list')
        )
