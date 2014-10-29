from cqlsl.sessions import SyncSession
from cqlsl.statements import select
from tests.base import CqlslTestCase


__all__ = ['SelectsTest']


class SelectsTest(CqlslTestCase):
    def setUp(self):
        self.session = SyncSession(keyspace='cqlsl')
        self.session.execute(
            '''
            CREATE TABLE IF NOT EXISTS select_stmt_test (
                test_id int,
                test_text text,
                test_int int,
                test_float float,
                test_map map<text,int>,
                test_list list<text>,
                test_set set<int>,
                PRIMARY KEY (test_id)
            )
            '''
        )

    def tearDown(self):
        self.session.execute('DROP TABLE select_stmt_test')

    def assertTypeRestored(self, column, expected_value, special_assert=None):
        self.session.execute(
            'INSERT INTO select_stmt_test (test_id, {}) VALUES (%s, %s)'.format(column), (1, expected_value,)
        )
        rows = self.session.execute(select('select_stmt_test').fields(column))
        self.assertEqual(1, len(rows))
        self.assertEqual(1, len(rows[0].keys()))

        restored_value = rows[0].values()[0]

        if special_assert:
            special_assert(expected_value, restored_value)
        else:
            self.assertEqual(expected_value, restored_value)

    def test_select_text(self):
        self.assertTypeRestored('test_text', u'Some test text')

    def test_select_int(self):
        self.assertTypeRestored('test_int', 123)

    def test_select_float(self):
        special_assert = lambda x, y: self.assertAlmostEqual(x, y, 3)
        self.assertTypeRestored('test_float', 4.567, special_assert)

    def test_select_map(self):
        self.assertTypeRestored('test_map', {'a': 1, 'b': 2})

    def test_select_list(self):
        self.assertTypeRestored('test_list', ['a', 'b', 'c'])

    def test_select_set(self):
        self.assertTypeRestored('test_set', {1, 2, 3})

    def test_select_all(self):
        self.session.execute("INSERT INTO select_stmt_test (test_id, test_text, test_int) VALUES (1, 'some text', 5)")

        results = self.session.execute(select('select_stmt_test'))
        self.assertEqual(1, len(results))

        self.assertEqual(
            {
                'test_id': 1,
                'test_text': 'some text',
                'test_int': 5,
                'test_float': None,
                'test_list': None,
                'test_map': None,
                'test_set': None,
            },
            results[0]
        )
