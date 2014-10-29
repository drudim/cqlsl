from cqlsl.sessions import SyncSession
from cqlsl.statements import update
from tests.base import CqlslTestCase


class UpdatesTest(CqlslTestCase):
    def setUp(self):
        self.session = SyncSession(keyspace='cqlsl')
        self.session.execute(
            '''
            CREATE TABLE IF NOT EXISTS update_stmt_test (
                test_id int,
                test_text text,
                test_set set<text>,
                test_map map<text,int>,
                test_list list<text>,
                PRIMARY KEY (test_id)
            )
            '''
        )

    def tearDown(self):
        self.session.execute('DROP TABLE update_stmt_test')

    def test_update(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_text) VALUES (1, 'some text')")
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_text) VALUES (2, 'another text')")

        self.assertItemsEqual(
            ('some text', 'another text'),
            map(lambda x: x.get('test_text'), self.session.execute('SELECT test_text FROM update_stmt_test')),
        )
        self.session.execute(update('update_stmt_test').set(test_text='updated text').where(test_id__in=(1,2)))
        self.assertItemsEqual(
            ('updated text', 'updated text'),
            map(lambda x: x.get('test_text'), self.session.execute('SELECT test_text FROM update_stmt_test')),
        )

    def test_update_with_where(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_text) VALUES (1, 'some text')")
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_text) VALUES (2, 'another text')")

        self.assertItemsEqual(
            ('some text', 'another text'),
            map(lambda x: x.get('test_text'), self.session.execute('SELECT test_text FROM update_stmt_test')),
        )
        self.session.execute(update('update_stmt_test').set(test_text='updated text').where(test_id=1))
        self.assertItemsEqual(
            ('updated text', 'another text'),
            map(lambda x: x.get('test_text'), self.session.execute('SELECT test_text FROM update_stmt_test')),
        )

    def test_update_set_add(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_set) VALUES (1, {'a'})")

        self.assertEqual({'a'}, self.session.execute('SELECT test_set FROM update_stmt_test')[0].get('test_set'))
        self.session.execute(update('update_stmt_test').set(test_set__add={'b'}).where(test_id=1))
        self.assertEqual(
            {'a', 'b'}, self.session.execute('SELECT test_set FROM update_stmt_test')[0].get('test_set')
        )

    def test_update_set_remove(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_set) VALUES (1, {'a', 'b'})")

        self.assertEqual({'a', 'b'}, self.session.execute('SELECT test_set FROM update_stmt_test')[0].get('test_set'))
        self.session.execute(update('update_stmt_test').set(test_set__remove={'a'}).where(test_id=1))
        self.assertEqual({'b'}, self.session.execute('SELECT test_set FROM update_stmt_test')[0].get('test_set'))

    def test_update_dict_update(self):
        self.session.execute(
            "INSERT INTO update_stmt_test (test_id, test_map) VALUES (1, {'a': 1, 'b': 2, 'c': 3})"
        )

        self.assertEqual(
            {'a': 1, 'b': 2, 'c': 3},
            self.session.execute('SELECT test_map FROM update_stmt_test')[0].get('test_map')
        )
        self.session.execute(update('update_stmt_test').set(test_map__update={'a': 4, 'b': 5}).where(test_id=1))
        self.assertEqual(
            {'a': 4, 'b': 5, 'c': 3},
            self.session.execute('SELECT test_map FROM update_stmt_test')[0].get('test_map')
        )

    def test_update_list_prepend(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a'])")

        self.assertEqual(['a'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list'))
        self.session.execute(update('update_stmt_test').set(test_list__prepend=['b']).where(test_id=1))
        self.assertEqual(
            ['b', 'a'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )

    def test_update_list_append(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a'])")

        self.assertEqual(['a'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list'))
        self.session.execute(update('update_stmt_test').set(test_list__append=['b']).where(test_id=1))
        self.assertEqual(
            ['a', 'b'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )

    def test_update_list_insert(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a', 'b'])")

        self.assertEqual(
            ['a', 'b'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )
        self.session.execute(update('update_stmt_test').set(test_list__insert__1='c').where(test_id=1))
        self.assertEqual(
            ['a', 'c'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )

    def test_update_list_remove(self):
        self.session.execute("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a', 'b', 'a'])")

        self.assertEqual(
            ['a', 'b', 'a'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )
        self.session.execute(update('update_stmt_test').set(test_list__remove=['a']).where(test_id=1))
        self.assertEqual(
            ['b'], self.session.execute('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )
