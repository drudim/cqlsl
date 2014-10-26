from unittest import TestCase
from sessions import SyncSession
from statements import update


class UpdatesTest(TestCase):
    def setUp(self):
        self.session = SyncSession(keyspace='cqlsl')
        self.session.execute_raw(
            '''
            CREATE TABLE update_stmt_test (
                test_id int,
                test_text text,
                test_int int,
                test_set set<text>,
                test_map map<text,int>,
                test_list list<text>,
                PRIMARY KEY (test_id)
            )
            '''
        )

    def tearDown(self):
        self.session.execute_raw('DROP TABLE update_stmt_test')

    def test_update(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_text) VALUES (1, 'some text')")
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_text) VALUES (2, 'another text')")

        self.assertEqual(
            ('some text', 'another text'),
            map(lambda x: x.get('test_text'), self.session.execute_raw('SELECT test_text FROM update_stmt_test')),
        )
        self.session.execute(update('update_stmt_test').set(test_text='updated text'))
        self.assertEqual(
            ('updated text', 'updated text'),
            map(lambda x: x.get('test_text'), self.session.execute_raw('SELECT test_text FROM update_stmt_test')),
        )

    def test_update_with_where(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_text) VALUES (1, 'some text')")
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_text) VALUES (2, 'another text')")

        self.assertEqual(
            ('some text', 'another text'),
            map(lambda x: x.get('test_text'), self.session.execute_raw('SELECT test_text FROM update_stmt_test')),
        )
        self.session.execute(update('update_stmt_test').set(test_text='updated text').where(test_id=1))
        self.assertItemsEqual(
            ('updated text', 'another text'),
            map(lambda x: x.get('test_text'), self.session.execute_raw('SELECT test_text FROM update_stmt_test')),
        )

    def test_update_counter_increment(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_text) VALUES (1, 5)")

        self.assertEqual(5, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_int'))
        self.session.execute(update('update_stmt_test').set(test_int__inc=2))
        self.assertEqual(7, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_int'))

    def test_update_counter_decrement(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_text) VALUES (1, 5)")

        self.assertEqual(5, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_int'))
        self.session.execute(update('update_stmt_test').set(test_int__dec=2))
        self.assertEqual(3, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_int'))

    def test_update_set_add(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_set) VALUES (1, {'a'})")

        self.assertEqual({'a'}, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_set'))
        self.session.execute(update('update_stmt_test').set(test_set__add={'b'}))
        self.assertEqual(
            {'a', 'b'}, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_set')
        )

    def test_update_set_remove(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_set) VALUES (1, {'a'})")

        self.assertEqual({'a'}, self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_set'))
        self.session.execute(update('update_stmt_test').set(test_set__remove={'b'}))
        self.assertEqual(set(), self.session.execute_raw('SELECT test_int FROM update_stmt_test')[0].get('test_set'))

    def test_update_dict_update(self):
        self.session.execute_raw(
            "INSERT INTO update_stmt_test (test_id, test_map) VALUES (1, {'a': 1, 'b': 2, 'c': 3})"
        )

        self.assertEqual(
            {'a': 1, 'b': 2, 'c': 3},
            self.session.execute_raw('SELECT test_map FROM update_stmt_test')[0].get('test_map')
        )
        self.session.execute(update('update_stmt_test').set(test_map__update={'a': 4, 'b': 5}))
        self.assertEqual(
            {'a': 4, 'b': 5, 'c': 3},
            self.session.execute_raw('SELECT test_map FROM update_stmt_test')[0].get('test_map')
        )

    def test_update_list_prepend(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a']})")

        self.assertEqual(['a'], self.session.execute_raw('SELECT test_map FROM update_stmt_test')[0].get('test_list'))
        self.session.execute(update('update_stmt_test').set(test_list__prepend=['b']))
        self.assertEqual(
            ['b', 'a'], self.session.execute_raw('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )

    def test_update_list_append(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a']})")

        self.assertEqual(['a'], self.session.execute_raw('SELECT test_list FROM update_stmt_test')[0].get('test_list'))
        self.session.execute(update('update_stmt_test').set(test_list__append=['b']))
        self.assertEqual(
            ['a', 'b'], self.session.execute_raw('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )

    def test_update_list_insert(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a', 'b']})")

        self.assertEqual(
            ['a', 'b'], self.session.execute_raw('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )
        self.session.execute(update('update_stmt_test').set(test_list__insert__1=['c']))
        self.assertEqual(
            ['a', 'c'], self.session.execute_raw('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )

    def test_update_list_remove(self):
        self.session.execute_raw("INSERT INTO update_stmt_test (test_id, test_list) VALUES (1, ['a', 'b', 'a']})")

        self.assertEqual(
            ['a', 'b', 'a'], self.session.execute_raw('SELECT test_map FROM update_stmt_test')[0].get('test_list')
        )
        self.session.execute(update('update_stmt_test').set(test_list__remove=['a']))
        self.assertEqual(
            ['b'], self.session.execute_raw('SELECT test_list FROM update_stmt_test')[0].get('test_list')
        )
