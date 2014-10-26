# coding=utf-8
from unittest import TestCase
from uuid import uuid4
from sessions import SyncSession
from statements import insert


__all__ = ['InsertsTest']


class InsertsTest(TestCase):
    def setUp(self):
        self.session = SyncSession(keyspace='cqlsl')
        self.session.execute_raw(
            '''
            CREATE TABLE insert_stmt_test (
                test_id uuid,
                test_text text,
                test_int int,
                test_float float,
                test_map map<text,int>,
                test_list list<text>,
                test_set set<float>,
                PRIMARY KEY (test_id)
            );
            '''
        )

    def tearDown(self):
        self.session.execute_raw('DROP TABLE insert_stmt_test')

    def assertTypeRestored(self, column, expected_value, special_assert=None):
        values = {column: expected_value}
        values['test_id'] = values.get('test_id', uuid4())
        self.session.execute(insert('insert_stmt_test').values(**values))

        restored_value = self.session.execute_raw('select {} from insert_stmt_test'.format(column))[0][column]

        if special_assert:
            special_assert(expected_value, restored_value)
        else:
            self.assertEqual(expected_value, restored_value)

    def test_insert_uuid(self):
        self.assertTypeRestored('test_id', uuid4())

    def test_insert_unicode_text(self):
        self.assertTypeRestored('test_text', u"⊙△⊙")

    def test_insert_str_text(self):
        self.assertTypeRestored('test_text', "simple text")

    def test_insert_int(self):
        self.assertTypeRestored('test_int', 123)

    def test_insert_float(self):
        special_assert = lambda x, y: self.assertAlmostEqual(x, y, 3)
        self.assertTypeRestored('test_float', 4.123, special_assert)

    def test_insert_map(self):
        self.assertTypeRestored('test_map', {'a': 1, 'b': 2})

    def test_insert_list(self):
        self.assertTypeRestored('test_list', ['a', 'b', 'c'])

    def test_insert_set(self):
        self.assertTypeRestored('test_set', {1, 2, 3})
