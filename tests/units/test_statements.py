from unittest import TestCase
from cassandra.encoder import ValueSequence
from statements import insert, delete


__all__ = ['StatementsTest']


class StatementsTest(TestCase):
    def test_insert(self):
        stmt = insert('test_table').values(some_string='New title', some_number=1, some_numbers=[2, 3, 4])

        self.assertEqual(
            'INSERT INTO test_table (some_number, some_numbers, some_string) VALUES (%s, %s, %s)',
            stmt.query
        )
        self.assertEqual((1, ValueSequence([2, 3, 4]), 'New title'), stmt.context)

    def test_delete_fields(self):
        stmt = delete('test_table').fields('some_number', 'some_string')

        self.assertEqual('DELETE some_number, some_string FROM test_table', stmt.query)
        self.assertEqual((), stmt.context)

    def test_delete_with_where(self):
        stmt = delete('test_table').fields('some_number').where(some_id=1)

        self.assertEqual('DELETE some_number FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_map_item_delete(self):
        stmt = delete('test_table').fields('some_map__some_field')

        self.assertEqual('DELETE some_map[%s] FROM test_table', stmt.query)
        self.assertEqual(('some_field',), stmt.context)

    def test_map_multiple_item_delete(self):
        stmt = delete('test_table').fields(some_map__keys=('some_field', 'other_field'))

        self.assertEqual('DELETE some_map[%s], some_map[%s] FROM test_table', stmt.query)
        self.assertEqual(('some_field', 'other_field'), stmt.context)

    def test_list_item_delete(self):
        stmt = delete('test_table').fields(some_list__indexes=(3, 4))

        self.assertEqual('DELETE some_list[%s], some_list[%s] FROM test_table', stmt.query)
        self.assertEqual((3, 4), stmt.context)
