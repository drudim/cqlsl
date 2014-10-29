from cassandra.encoder import ValueSequence
from statements import insert, delete, update, select
from tests.base import CqlslTestCase


__all__ = ['StatementsTest']


class StatementsTest(CqlslTestCase):
    def test_insert(self):
        stmt = insert('test_table').values(some_string='New title', some_number=1, some_numbers=[2, 3, 4])

        self.assertEqual(
            'INSERT INTO test_table (some_number, some_numbers, some_string) VALUES (%s, %s, %s)',
            stmt.query
        )
        self.assertEqual((1, ValueSequence([2, 3, 4]), 'New title'), stmt.context)

    def test_select_all(self):
        stmt = select('test_table')

        self.assertEqual('SELECT * FROM test_table', stmt.query)
        self.assertEqual(tuple(), stmt.context)

    def test_select_fields(self):
        stmt = select('test_table').fields('some_number', 'some_string')

        self.assertEqual('SELECT some_number, some_string FROM test_table', stmt.query)
        self.assertEqual(tuple(), stmt.context)

    def test_select_with_where(self):
        stmt = select('test_table').where(test_id=1)

        self.assertEqual('SELECT * FROM test_table WHERE test_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_select_with_limit(self):
        stmt = select('test_table').limit(10)

        self.assertEqual('SELECT * FROM test_table LIMIT %s', stmt.query)
        self.assertEqual((10,), stmt.context)

    def test_delete(self):
        stmt = delete('test_table').where(some_id=1)

        self.assertEqual('DELETE  FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_delete_fields(self):
        stmt = delete('test_table').fields('some_number', 'some_string').where(some_id=1)

        self.assertEqual('DELETE some_number, some_string FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_delete_with_where(self):
        stmt = delete('test_table').fields('some_number').where(some_id=1)

        self.assertEqual('DELETE some_number FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_delete_map_item(self):
        stmt = delete('test_table').fields(some_map__keys=('some_field',)).where(some_id=1)

        self.assertEqual('DELETE some_map[%s] FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual(('some_field', 1), stmt.context)

    def test_delete_multiple_map_items(self):
        stmt = delete('test_table').fields(some_map__keys=('some_field', 'other_field')).where(some_id=1)

        self.assertEqual('DELETE some_map[%s], some_map[%s] FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual(('some_field', 'other_field', 1), stmt.context)

    def test_delete_list_item(self):
        stmt = delete('test_table').fields(some_list__indexes=(3, 4)).where(some_id=1)

        self.assertEqual('DELETE some_list[%s], some_list[%s] FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((3, 4, 1), stmt.context)

    def test_update_with_where(self):
        stmt = update('test_table').set(title='New title').where(id__in=['a', 'b', 'c'])

        self.assertEqual('UPDATE test_table SET title = %s WHERE id IN %s', stmt.query)
        self.assertEqual(('New title', ValueSequence(['a', 'b', 'c'])), stmt.context)

    def test_update_set_add(self):
        stmt = update('test_table').set(some_set__add={'a'}).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_set = some_set + %s WHERE some_id = %s', stmt.query)
        self.assertEqual(({'a'}, 1), stmt.context)

    def test_update_set_remove(self):
        stmt = update('test_table').set(some_set__remove={'a'}).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_set = some_set - %s WHERE some_id = %s', stmt.query)
        self.assertEqual(({'a'}, 1), stmt.context)

    def test_update_dict(self):
        stmt = update('test_table').set(some_dict={'a': 1, 'b': 2}).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_dict = %s WHERE some_id = %s', stmt.query)
        self.assertEqual(({'a': 1, 'b': 2}, 1), stmt.context)

    def test_update_dict_update(self):
        stmt = update('test_table').set(some_dict__update={'a': 1, 'b': 2}).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_dict[%s] = %s, some_dict[%s] = %s WHERE some_id = %s', stmt.query)
        self.assertEqual(('a', 1, 'b', 2, 1), stmt.context)

    def test_update_list_prepend(self):
        stmt = update('test_table').set(some_list__prepend=['a']).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_list = %s + some_list WHERE some_id = %s', stmt.query)
        self.assertEqual((['a'], 1), stmt.context)

    def test_update_list_append(self):
        stmt = update('test_table').set(some_list__append=['a']).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_list = some_list + %s WHERE some_id = %s', stmt.query)
        self.assertEqual((['a'], 1), stmt.context)

    def test_update_list_insert(self):
        stmt = update('test_table').set(some_list__insert__2='a').where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_list[2] = %s WHERE some_id = %s', stmt.query)
        self.assertEqual(('a', 1), stmt.context)

    def test_update_list_remove(self):
        stmt = update('test_table').set(some_list__remove=['a']).where(some_id=1)

        self.assertEqual('UPDATE test_table SET some_list = some_list - %s WHERE some_id = %s', stmt.query)
        self.assertEqual((['a'], 1), stmt.context)
