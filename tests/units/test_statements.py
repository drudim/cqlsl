from cassandra.encoder import ValueSequence
from cqlsl.statements import insert, delete, update, select
from ..base import BaseTestCase


__all__ = ['StatementsTest']


class StatementsTest(BaseTestCase):
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

    def test_select_with_complex_where(self):
        stmt = select('test_table').where(part_a=1, part_b=2, part_c=3)

        self.assertEqual('SELECT * FROM test_table WHERE part_a = %s AND part_b = %s AND part_c = %s', stmt.query)
        self.assertEqual((1, 2, 3), stmt.context)

    def test_select_with_where_gt_and_lt(self):
        stmt = select('test_table').where(part_a__gt=1, part_b__lt=2)

        self.assertEqual('SELECT * FROM test_table WHERE part_a > %s AND part_b < %s', stmt.query)
        self.assertEqual((1, 2), stmt.context)

    def test_select_with_where_gte_and_lte(self):
        stmt = select('test_table').where(part_a__gte=1, part_b__lte=2)

        self.assertEqual('SELECT * FROM test_table WHERE part_a >= %s AND part_b <= %s', stmt.query)
        self.assertEqual((1, 2), stmt.context)

    def test_select_with_limit(self):
        stmt = select('test_table').limit(10)

        self.assertEqual('SELECT * FROM test_table LIMIT %s', stmt.query)
        self.assertEqual((10,), stmt.context)

    def test_select_with_ordering(self):
        stmt = select('test_table').where(id=1).order_by('date', '-order').limit(10)

        self.assertEqual('SELECT * FROM test_table WHERE id = %s ORDER BY date ASC, order DESC LIMIT %s', stmt.query)
        self.assertEqual((1, 10), stmt.context)

    def test_select_with_allow_filtering(self):
        stmt = select('test_table').where(id__gt=1).allow_filtering()

        self.assertEqual('SELECT * FROM test_table WHERE id > %s ALLOW FILTERING', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_delete(self):
        stmt = delete('test_table').where(some_id=1)

        self.assertEqual('DELETE  FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_delete_fields(self):
        stmt = delete('test_table').fields('some_number', 'some_string').where(some_id=1)

        self.assertEqual('DELETE some_number, some_string FROM test_table WHERE some_id = %s', stmt.query)
        self.assertEqual((1,), stmt.context)

    def test_delete_with_complex_where(self):
        stmt = delete('test_table').fields('some_number').where(part_a__in=['a'], part_b=1)

        self.assertEqual('DELETE some_number FROM test_table WHERE part_a IN %s AND part_b = %s', stmt.query)
        self.assertEqual((ValueSequence(['a']), 1), stmt.context)

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

    def test_update(self):
        stmt = update('test_table').set(title='New title').where(id__in=['a', 'b', 'c'])

        self.assertEqual('UPDATE test_table SET title = %s WHERE id IN %s', stmt.query)
        self.assertEqual(('New title', ValueSequence(['a', 'b', 'c'])), stmt.context)

    def test_update_with_complex_where(self):
        stmt = update('test_table').set(title='New title').where(part_a=1, part_b__in=['a', 'b'])

        self.assertEqual('UPDATE test_table SET title = %s WHERE part_a = %s AND part_b IN %s', stmt.query)
        self.assertEqual(('New title', 1, ValueSequence(['a', 'b'])), stmt.context)

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
        try:
            self.assertEqual(('a', 1, 'b', 2, 1), stmt.context)
        except AssertionError:
            self.assertEqual(('b', 2, 'a', 1, 1), stmt.context)

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
