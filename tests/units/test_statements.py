from unittest import TestCase
from cassandra.encoder import ValueSequence
from statements import insert


__all__ = ['StatementsTest']


class StatementsTest(TestCase):
    def test_insert(self):
        stmt = insert('test_table').values(some_string='New title', some_number=1, some_numbers=[2, 3, 4])

        self.assertEqual(
            'INSERT INTO test_table (some_number, some_numbers, some_string) VALUES (%s, %s, %s)',
            stmt.query
        )
        self.assertEqual((1, ValueSequence([2, 3, 4]), 'New title'), stmt.context)
