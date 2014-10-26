import collections
from cassandra.query import ValueSequence
from base import BaseStatement


__all__ = ['delete']


class DeleteStatement(BaseStatement):
    def __init__(self, *args, **kwargs):
        super(DeleteStatement, self).__init__(*args, **kwargs)
        self._delete_entirely = []
        self._delete_partially = {}
        self._where_conditions = {}
        self._context_values = []

    def fields(self, *entirely, **partially):
        self._delete_entirely = entirely
        self._delete_partially = partially
        return self

    def where(self, **conditions):
        self._where_conditions = conditions
        return self

    @property
    def query(self):
        if not self._where_conditions:
            raise Exception('WHERE is mandatory for DELETE statement. To delete all rows use TRUNCATE.')

        self._context_values = []

        # Generate fields clause
        delete_entirely = ', '.join(self._delete_entirely)
        delete_partially = []
        for (modifier, values) in self._delete_partially.items():
            if modifier.endswith('__keys') or modifier.endswith('__indexes'):
                field_name = modifier.replace('__keys', '').replace('__indexes', '')
                for value in values:
                    delete_partially.append('{}[%s]'.format(field_name))
                    self._context_values.append(value)
        delete_partially = ', '.join(delete_partially)

        # Generate where clause
        where_items = []
        for (field_name, value) in self._where_conditions.items():
            if field_name.endswith('__in'):
                field_name = field_name.replace('__in', '')
                where_items.append('{} IN %s'.format(field_name))
            else:
                where_items.append('{} = %s'.format(field_name))

            if isinstance(value, collections.Iterable):
                value = ValueSequence(value)
            self._context_values.append(value)

        return 'DELETE {fields_clause} FROM {table} WHERE {where_clause}'.format(
            table=self.table_name,
            fields_clause=', '.join([x for x in (delete_entirely, delete_partially) if x]),
            where_clause=' AND '.join(where_items)
        )

    @property
    def context(self):
        return self.query and tuple(self._context_values)


delete = DeleteStatement
