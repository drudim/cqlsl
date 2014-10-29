import collections
from cassandra.query import ValueSequence
from base import BaseStatement
from utils import sorted_kwargs


__all__ = ['update']


class UpdateStatement(BaseStatement):
    def __init__(self, *args, **kwargs):
        super(UpdateStatement, self).__init__(*args, **kwargs)
        self._set_values = {}
        self._where_conditions = {}
        self._context_values = []

    def set(self, **values):
        self._set_values = sorted_kwargs(**values)
        return self

    def where(self, **conditions):
        self._where_conditions = sorted_kwargs(**conditions)
        return self

    @property
    def query(self):
        if not self._where_conditions:
            raise Exception('WHERE is mandatory for UPDATE statement.')

        self._context_values = []

        # Generate set values
        set_items = []
        for field, value in self._set_values.items():
            try:
                field, modifier = field.split('__', 1)
            except ValueError:
                modifier = ''

            if modifier in ('add', 'append'):
                set_items.append('{field} = {field} + %s'.format(field=field))
            elif modifier == 'remove':
                set_items.append('{field} = {field} - %s'.format(field=field))
            elif modifier == 'prepend':
                set_items.append('{field} = %s + {field}'.format(field=field))
            elif modifier.startswith('insert__'):
                index = int(modifier.replace('insert__', ''))
                set_items.append('{field}[{index}] = %s'.format(field=field, index=index))
            elif modifier == 'update' and isinstance(value, dict):
                for dict_key, dict_value in value.items():
                    set_items.append('{field}[%s] = %s'.format(field=field))
                    self._context_values.append(dict_key)
                    self._context_values.append(dict_value)
                continue
            else:
                set_items.append('{field} = %s'.format(field=field))

            self._context_values.append(value)

        # Generate where clause
        where_items = []
        for (field_name, value) in self._where_conditions.items():
            if field_name.endswith('__in'):
                field_name = field_name.replace('__in', '')
                where_items.append('{} IN %s'.format(field_name))
            else:
                where_items.append('{} = %s'.format(field_name))

            if isinstance(value, collections.Iterable) and not isinstance(value, basestring):
                value = ValueSequence(value)

            self._context_values.append(value)

        query = 'UPDATE {table} SET {set_clause}'.format(
            table=self.table_name,
            set_clause=', '.join(set_items),
        )

        if where_items:
            query += ' WHERE {where_clause}'.format(where_clause=' AND '.join(where_items))

        return query

    @property
    def context(self):
        return self.query and tuple(self._context_values)

update = UpdateStatement
