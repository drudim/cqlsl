from base import BaseStatement, WhereClauseMixin
from utils import sorted_kwargs


__all__ = ['update']


class UpdateStatement(BaseStatement, WhereClauseMixin):
    def __init__(self, *args, **kwargs):
        BaseStatement.__init__(self, *args, **kwargs)
        WhereClauseMixin.__init__(self)
        self._set_values = {}
        self._context_values = []

    def set(self, **values):
        self._set_values = sorted_kwargs(**values)
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

        query = 'UPDATE {table} SET {set_clause}'.format(
            table=self.table_name,
            set_clause=', '.join(set_items),
        )

        if self._where_conditions:
            query += ' WHERE {where_clause}'.format(where_clause=self._get_where_clause())

        return query

    @property
    def context(self):
        return self.query and tuple(self._context_values) + self._get_where_context()

update = UpdateStatement
