from itertools import chain
from cqlsl.utils import sorted_kwargs
from base import BaseStatement, WhereClauseMixin


__all__ = ['update']


class UpdateStatement(BaseStatement, WhereClauseMixin):
    def __init__(self, *args, **kwargs):
        BaseStatement.__init__(self, *args, **kwargs)
        WhereClauseMixin.__init__(self)
        self._set_values = {}

    def set(self, **values):
        self._set_values = sorted_kwargs(**values)
        return self

    @property
    def query(self):
        if not self._where_conditions:
            raise Exception('WHERE is mandatory for UPDATE statement.')

        return 'UPDATE {table} SET {set_clause} WHERE {where_clause}'.format(
            table=self.table_name,
            set_clause=self._get_set_clause(),
            where_clause=self._get_where_clause(),
        )

    @property
    def context(self):
        return self._get_set_context() + self._get_where_context()

    def _get_set_clause(self):
        set_items = []
        for field, modifier, value in self._unpack_set_items():
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
                set_items += ['{field}[%s] = %s'.format(field=field)] * len(value.items())
            else:
                set_items.append('{field} = %s'.format(field=field))

        return ', '.join(set_items)

    def _get_set_context(self):
        context = []
        for field, modifier, value in self._unpack_set_items():
            if modifier == 'update' and isinstance(value, dict):
                context += list(chain(*value.items()))
            else:
                context.append(value)

        return tuple(context)

    def _unpack_set_items(self):
        for field, value in self._set_values.items():
            try:
                field, modifier = field.split('__', 1)
            except ValueError:
                modifier = ''

            yield field, modifier, value

update = UpdateStatement
