from itertools import chain
from cqlsl.utils import sorted_kwargs
from base import BaseStatement, WhereClauseMixin


__all__ = ['delete']


class DeleteStatement(BaseStatement, WhereClauseMixin):
    def __init__(self, *args, **kwargs):
        BaseStatement.__init__(self, *args, **kwargs)
        WhereClauseMixin.__init__(self)
        self._delete_entirely = []
        self._delete_partially = {}

    def fields(self, *entirely, **partially):
        self._delete_entirely = entirely
        self._delete_partially = sorted_kwargs(**partially)
        return self

    @property
    def query(self):
        if not self._where_conditions:
            raise Exception('WHERE is mandatory for DELETE statement. To delete all rows use TRUNCATE.')

        return 'DELETE {fields_clause} FROM {table} WHERE {where_clause}'.format(
            table=self.table_name,
            fields_clause=self._get_fields_clause(),
            where_clause=self._get_where_clause(),
        )

    @property
    def context(self):
        return self._get_fields_context() + self._get_where_context()

    def _get_fields_clause(self):
        delete_entirely = ', '.join(self._delete_entirely)

        delete_partially = []
        for (modifier, values) in self._delete_partially.items():
            if modifier.endswith('__keys') or modifier.endswith('__indexes'):
                field_name = modifier.replace('__keys', '').replace('__indexes', '')
                delete_partially += len(values) * ['{}[%s]'.format(field_name)]

        delete_partially = ', '.join(delete_partially)

        return ', '.join([x for x in (delete_entirely, delete_partially) if x])

    def _get_fields_context(self):
        return tuple(chain(*self._delete_partially.values()))


delete = DeleteStatement
