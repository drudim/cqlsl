from base import BaseStatement, WhereClauseMixin


__all__ = ['delete']


class DeleteStatement(BaseStatement, WhereClauseMixin):
    def __init__(self, *args, **kwargs):
        BaseStatement.__init__(self, *args, **kwargs)
        WhereClauseMixin.__init__(self)
        self._delete_entirely = []
        self._delete_partially = {}
        self._context_values = []

    def fields(self, *entirely, **partially):
        self._delete_entirely = entirely
        self._delete_partially = partially
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

        return 'DELETE {fields_clause} FROM {table} WHERE {where_clause}'.format(
            table=self.table_name,
            fields_clause=', '.join([x for x in (delete_entirely, delete_partially) if x]),
            where_clause=self._get_where_clause(),
        )

    @property
    def context(self):
        return self.query and tuple(self._context_values) + self._get_where_context()


delete = DeleteStatement
