from base import BaseStatement, WhereClauseMixin


__all__ = ['select']


class SelectStatement(BaseStatement, WhereClauseMixin):
    def __init__(self, *args, **kwargs):
        BaseStatement.__init__(self, *args, **kwargs)
        WhereClauseMixin.__init__(self)
        self._fields_to_select = []
        self._fields_to_order_by = []
        self._limit = None
        self._allow_filtering = False

    def fields(self, *fields_to_select):
        self._fields_to_select = fields_to_select
        return self

    def limit(self, limit_to):
        self._limit = limit_to
        return self

    def order_by(self, *fields_to_order_by):
        self._fields_to_order_by = fields_to_order_by
        return self

    def allow_filtering(self):
        self._allow_filtering = True
        return self

    @property
    def query(self):
        query_ = 'SELECT {fields_clause} FROM {table}'.format(
            fields_clause=self._get_fields_clause(),
            table=self.table_name,
        )

        if self._where_conditions:
            query_ += ' WHERE {}'.format(self._get_where_clause())

        if self._fields_to_order_by:
            query_ += ' ORDER BY {}'.format(self._get_ordering_clause())

        if self._limit:
            query_ += ' LIMIT %s'

        if self._allow_filtering:
            query_ += ' ALLOW FILTERING'

        return query_

    @property
    def context(self):
        context_ = self._get_where_context()

        if self._limit:
            context_ += (self._limit,)

        return context_

    def _get_fields_clause(self):
        return ', '.join(self._fields_to_select) or '*'

    def _get_ordering_clause(self):
        ordering = []
        for field in self._fields_to_order_by:
            if field.startswith('-'):
                ordering.append('{} DESC'.format(field[1:]))
            elif field.startswith('+'):
                ordering.append('{} ASC'.format(field[1:]))
            else:
                ordering.append('{} ASC'.format(field))

        return ', '.join(ordering)


select = SelectStatement
