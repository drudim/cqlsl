import collections
from cqlsl.utils import sorted_kwargs
from cassandra.query import ValueSequence


class BaseStatement(object):
    def __init__(self, table_name):
        self.table_name = table_name

    @property
    def query(self):
        raise NotImplementedError('Redefine this in derived classes')

    @property
    def context(self):
        raise NotImplementedError('Redefine this in derived classes')


class WhereClauseMixin(object):
    def __init__(self):
        self._where_conditions = {}

    def where(self, **conditions):
        self._where_conditions = sorted_kwargs(**conditions)
        return self

    def _get_where_clause(self):
        conditions = []

        for field_name in self._where_conditions.keys():
            if field_name.endswith('__in'):
                field_name = field_name.replace('__in', '')
                conditions.append('{} IN %s'.format(field_name))
            else:
                conditions.append('{} = %s'.format(field_name))

        return ', '.join(conditions)

    def _get_where_context(self):
        context = []

        for value in self._where_conditions.values():
            if isinstance(value, collections.Iterable) and not isinstance(value, basestring):
                value = ValueSequence(value)

            context.append(value)

        return tuple(context)
