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
    CONDITIONS = {
        'in': 'IN',
        'gt': '>',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
    }

    def __init__(self):
        self._where_conditions = {}

    def where(self, **conditions):
        self._where_conditions = sorted_kwargs(**conditions)
        return self

    def _get_where_clause(self):
        conditions = []

        for expression in self._where_conditions.keys():
            try:
                field, condition = expression.rsplit('__', 1)
            except ValueError:
                field, condition = expression, None

            if condition and condition not in self.CONDITIONS:
                raise AttributeError('Unknown condition "{}"'.format(condition))

            condition = self.CONDITIONS.get(condition, '=')
            conditions.append('{field} {condition} %s'.format(field=field, condition=condition))

        return ' AND '.join(conditions)

    def _get_where_context(self):
        context = []

        for value in self._where_conditions.values():
            if isinstance(value, collections.Iterable) and not isinstance(value, basestring):
                value = ValueSequence(value)

            context.append(value)

        return tuple(context)
