from itertools import repeat
from base import BaseStatement
from utils import sorted_kwargs


__all__ = ['insert']


class InsertStatement(BaseStatement):
    def __init__(self, *args, **kwargs):
        super(InsertStatement, self).__init__(*args, **kwargs)
        self.values_kwargs = {}

    def values(self, **kwargs):
        self.values_kwargs = sorted_kwargs(**kwargs)
        return self

    @property
    def query(self):
        return 'INSERT INTO %(table)s (%(keys)s) VALUES (%(values)s)' % {
            'table': self.table_name,
            'keys': ', '.join(self.values_kwargs.keys()),
            'values': ', '.join(repeat('%s', len(self.values_kwargs)))
        }

    @property
    def context(self):
        return tuple(self.values_kwargs.values())


insert = InsertStatement
