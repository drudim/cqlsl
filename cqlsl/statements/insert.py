from itertools import repeat
from cqlsl.utils import sorted_kwargs
from base import BaseStatement


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
        return 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(
            table=self.table_name,
            keys=', '.join(self.values_kwargs.keys()),
            values=', '.join(repeat('%s', len(self.values_kwargs)))
        )

    @property
    def context(self):
        return tuple(self.values_kwargs.values())


insert = InsertStatement
