from base import BaseStatement


__all__ = ['update']


class UpdateStatement(BaseStatement):
    def set(self, **values):
        return self

    def where(self, **conditions):
        return self

    @property
    def query(self):
        return ''

    @property
    def context(self):
        return tuple()

update = UpdateStatement
