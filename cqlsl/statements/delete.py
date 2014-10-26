from base import BaseStatement


__all__ = ['delete']


class DeleteStatement(BaseStatement):
    def fields(self, *entirely, **partially):
        return self

    def where(self, **conditions):
        return self

    @property
    def query(self):
        return ''

    @property
    def context(self):
        return ''


delete = DeleteStatement
