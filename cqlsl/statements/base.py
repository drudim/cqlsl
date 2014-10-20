class BaseStatement(object):
    def __init__(self, table_name):
        self.table_name = table_name

    @property
    def query(self):
        raise NotImplementedError('Redefine this in derived classes')

    @property
    def context(self):
        raise NotImplementedError('Redefine this in derived classes')
