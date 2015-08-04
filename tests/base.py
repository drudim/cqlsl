import logging
from unittest import TestCase
import sys


__all__ = ['BaseTestCase']


class BaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        logger = logging.getLogger('cqlsl')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler(sys.stdout))

    def assertItemsEqual(self, *args, **kwargs):
        if sys.version_info >= (3, 2):
            return super(BaseTestCase, self).assertCountEqual(*args, **kwargs)
        else:
            return super(BaseTestCase, self).assertItemsEqual(*args, **kwargs)
