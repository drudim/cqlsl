import logging
from unittest import TestCase
import sys


class CqlslTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        logger = logging.getLogger('cqlsl')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler(sys.stdout))
