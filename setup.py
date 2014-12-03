"""
CQLSL
-----

CQL for Python without any additional abstraction layers.
Designed to be simple and fast tool for communication with Cassandra.
CQLSL is something between native Cassandra driver (which is really awesome!) and ORM.

[Installation](https://github.com/drudim/cqlsl#installation)

[Documentation](https://github.com/drudim/cqlsl#getting-started)
"""
from setuptools import setup
import cqlsl


setup(
    name='cqlsl',
    version=cqlsl.__version__,
    url='https://github.com/drudim/cqlsl',
    license='MIT',
    author='Dmytro Popovych',
    author_email='drudim.ua@gmail.com',
    description='CQL for Python without any additional abstraction layers',
    long_description=__doc__,
    keywords=['cassandra', 'cql'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ],
    packages=['cqlsl'],
    include_package_data=True,
    install_requires=['cassandra-driver >= 2.1.1'],
)
