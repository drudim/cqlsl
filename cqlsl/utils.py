from collections import OrderedDict


__all__ = ['sorted_kwargs']


def sorted_kwargs(**kwargs):
    """
    >>> sorted_kwargs(b=1, a=1, c=1).keys()
    ['a', 'b', 'c']
    >>> sorted_kwargs(b=2, a=1, c=3).values()
    [1, 2, 3]
    """
    return OrderedDict(sorted(kwargs.items(), key=lambda x: x[0]))
