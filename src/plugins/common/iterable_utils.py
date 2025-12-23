from collections.abc import Iterable

from wirerope.callable import Callable


def flat_map(func: Callable | None, iterable: Iterable):
    """
    return flattened map of iterable
    data = [[1, 2], [3, 4], 5]
    list(flat_map(None, data))  # [1, 2, 3, 4, 5]
    :param func: function to apply to each item, if None, identity function is used
    :param iterable: iterable to process
    """
    if iterable is None:
        return

    if func is None:
        func = lambda x: x

    for item in iterable:
        result = func(item)
        if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
            yield from result
        else:
            yield result
