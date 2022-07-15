import collections
from typing import Iterator, Callable, TypeVar, Any


def consume(iterator: Iterator):
    collections.deque(iterator, maxlen=0)


_Any = TypeVar('_Any')


def apply(fn: Callable[[_Any], Any], iterator: Iterator[_Any]) -> None:
    consume(map(fn, iterator))


def to_gb(_bytes: int) -> float:
    return _bytes / 1024 / 1024 / 1024
