"""
pipeline.py — Chainable data transformation pipeline.

Advanced features used:
  - Generic types & TypeVar
  - __call__ protocol / callable objects
  - functools.reduce for chaining
  - @property & __repr__
  - *args/**kwargs forwarding
"""

from __future__ import annotations
from typing import Any, Callable, Generic, Iterable, TypeVar
from functools import reduce

T = TypeVar("T")
U = TypeVar("U")


class Pipeline(Generic[T]):
    """
    A lazy, chainable pipeline that applies a sequence of transforms
    to an iterable of data records.

    Example
    -------
    >>> result = (
    ...     Pipeline([1, 2, 3, 4, 5])
    ...     .map(lambda x: x * 2)
    ...     .filter(lambda x: x > 4)
    ...     .collect()
    ... )
    >>> print(result)   # [6, 8, 10]
    """

    def __init__(self, data: Iterable[T]) -> None:
        self._data: Iterable[T] = data
        self._steps: list[tuple[str, Callable]] = []

    # ------------------------------------------------------------------ #
    # Chainable transform builders
    # ------------------------------------------------------------------ #

    def map(self, func: Callable[[T], U]) -> "Pipeline[U]":
        """Apply *func* to every element (lazy)."""
        self._steps.append(("map", func))
        self._data = (func(item) for item in self._data)  # type: ignore[assignment]
        return self  # type: ignore[return-value]

    def filter(self, predicate: Callable[[T], bool]) -> "Pipeline[T]":
        """Keep only elements where *predicate* returns True (lazy)."""
        self._steps.append(("filter", predicate))
        self._data = (item for item in self._data if predicate(item))
        return self

    def apply(self, func: Callable[[Iterable[T]], Iterable[U]]) -> "Pipeline[U]":
        """Apply a bulk transform that receives the whole iterable."""
        self._steps.append(("apply", func))
        self._data = func(self._data)  # type: ignore[assignment]
        return self  # type: ignore[return-value]

    def batch(self, size: int) -> "Pipeline[list[T]]":
        """Group elements into fixed-size chunks."""
        def _batch(iterable: Iterable[T]) -> Iterable[list[T]]:
            chunk: list[T] = []
            for item in iterable:
                chunk.append(item)
                if len(chunk) == size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

        self._steps.append(("batch", _batch))
        self._data = _batch(self._data)  # type: ignore[assignment]
        return self  # type: ignore[return-value]

    # ------------------------------------------------------------------ #
    # Terminal operations
    # ------------------------------------------------------------------ #

    def collect(self) -> list[T]:
        """Materialise the pipeline into a list."""
        return list(self._data)

    def reduce(self, func: Callable[[U, T], U], initial: U) -> U:
        """Fold the pipeline into a single value."""
        return reduce(func, self._data, initial)

    def first(self, n: int = 1) -> list[T]:
        """Return the first *n* items without consuming the whole pipeline."""
        result: list[T] = []
        for item in self._data:
            result.append(item)
            if len(result) == n:
                break
        return result

    # ------------------------------------------------------------------ #
    # Dunder helpers
    # ------------------------------------------------------------------ #

    @property
    def step_count(self) -> int:
        return len(self._steps)

    def __repr__(self) -> str:
        steps = " → ".join(f"{name}()" for name, _ in self._steps) or "identity"
        return f"Pipeline[{steps}]"
