"""
transforms.py — Reusable, composable transform factories.

Advanced features used:
  - Closures / factory functions
  - Decorators with functools.wraps
  - dataclasses
  - Generator functions (yield)
  - *args / **kwargs
  - Type aliases
"""

from __future__ import annotations
import re
import time
import functools
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, TypeVar

T = TypeVar("T")
TransformFn = Callable[[Any], Any]


# ────────────────────────────────────────────────────────────────────────────
# Decorator: @timed  — logs how long a transform takes
# ────────────────────────────────────────────────────────────────────────────

def timed(func: Callable) -> Callable:
    """Decorator that prints execution time for any callable."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"  ⏱  {func.__name__} completed in {elapsed:.4f}s")
        return result
    return wrapper


# ────────────────────────────────────────────────────────────────────────────
# Factory functions (closures) — each returns a ready-to-use transform fn
# ────────────────────────────────────────────────────────────────────────────

def clamp(low: float, high: float) -> Callable[[float], float]:
    """Return a function that clamps a number to [low, high]."""
    def _clamp(value: float) -> float:
        return max(low, min(high, value))
    _clamp.__name__ = f"clamp({low}, {high})"
    return _clamp


def normalize(min_val: float, max_val: float) -> Callable[[float], float]:
    """Return a function that normalises a value to [0, 1]."""
    span = max_val - min_val
    if span == 0:
        raise ValueError("min_val and max_val must differ")
    def _normalize(value: float) -> float:
        return (value - min_val) / span
    _normalize.__name__ = "normalize"
    return _normalize


def regex_filter(pattern: str, flags: int = 0) -> Callable[[str], bool]:
    """Return a predicate that keeps strings matching *pattern*."""
    compiled = re.compile(pattern, flags)
    def _match(text: str) -> bool:
        return bool(compiled.search(text))
    _match.__name__ = f"regex_filter({pattern!r})"
    return _match


def field_extractor(*keys: str) -> Callable[[dict], dict]:
    """Return a function that picks *keys* from a dict."""
    def _extract(record: dict) -> dict:
        return {k: record[k] for k in keys if k in record}
    _extract.__name__ = f"field_extractor({keys})"
    return _extract


def compose(*funcs: Callable) -> Callable:
    """
    Compose multiple functions right-to-left.
    compose(f, g, h)(x) == f(g(h(x)))
    """
    def _composed(value: Any) -> Any:
        return functools.reduce(lambda v, f: f(v), reversed(funcs), value)
    _composed.__name__ = " ∘ ".join(f.__name__ for f in funcs)
    return _composed


# ────────────────────────────────────────────────────────────────────────────
# Generator-based transforms
# ────────────────────────────────────────────────────────────────────────────

def sliding_window(iterable: Iterable[T], size: int) -> Iterable[tuple]:
    """Yield overlapping windows of *size* elements."""
    buf: list[T] = []
    for item in iterable:
        buf.append(item)
        if len(buf) == size:
            yield tuple(buf)
            buf.pop(0)


def flatten(nested: Iterable[Iterable[T]]) -> Iterable[T]:
    """Flatten one level of nesting."""
    for sub in nested:
        yield from sub


# ────────────────────────────────────────────────────────────────────────────
# Dataclass: TransformStats — a lightweight result container
# ────────────────────────────────────────────────────────────────────────────

@dataclass
class TransformStats:
    """Holds summary statistics after processing a numeric pipeline."""
    values: list[float]
    _computed: dict = field(default_factory=dict, init=False, repr=False)

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def total(self) -> float:
        return self._cache("total", lambda: sum(self.values))

    @property
    def mean(self) -> float:
        return self._cache("mean", lambda: self.total / self.count if self.count else 0.0)

    @property
    def minimum(self) -> float:
        return self._cache("min", lambda: min(self.values))

    @property
    def maximum(self) -> float:
        return self._cache("max", lambda: max(self.values))

    def _cache(self, key: str, compute: Callable) -> float:
        if key not in self._computed:
            self._computed[key] = compute()
        return self._computed[key]

    def summary(self) -> str:
        return (
            f"TransformStats(n={self.count}, "
            f"min={self.minimum:.2f}, mean={self.mean:.2f}, max={self.maximum:.2f})"
        )
