"""
tests/test_dataforge.py — Unit tests for DataForge.
Run with:  python -m pytest tests/ -v
"""

import pytest
from dataforge.pipeline import Pipeline
from dataforge.transforms import (
    clamp, normalize, regex_filter, field_extractor,
    compose, sliding_window, flatten, TransformStats,
)


# ── Pipeline ─────────────────────────────────────────────────────────────────

class TestPipeline:

    def test_map(self):
        result = Pipeline([1, 2, 3]).map(lambda x: x * 10).collect()
        assert result == [10, 20, 30]

    def test_filter(self):
        result = Pipeline(range(10)).filter(lambda x: x % 2 == 0).collect()
        assert result == [0, 2, 4, 6, 8]

    def test_chain(self):
        result = (
            Pipeline(range(1, 6))
            .map(lambda x: x ** 2)
            .filter(lambda x: x > 5)
            .collect()
        )
        assert result == [9, 16, 25]

    def test_reduce(self):
        total = Pipeline([1, 2, 3, 4, 5]).reduce(lambda a, b: a + b, 0)
        assert total == 15

    def test_batch(self):
        result = Pipeline(range(7)).batch(3).collect()
        assert result == [[0, 1, 2], [3, 4, 5], [6]]

    def test_first(self):
        result = Pipeline(range(100)).first(5)
        assert result == [0, 1, 2, 3, 4]

    def test_repr(self):
        p = Pipeline([]).map(str).filter(bool)
        assert "map" in repr(p)
        assert "filter" in repr(p)

    def test_step_count(self):
        p = Pipeline([]).map(str).map(str.upper).filter(bool)
        assert p.step_count == 3


# ── Transforms ───────────────────────────────────────────────────────────────

class TestClamp:
    def test_within(self):
        assert clamp(0, 10)(5) == 5

    def test_below(self):
        assert clamp(0, 10)(-3) == 0

    def test_above(self):
        assert clamp(0, 10)(99) == 10


class TestNormalize:
    def test_midpoint(self):
        assert normalize(0, 10)(5) == pytest.approx(0.5)

    def test_min(self):
        assert normalize(0, 10)(0) == pytest.approx(0.0)

    def test_max(self):
        assert normalize(0, 10)(10) == pytest.approx(1.0)

    def test_invalid_range(self):
        with pytest.raises(ValueError):
            normalize(5, 5)


class TestRegexFilter:
    def test_match(self):
        assert regex_filter(r"\d+")(  "abc123") is True

    def test_no_match(self):
        assert regex_filter(r"\d+")("abc") is False


class TestFieldExtractor:
    def test_extracts(self):
        record = {"a": 1, "b": 2, "c": 3}
        assert field_extractor("a", "c")(record) == {"a": 1, "c": 3}

    def test_missing_key_ignored(self):
        record = {"a": 1}
        assert field_extractor("a", "z")(record) == {"a": 1}


class TestCompose:
    def test_order(self):
        add1 = lambda x: x + 1
        double = lambda x: x * 2
        # compose(f, g)(x) == f(g(x))
        fn = compose(add1, double)
        assert fn(3) == 7   # double(3)=6 → add1(6)=7


class TestSlidingWindow:
    def test_basic(self):
        result = list(sliding_window([1, 2, 3, 4], 2))
        assert result == [(1, 2), (2, 3), (3, 4)]

    def test_too_small(self):
        result = list(sliding_window([1], 3))
        assert result == []


class TestFlatten:
    def test_flatten(self):
        result = list(flatten([[1, 2], [3], [4, 5]]))
        assert result == [1, 2, 3, 4, 5]


class TestTransformStats:
    def setup_method(self):
        self.stats = TransformStats([1.0, 2.0, 3.0, 4.0, 5.0])

    def test_count(self):
        assert self.stats.count == 5

    def test_mean(self):
        assert self.stats.mean == pytest.approx(3.0)

    def test_min(self):
        assert self.stats.minimum == pytest.approx(1.0)

    def test_max(self):
        assert self.stats.maximum == pytest.approx(5.0)

    def test_summary_string(self):
        s = self.stats.summary()
        assert "TransformStats" in s
        assert "mean=3.00" in s
