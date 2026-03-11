"""
demo.py — End-to-end walkthrough of DataForge features.

Run with:  python demo.py
"""

from dataforge.pipeline import Pipeline
from dataforge.transforms import (
    clamp, normalize, regex_filter, field_extractor,
    compose, sliding_window, flatten, TransformStats, timed
)


# ═══════════════════════════════════════════════════════════════════════════
# 1. Basic numeric pipeline
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 50)
print("1. Numeric pipeline")
print("=" * 50)

result = (
    Pipeline(range(1, 21))          # 1 … 20
    .filter(lambda x: x % 2 == 0)  # keep evens
    .map(clamp(4, 14))              # clamp to [4, 14]
    .map(normalize(4, 14))          # scale to [0, 1]
    .collect()
)
print(f"Result ({len(result)} items): {[round(v, 2) for v in result]}")
stats = TransformStats(result)
print(stats.summary())


# ═══════════════════════════════════════════════════════════════════════════
# 2. Text pipeline with regex
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("2. Text pipeline with regex filter")
print("=" * 50)

emails = [
    "alice@example.com",
    "bob_at_home",
    "carol@work.org",
    "not-an-email",
    "dave@corp.io",
]

valid = (
    Pipeline(emails)
    .filter(regex_filter(r"^[\w.+-]+@[\w-]+\.[a-z]{2,}$"))
    .map(str.lower)
    .collect()
)
print("Valid emails:", valid)


# ═══════════════════════════════════════════════════════════════════════════
# 3. Dict pipeline with field extraction
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("3. Record pipeline with field extraction")
print("=" * 50)

records = [
    {"id": 1, "name": "Alice",   "score": 88, "dept": "Engineering"},
    {"id": 2, "name": "Bob",     "score": 55, "dept": "Marketing"},
    {"id": 3, "name": "Carol",   "score": 92, "dept": "Engineering"},
    {"id": 4, "name": "Dave",    "score": 70, "dept": "Design"},
    {"id": 5, "name": "Eve",     "score": 61, "dept": "Engineering"},
]

top_eng = (
    Pipeline(records)
    .filter(lambda r: r["dept"] == "Engineering")
    .filter(lambda r: r["score"] >= 80)
    .map(field_extractor("name", "score"))
    .collect()
)
print("Top Engineering scorers:", top_eng)


# ═══════════════════════════════════════════════════════════════════════════
# 4. Function composition
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("4. Function composition  (compose)")
print("=" * 50)

clean = compose(str.strip, str.lower, lambda s: s.replace("-", " "))
words = ["  Hello-World ", "PYTHON-ROCKS ", "  Data-Forge  "]
cleaned = Pipeline(words).map(clean).collect()
print("Cleaned strings:", cleaned)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Sliding window + batch
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("5. Sliding window  →  batch")
print("=" * 50)

data = [10, 20, 30, 40, 50, 60, 70]
windows = list(sliding_window(data, 3))
print("Windows (size=3):", windows)

batched = Pipeline(data).batch(3).collect()
print("Batches (size=3):", batched)


# ═══════════════════════════════════════════════════════════════════════════
# 6. reduce terminal operation
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("6. Pipeline.reduce  (running sum)")
print("=" * 50)

total = (
    Pipeline(range(1, 11))
    .filter(lambda x: x % 2 != 0)   # odd numbers only: 1 3 5 7 9
    .reduce(lambda acc, x: acc + x, 0)
)
print("Sum of odd 1–10:", total)


# ═══════════════════════════════════════════════════════════════════════════
# 7. @timed decorator
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("7. @timed decorator")
print("=" * 50)

@timed
def heavy_pipeline(n: int) -> list[float]:
    return (
        Pipeline(range(n))
        .filter(lambda x: x % 3 == 0)
        .map(normalize(0, n))
        .collect()
    )

output = heavy_pipeline(10_000)
print(f"Produced {len(output)} normalised multiples-of-3")


print("\n✅  All demos complete.")
