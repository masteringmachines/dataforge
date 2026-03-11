# DataForge 🔧

A concise Python toolkit that demonstrates **advanced language features** through a clean, chainable **data transformation pipeline**.

## Features

| Feature | Where |
|---|---|
| Generic types & `TypeVar` | `pipeline.py` |
| Closures / factory functions | `transforms.py` |
| `@timed` decorator with `functools.wraps` | `transforms.py` |
| `functools.reduce` | `pipeline.py`, `transforms.py` |
| `dataclass` with cached `@property` | `transforms.py` |
| Generator functions (`yield`) | `transforms.py` |
| Function composition | `transforms.py` |

## Quick start

```bash
# No external dependencies required — standard library only
python demo.py
```

## Run tests

```bash
pip install pytest
python -m pytest tests/ -v
```

## Project layout

```
dataforge/
├── dataforge/
│   ├── __init__.py
│   ├── pipeline.py      # Pipeline[T] class
│   └── transforms.py    # Transform factories & utilities
├── tests/
│   └── test_dataforge.py
├── demo.py              # Full walkthrough
└── README.md
```

## Pipeline API

```python
from dataforge import Pipeline
from dataforge.transforms import clamp, normalize, field_extractor

result = (
    Pipeline(range(1, 21))
    .filter(lambda x: x % 2 == 0)   # keep evens
    .map(clamp(4, 14))               # clamp to [4, 14]
    .map(normalize(4, 14))           # scale to [0.0, 1.0]
    .collect()
)
```

## License

MIT
