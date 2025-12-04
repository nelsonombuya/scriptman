# 🔄 Scriptman Serialization Module — Implementation Complete

> **Status:** ✅ Implemented
> **Location:** `scriptman/serialization.py`
> **Tests:** `tests/test_serialization.py`
> **Coverage:** 100%

---

## Overview

Simple, focused utilities for converting Python objects to JSON/TOML-compatible formats. Used throughout Scriptman for config persistence, cache key generation, event storage, and API responses.

**Design Philosophy:** Do one thing well. No reverse deserialization — that's handled by Pydantic or explicit constructors where needed.

---

## ✅ Implemented Features

| Feature            | Status | Description                                             |
| ------------------ | ------ | ------------------------------------------------------- |
| `serialize(value)` | ✅      | Convert any Python value to JSON-compatible format      |
| Path support       | ✅      | `PurePath` and all subclasses → string                  |
| DateTime support   | ✅      | `datetime`, `date`, `time` → ISO 8601 string            |
| Enum support       | ✅      | `Enum` → underlying value                               |
| UUID support       | ✅      | `UUID` → string                                         |
| Decimal support    | ✅      | `Decimal` → string (preserves precision)                |
| Pydantic support   | ✅      | `BaseModel` → dict (via `model_dump()`)                 |
| Nested structures  | ✅      | Recursive handling of dict, list, tuple, set, frozenset |
| Passthrough        | ✅      | Unknown types pass through unchanged                    |

### ❌ Removed Features

| Feature          | Reason                                                                                |
| ---------------- | ------------------------------------------------------------------------------------- |
| `to_path(value)` | Violated YAGNI — zero usage in codebase. Users can simply use `Path(value)` directly. |

---

## 📁 File Structure

```
scriptman/
├── serialization.py          # 85 lines, single function
└── __init__.py               # Re-exports serialize

tests/
└── test_serialization.py     # 49 tests, 100% coverage
```

---

## 🎯 API Reference

### `serialize(value: Any) -> Any`

Convert Python value to JSON/TOML-compatible format.

```python
from scriptman import serialize
from pathlib import Path
from datetime import datetime
from decimal import Decimal

# Basic types pass through
serialize("hello")          # "hello"
serialize(42)               # 42
serialize(None)             # None

# Special types convert to strings
serialize(Path(".logs"))                    # ".logs"
serialize(datetime(2024, 1, 15, 10, 30))   # "2024-01-15T10:30:00"
serialize(Decimal("3.14159"))              # "3.14159"

# Nested structures handled recursively
serialize({
    "path": Path(".data"),
    "timestamp": datetime.now(),
    "tags": {"cache", "config"},
})
# {'path': '.data', 'timestamp': '2024-01-15T10:30:00', 'tags': ['cache', 'config']}

# Pydantic models via model_dump()
from pydantic import BaseModel

class Config(BaseModel):
    path: Path
    timeout: int

serialize(Config(path=Path(".data"), timeout=30))
# {'path': '.data', 'timeout': 30}
```

---

## 🏗️ Design Decisions

### 1. `PurePath` over `Path` for isinstance check

```python
# ✅ Current implementation
if isinstance(value, PurePath):
    return str(value)
```

**Why:** `PurePath` is the base class for all path types. This catches:
- `Path`, `PosixPath`, `WindowsPath` (concrete paths)
- `PurePath`, `PurePosixPath`, `PureWindowsPath` (pure paths)

Using `Path` would miss pure path variants that users might use for cross-platform path manipulation.

### 2. Modern Union Syntax in `isinstance()`

```python
# ✅ Python 3.10+ syntax (PEP 604)
if isinstance(value, datetime | date | time):
    return value.isoformat()

# Instead of traditional tuple syntax
if isinstance(value, (datetime, date, time)):
    ...
```

**Why:** Ruff's UP038 rule recommends the modern union syntax for readability. Project targets Python 3.12+.

### 3. No Deserialization

The module intentionally does NOT provide `deserialize()` because:
- Deserialization requires knowing the target type
- Pydantic handles this better with validation
- Config readers handle their own type coercion
- YAGNI — no demonstrated need in the codebase

### 4. Passthrough for Unknown Types

```python
# Unknown types pass through unchanged
serialize(custom_object)  # Returns custom_object as-is
```

**Why:**
- Allows composition with other serializers
- Doesn't break on unexpected types
- Caller can handle edge cases if needed

### 5. Collections Convert to Lists

```python
serialize((1, 2, 3))        # [1, 2, 3] — tuple → list
serialize({1, 2, 3})        # [1, 2, 3] — set → list
serialize(frozenset([1]))   # [1] — frozenset → list
```

**Why:** JSON only supports arrays, not tuples or sets.

---

## 🧪 Test Coverage

### Test Classes (49 tests total)

| Class                       | Tests | Coverage                                            |
| --------------------------- | ----- | --------------------------------------------------- |
| `TestPrimitives`            | 5     | None, str, int, float, bool                         |
| `TestPathSerialization`     | 3     | Path, PurePath, nested paths                        |
| `TestDateTimeSerialization` | 5     | datetime, date, time + microseconds                 |
| `TestEnumSerialization`     | 2     | String and int enum values                          |
| `TestUUIDSerialization`     | 2     | UUID to string, lowercase                           |
| `TestDecimalSerialization`  | 4     | Precision, large numbers, negatives                 |
| `TestPydanticSerialization` | 3     | Models, nested types, nested models                 |
| `TestNestedStructures`      | 7     | Dict, list, tuple, set, frozenset, deep nesting     |
| `TestEdgeCases`             | 15    | Empty values, unicode, special chars, unknown types |
| `TestIntegration`           | 3     | Config-like, event-like, API response scenarios     |

### Edge Cases Covered

- ✅ Empty containers (`{}`, `[]`, `""`)
- ✅ Unicode strings (Chinese, emojis)
- ✅ Special characters (`\n`, `\t`, `{curly}`)
- ✅ Unknown types (pass through)
- ✅ Bytes (pass through)
- ✅ Callables and classes (pass through)
- ✅ Zero values (`0`, `0.0`, `Decimal("0")`)
- ✅ Negative numbers
- ✅ Very long strings (10,000+ chars)
- ✅ Deeply nested structures

---

## 📡 Usage Across Codebase

| Module                             | Usage                                   |
| ---------------------------------- | --------------------------------------- |
| `scriptman/cache/__init__.py`      | Cache key generation from args/kwargs   |
| `scriptman/config/readers/toml.py` | Config value serialization before write |
| `scriptman/observe/event.py`       | Event data serialization for storage    |
| `scriptman/_internal/log.py`       | Log data formatting                     |

---

## ✅ Compliance Checklist

- [x] Zero-config usage works for common case
- [x] Type hints complete (modern 3.12+ syntax)
- [x] Docstring with emoji, example, and error docs
- [x] Error handling (RecursionError documented)
- [x] Tests written with 100% coverage
- [x] Edge cases handled (unicode, empty, long values)
- [x] Test classes organized by functionality
- [x] Test docstrings use emoji prefixes
- [x] No unused code (YAGNI — `to_path` removed)
- [x] `__all__` exports only public API
- [x] No external dependencies (stdlib only)

---

## 📝 Changelog

### v3.0.0

**Added:**
- `serialize()` function for Python-to-JSON conversion
- Support for Path, datetime, Enum, UUID, Decimal, Pydantic
- Recursive handling of nested structures
- 100% test coverage

**Removed:**
- `to_path()` — violated YAGNI, zero usage

---

## 🔗 References

- **Module:** `scriptman/serialization.py`
- **Tests:** `tests/test_serialization.py`
- **Feature Guide:** `.migration/FEATURE_GUIDE.md`
- **Rules:** `.cursor/rules/scriptman.mdc`
