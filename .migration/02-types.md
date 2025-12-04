# 🔧 Migration Plan: Types Module Simplification

> **Status:** ✅ Implemented

## Overview

This plan simplified `scriptman/types.py` based on research into Python's type system for sync/async decorators. The key insight: use `@overload` pattern instead of complex wrapper utilities.

**Reference:** See `docs/sync-async-decorators.md` for the full pattern documentation.

## Current State (Implemented)

```python
# Simplified exports
from scriptman.types import (
    # Type variables
    T, P, R, C, BaseModelT,
    # Function types (only two needed!)
    Func,       # Callable[P, R] - THE type for decorator signatures
    AsyncFunc,  # Callable[P, Coroutine[Any, Any, R]] - explicit async
    # Utilities
    is_async,   # Detect async def functions
    # Locks
    AsyncLock,
    StampedeLock,
)
```

## Changes Made

### 1. Removed Redundant Type Aliases

| Removed           | Reason                          |
| ----------------- | ------------------------------- |
| `SyncFunc`        | Identical to `Func`, just noise |
| `SyncOrAsyncFunc` | Replaced by `@overload` pattern |

### 2. Fixed `AsyncFunc` Definition

```python
# Before (incorrect)
AsyncFunc = Callable[P, Awaitable[R]]

# After (correct - async def always returns Coroutine)
AsyncFunc = Callable[P, Coroutine[Any, Any, R]]
```

### 3. Removed Wrapper Utilities

| Removed                       | Reason                                        |
| ----------------------------- | --------------------------------------------- |
| `await_async()`               | Sync-ifying is bad pattern, blocks event loop |
| `wrap_function()`             | Use `@overload` pattern directly              |
| `wrap_function_in_context()`  | Use `@overload` pattern directly              |
| `make_sync_async_decorator()` | Use `@overload` pattern directly              |

These utilities tried to hide complexity but created more confusion. The `@overload` pattern is clearer and gives better types.

### 4. Kept Essential Utilities

| Kept           | Reason                                        |
| -------------- | --------------------------------------------- |
| `is_async()`   | Essential for runtime detection in decorators |
| `AsyncLock`    | Useful for dual sync/async lock acquisition   |
| `StampedeLock` | Cache stampede prevention                     |

## Implementation Status

### ✅ Step 1: Updated `scriptman/types.py`

- Removed `SyncFunc`, `SyncOrAsyncFunc`, `await_async`, `wrap_function`, `wrap_function_in_context`, `make_sync_async_decorator`
- Updated `AsyncFunc` to use `Coroutine[Any, Any, R]`
- Updated docstrings to reference `docs/sync-async-decorators.md`

### ✅ Step 2: Updated `scriptman/__init__.py`

Removed deprecated exports.

### ✅ Step 3: Updated Existing Decorators

- ✅ `scriptman/cache/__init__.py` - `@cache.result` uses `@overload`
- ✅ `scriptman/observe/decorators.py` - `@observe` uses `@overload`

### ✅ Step 4: Updated Tests

- Removed tests for removed utilities
- Updated tests for simplified exports

### ✅ Step 5: Updated Documentation

- ✅ `docs/sync-async-decorators.md` - Full pattern guide
- ✅ `.cursor/rules/scriptman.mdc` - Updated rules

## Breaking Changes

| Change                              | Migration                         |
| ----------------------------------- | --------------------------------- |
| `SyncFunc` removed                  | Use `Func` instead                |
| `SyncOrAsyncFunc` removed           | Use `@overload` pattern           |
| `await_async` removed               | Don't sync-ify, preserve async    |
| `wrap_function` removed             | Use `@overload` pattern           |
| `wrap_function_in_context` removed  | Use `@overload` pattern           |
| `make_sync_async_decorator` removed | Use `@overload` pattern           |
| `AsyncFunc` changed                 | Now `Callable[P, Coroutine[...]]` |

## Testing Checklist

- [x] `is_async()` correctly detects async def
- [x] `Func` type alias works in decorator signatures
- [x] `AsyncFunc` type alias matches Coroutine return
- [x] `AsyncLock` works for both sync and async
- [x] `StampedeLock` prevents stampedes correctly
- [x] Existing decorators work with new pattern
- [x] No linter errors for users of decorated functions

---

## 🔗 References

- **Module:** `scriptman/types.py`
- **Tests:** `tests/test_types.py`
- **Documentation:** `docs/sync-async-decorators.md`
- **Feature Guide:** `.migration/FEATURE_GUIDE.md`
- **Rules:** `.cursor/rules/scriptman.mdc`
