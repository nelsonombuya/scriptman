# 🔧 Migration Plan: Types Module Simplification

## Overview

This plan simplifies `scriptman/types.py` based on research into Python's type system for sync/async decorators. The key insight: use `@overload` pattern instead of complex wrapper utilities.

**Reference:** See `docs/sync-async-decorators.md` for the full pattern documentation.

## Current State

```python
# Current exports (too many, confusing)
from scriptman.types import (
    T, P, R, C, BaseModelT,
    Func,           # Callable[P, R]
    SyncFunc,       # Callable[P, R] (same as Func!)
    AsyncFunc,      # Callable[P, Awaitable[R]] (should be Coroutine)
    SyncOrAsyncFunc,  # Union (replaced by @overload)
    is_async,
    await_async,    # Sync-ifies async (removed - bad pattern)
    wrap_function,  # Complex utility (removed)
    wrap_function_in_context,  # Complex utility (removed)
    make_sync_async_decorator,  # Complex utility (removed)
    AsyncLock,
    StampedeLock,
)
```

## Target State

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

## Changes

### 1. Remove Redundant Type Aliases

| Remove | Reason |
|--------|--------|
| `SyncFunc` | Identical to `Func`, just noise |
| `SyncOrAsyncFunc` | Replaced by `@overload` pattern |

### 2. Fix `AsyncFunc` Definition

```python
# Before (incorrect)
AsyncFunc = Callable[P, Awaitable[R]]

# After (correct - async def always returns Coroutine)
AsyncFunc = Callable[P, Coroutine[Any, Any, R]]
```

### 3. Remove Wrapper Utilities

| Remove | Reason |
|--------|--------|
| `await_async()` | Sync-ifying is bad pattern, blocks event loop |
| `wrap_function()` | Use `@overload` pattern directly |
| `wrap_function_in_context()` | Use `@overload` pattern directly |
| `make_sync_async_decorator()` | Use `@overload` pattern directly |

These utilities tried to hide complexity but created more confusion. The `@overload` pattern is clearer and gives better types.

### 4. Keep Essential Utilities

| Keep | Reason |
|------|--------|
| `is_async()` | Essential for runtime detection in decorators |
| `AsyncLock` | Useful for dual sync/async lock acquisition |
| `StampedeLock` | Cache stampede prevention |

## Implementation Steps

### Step 1: Update `scriptman/types.py`

```python
"""🔧 Reusable type definitions for generic programming.

This module provides:
- Type variables: T, P, R, C, BaseModelT
- Function types: Func, AsyncFunc
- Utilities: is_async()
- Locks: AsyncLock, StampedeLock

For decorator patterns, see docs/sync-async-decorators.md
"""

from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

from pydantic import BaseModel

__all__ = [
    # Type variables
    "T", "P", "R", "C", "BaseModelT",
    # Function types
    "Func", "AsyncFunc",
    # Utilities
    "is_async",
    # Locks
    "AsyncLock", "StampedeLock",
]

# ═══════════════════════════════════════════════════════════════════════════════
# TYPE VARIABLES
# ═══════════════════════════════════════════════════════════════════════════════

T = TypeVar("T")
"""Generic type variable for any type."""

P = ParamSpec("P")
"""Parameter specification for generic function signatures."""

R = TypeVar("R")
"""Return type variable."""

C = TypeVar("C")
"""Context type variable (for context manager return types)."""

BaseModelT = TypeVar("BaseModelT", bound=BaseModel)
"""TypeVar bound to Pydantic BaseModel."""


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION TYPE ALIASES
# ═══════════════════════════════════════════════════════════════════════════════

Func = Callable[P, R]
"""Type alias for any callable function.

Use this as the primary type for decorator signatures with @overload.
See docs/sync-async-decorators.md for the pattern.
"""

AsyncFunc = Callable[P, Coroutine[Any, Any, R]]
"""Type alias for async functions.

Use for explicit async typing or casting inside decorators.
Note: async def always returns Coroutine, not just Awaitable.
"""


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def is_async(func: Callable[..., object]) -> bool:
    """🔍 Check if a function is an async def.

    Uses iscoroutinefunction() which detects async def functions.
    Does NOT detect sync functions that return awaitables.

    Args:
        func: Function to check

    Returns:
        True if func is an async def, False otherwise
    """
    from inspect import iscoroutinefunction
    return iscoroutinefunction(func)


# ═══════════════════════════════════════════════════════════════════════════════
# LOCKS
# ═══════════════════════════════════════════════════════════════════════════════

# (Keep existing AsyncLock and StampedeLock implementations)
```

### Step 2: Update `scriptman/__init__.py`

Remove deprecated exports:
- `SyncFunc`
- `SyncOrAsyncFunc`
- `await_async`
- `wrap_function`
- `wrap_function_in_context`
- `make_sync_async_decorator`

### Step 3: Update Existing Decorators

Update these files to use `@overload` pattern:
- `scriptman/cache/__init__.py` - `@cache.result`
- `scriptman/observe/decorators.py` - `@observe`

### Step 4: Update Tests

- Remove tests for removed utilities
- Add tests for simplified exports
- Test that `@overload` pattern works correctly

### Step 5: Update Documentation

Already done:
- ✅ `docs/sync-async-decorators.md` - Full pattern guide
- ✅ `.cursor/rules/scriptman.mdc` - Updated rules

## Breaking Changes

| Change | Migration |
|--------|-----------|
| `SyncFunc` removed | Use `Func` instead |
| `SyncOrAsyncFunc` removed | Use `@overload` pattern |
| `await_async` removed | Don't sync-ify, preserve async |
| `wrap_function` removed | Use `@overload` pattern |
| `wrap_function_in_context` removed | Use `@overload` pattern |
| `make_sync_async_decorator` removed | Use `@overload` pattern |
| `AsyncFunc` changed | Now `Callable[P, Coroutine[...]]` |

## Testing Checklist

- [ ] `is_async()` correctly detects async def
- [ ] `Func` type alias works in decorator signatures
- [ ] `AsyncFunc` type alias matches Coroutine return
- [ ] `AsyncLock` works for both sync and async
- [ ] `StampedeLock` prevents stampedes correctly
- [ ] Existing decorators work with new pattern
- [ ] No linter errors for users of decorated functions

## Timeline

1. **Phase 1:** Create docs, update rules (DONE)
2. **Phase 2:** Simplify `scriptman/types.py`
3. **Phase 3:** Update existing decorators (`cache`, `observe`)
4. **Phase 4:** Update tests
5. **Phase 5:** Deprecation notices in changelog
