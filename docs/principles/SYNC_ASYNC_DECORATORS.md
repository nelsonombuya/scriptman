# 🔄 The Definitive Guide to Sync/Async Decorators in Python

> How to create ONE decorator that works perfectly with both sync and async functions.

## The Problem

You want to create a decorator that:
1. Works with both `def` and `async def` functions
2. Preserves the function's nature (async stays async, sync stays sync)
3. Has correct types (no confusing linter errors for users)

```python
@my_decorator
def sync_fn(): ...      # Should still be sync

@my_decorator
async def async_fn(): ...  # Should still be async
```

## The Solution: `@overload` Pattern

### Why It Works

Python's type system can't express "if the input is async, the output is async." But `@overload` lets us declare **multiple signatures** that the type checker chooses based on input:

```python
@overload
def my_decorator(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]: ...
@overload
def my_decorator(func: Callable[P, R]) -> Callable[P, R]: ...
```

- If you pass an async function (`Callable[..., Coroutine[...]]`) → first overload matches
- If you pass a sync function (`Callable[..., R]`) → second overload matches

### The Complete Pattern

```python
from collections.abc import Callable, Coroutine
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, ParamSpec, TypeVar, cast, overload

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")  # Inner return type (what you get after await)


def is_async(func: Callable[..., object]) -> bool:
    """🔍 Check if function is async."""
    return iscoroutinefunction(func)


# ═══════════════════════════════════════════════════════════════════════════════
# THE DECORATOR
# ═══════════════════════════════════════════════════════════════════════════════


# OVERLOAD 1: Async → Async (MUST come first - more specific)
@overload
def my_decorator(
    func: Callable[P, Coroutine[Any, Any, T]],
) -> Callable[P, Coroutine[Any, Any, T]]: ...


# OVERLOAD 2: Sync → Sync (fallback - less specific)
@overload
def my_decorator(func: Callable[P, R]) -> Callable[P, R]: ...


# IMPLEMENTATION
def my_decorator(func: Callable[P, R]) -> Callable[P, R]:
    """🔄 Decorator that preserves sync/async nature."""
    if is_async(func):
        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            # Your before logic here
            result = await func(*args, **kwargs)  # type: ignore[misc]
            # Your after logic here
            return result
        return cast(Callable[P, R], async_wrapper)
    else:
        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # Your before logic here
            result = func(*args, **kwargs)
            # Your after logic here
            return result
        return sync_wrapper
```

## Understanding the Type Escapes

The implementation needs three "escapes" from the type system. Here's why each is necessary:

### 1. `-> Any` on `async_wrapper`

```python
async def async_wrapper(...) -> Any:  # Why Any?
```

**Problem:**
- `R` = `Coroutine[Any, Any, int]` (the full async return type)
- After `await`, we get `int` (the inner type)
- We can't express "the inner type T where R = Coroutine[..., T]"

**Why it's okay:** The `@overload` ensures users see `Coroutine[Any, Any, T]`, not `Any`.

### 2. `type: ignore[misc]` on await

```python
result = await func(*args, **kwargs)  # type: ignore[misc]
```

**Problem:**
- Type checker sees `func` returns `R` (could be anything)
- We know it's a `Coroutine` from `is_async()` check
- Type checker can't narrow types based on runtime checks

**Why it's okay:** We've already verified `is_async(func)` is `True`.

### 3. `cast()` on return

```python
return cast(Callable[P, R], async_wrapper)
```

**Problem:**
- `async_wrapper` is typed as `Callable[P, Coroutine[Any, Any, Any]]`
- We need to return `Callable[P, R]` to match the signature

**Why it's okay:** The `@overload` ensures users see the correct type.

## Key Insight: `async def` Always Returns `Coroutine`

```python
async def foo() -> int:
    return 42

result = foo()
print(type(result))  # <class 'coroutine'> - ALWAYS!
```

This is why we use `Coroutine[Any, Any, T]` in the overload, not `Awaitable[T]`:
- `async def` functions **always** return `Coroutine`
- `Future`, `Task` are created explicitly, not by `async def`
- `iscoroutinefunction()` detects `async def`, which is what we're decorating

## Overload Order Matters!

Always put the **more specific** overload first:

```python
# ✅ CORRECT - Coroutine (specific) before R (general)
@overload
def deco(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]: ...
@overload
def deco(func: Callable[P, R]) -> Callable[P, R]: ...

# ❌ WRONG - General first would always match
@overload
def deco(func: Callable[P, R]) -> Callable[P, R]: ...  # Matches everything!
@overload
def deco(func: Callable[P, Coroutine[...]]) -> ...: ...  # Never reached
```

## What Users See

With this pattern, users get **perfect types**:

```python
@my_decorator
def sync_add(a: int, b: int) -> int:
    return a + b

@my_decorator
async def async_add(a: int, b: int) -> int:
    return a + b

# Type checker correctly infers:
result1 = sync_add(2, 3)        # int - no await needed
result2 = await async_add(2, 3) # int - must await (correctly!)
```

No linter errors. No confusing union types. Just clean, correct types.

## Summary

| Component            | Purpose                                 |
| -------------------- | --------------------------------------- |
| `@overload` #1       | Match async functions → return async    |
| `@overload` #2       | Match sync functions → return sync      |
| `is_async()`         | Runtime detection of `async def`        |
| `-> Any`             | Can't express inner type of Coroutine   |
| `type: ignore[misc]` | Type checker can't narrow on runtime    |
| `cast()`             | Convert wrapper to declared return type |

**The key principle:** Overloads make the PUBLIC API type-safe. Implementation escapes are INTERNAL and don't affect users.

---

## Full Working Example

```python
"""🧪 Debug file: The Correct @overload Pattern for Sync/Async Decorators.

This demonstrates the FINAL, CORRECT pattern for creating decorators that:
1. Preserve sync/async behavior (async stays async, sync stays sync)
2. Are fully type-safe for USERS (no confusing type errors)
3. Use minimal, justified type escapes in the IMPLEMENTATION

KEY INSIGHT:
- The @overload signatures define the PUBLIC API (what users see)
- The implementation needs type escapes because Python can't express:
  "if is_async(func), then R is Coroutine[Any, Any, T] and I need T"
"""

from collections.abc import Callable, Coroutine
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, ParamSpec, TypeVar, cast, overload

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")  # Inner return type (what you get after await)


def is_async(func: Callable[..., object]) -> bool:
    """🔍 Check if function is async."""
    return iscoroutinefunction(func)


# ═══════════════════════════════════════════════════════════════════════════════
# THE DECORATOR WITH @overload
# ═══════════════════════════════════════════════════════════════════════════════


# OVERLOAD 1: Async functions → Async functions
# Input:  Callable[P, Coroutine[Any, Any, T]]  (async def foo() -> T)
# Output: Callable[P, Coroutine[Any, Any, T]]  (still async, same inner type)
@overload
def my_decorator(
    func: Callable[P, Coroutine[Any, Any, T]],
) -> Callable[P, Coroutine[Any, Any, T]]: ...


# OVERLOAD 2: Sync functions → Sync functions
# Input:  Callable[P, R]  (def bar() -> R)
# Output: Callable[P, R]  (still sync, same return type)
@overload
def my_decorator(func: Callable[P, R]) -> Callable[P, R]: ...


# IMPLEMENTATION: The actual logic
# Note: Type escapes are necessary here because:
# - We use runtime is_async() check, which type system can't narrow
# - R in implementation = full return type (Coroutine[...] for async)
# - T in overload = inner type (what you get after await)
def my_decorator(func: Callable[P, R]) -> Callable[P, R]:
    """🔄 Decorator that preserves sync/async nature.

    For users, the types are perfect:
    - Pass async function → get async function back
    - Pass sync function → get sync function back

    Implementation uses minimal type escapes (cast) because Python's
    type system can't express runtime-dependent type transformations.
    """
    if is_async(func):
        # --- ASYNC PATH ---
        # func returns Coroutine[Any, Any, SomeType]
        # We need to create an async wrapper that:
        # 1. Awaits the original function
        # 2. Returns the unwrapped result
        # 3. Is itself async (so caller must await)

        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            # WHY Any? Because:
            # - R = Coroutine[Any, Any, T] (the full type)
            # - After await, we get T (the inner type)
            # - We can't express "T where R = Coroutine[..., T]" in Python
            print(f"[BEFORE] Calling async {func.__name__}")
            result = await func(*args, **kwargs)  # type: ignore[misc]
            # WHY type: ignore? Because:
            # - Type checker sees func returns R (could be anything)
            # - We know it's Coroutine (from is_async check)
            # - Type checker can't narrow based on runtime check
            print(f"[AFTER] Got result: {result}")
            return result

        # WHY cast? Because:
        # - async_wrapper is Callable[P, Coroutine[Any, Any, Any]]
        # - We need Callable[P, R] to satisfy return type
        # - The @overload ensures users see the correct type
        return cast(Callable[P, R], async_wrapper)

    else:
        # --- SYNC PATH ---
        # This path is fully type-safe, no escapes needed!

        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            print(f"[BEFORE] Calling sync {func.__name__}")
            result = func(*args, **kwargs)
            print(f"[AFTER] Got result: {result}")
            return result

        return sync_wrapper


# ═══════════════════════════════════════════════════════════════════════════════
# TEST FUNCTIONS - Users see perfect types!
# ═══════════════════════════════════════════════════════════════════════════════


@my_decorator
def sync_add(a: int, b: int) -> int:
    """Sync function - type checker knows: returns int"""
    return a + b


@my_decorator
async def async_add(a: int, b: int) -> int:
    """Async function - type checker knows: returns Coroutine[Any, Any, int]"""
    return a + b


# ═══════════════════════════════════════════════════════════════════════════════
# DEMO - Showing it works
# ═══════════════════════════════════════════════════════════════════════════════


def main() -> None:
    import asyncio

    print("=" * 70)
    print("THE CORRECT @overload PATTERN FOR SYNC/ASYNC DECORATORS")
    print("=" * 70)

    # --- SYNC USAGE ---
    print("\n1. SYNC FUNCTION")
    print("   Type checker sees: sync_add(int, int) -> int")
    print("   No await needed!")
    result1 = sync_add(2, 3)
    print(f"   Result: {result1} (type: {type(result1).__name__})")

    # --- ASYNC USAGE ---
    print("\n2. ASYNC FUNCTION")
    print("   Type checker sees: async_add(int, int) -> Coroutine[Any, Any, int]")
    print("   Must await!")
    coro = async_add(2, 3)
    print(f"   Coroutine: {coro}")
    result2 = asyncio.run(coro)
    print(f"   Result after await: {result2} (type: {type(result2).__name__})")

    # --- SUMMARY ---
    print("\n" + "=" * 70)
    print("✅ PATTERN SUMMARY")
    print("=" * 70)
    print("""
    PUBLIC API (what users see):
    ├─ @overload for Coroutine input → Coroutine output
    └─ @overload for R input → R output

    IMPLEMENTATION (internal):
    ├─ is_async() runtime check to branch
    ├─ async_wrapper returns Any (can't express T from Coroutine[..., T])
    ├─ type: ignore[misc] on await (type checker doesn't know it's awaitable)
    └─ cast() on return (convert async_wrapper to Callable[P, R])

    WHY THIS IS OKAY:
    ├─ Users get PERFECT types (no errors, correct inference)
    ├─ Runtime behavior is CORRECT (tested, works)
    └─ Implementation escapes are MINIMAL and DOCUMENTED
    """)


if __name__ == "__main__":
    main()
```
