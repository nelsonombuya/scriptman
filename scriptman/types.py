"""🔧 Reusable type definitions and decorator utilities.

This module provides:
- Common type aliases for generic programming
- Sync/async function detection and wrapping
- Decorator utilities that work with both sync and async functions
- Lock utilities for stampede prevention

Usage:
    >>> from scriptman.types import T, P, R, wrap_function
    >>>
    >>> # Create a decorator that works with sync and async
    >>> def my_decorator(func: Func[P, R]) -> Func[P, R]:
    ...     return wrap_function(func, before=lambda: print("before"))
"""

from __future__ import annotations

from asyncio import Lock as AsyncIOLock
from functools import wraps
from inspect import iscoroutinefunction
from threading import Lock as ThreadingLock
from typing import Any, Awaitable, Callable, ParamSpec, TypeVar, Union, cast

from pydantic import BaseModel

__all__ = [
    # Type variables
    "T",
    "P",
    "R",
    "BaseModelT",
    # Function type aliases
    "SyncFunc",
    "AsyncFunc",
    "Func",
    # Utilities
    "is_async",
    "wrap_function",
    "make_sync_async_decorator",
    # Locks
    "AsyncLock",
    "StampedeLock",
]


# ═══════════════════════════════════════════════════════════════════════════════
# TYPE VARIABLES
# ═══════════════════════════════════════════════════════════════════════════════

T = TypeVar("T")
"""Generic type variable for any type."""

P = ParamSpec("P")
"""Parameter specification for generic function signatures."""

R = TypeVar("R")
"""Return type variable (use when T is already used for arguments)."""

BaseModelT = TypeVar("BaseModelT", bound=BaseModel)
"""TypeVar bound to Pydantic BaseModel for model-generic functions."""


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION TYPE ALIASES
# ═══════════════════════════════════════════════════════════════════════════════

SyncFunc = Callable[P, R]
"""Type alias for synchronous functions."""

AsyncFunc = Callable[P, Awaitable[R]]
"""Type alias for asynchronous functions."""

Func = Union[SyncFunc[P, R], AsyncFunc[P, R]]
"""Type alias for functions that can be either sync or async."""


# ═══════════════════════════════════════════════════════════════════════════════
# DECORATOR UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════


def is_async(func: Callable[..., Any]) -> bool:
    """🔍 Check if a function is asynchronous.

    Works with regular async functions and async methods.

    Args:
        func: Function to check

    Returns:
        True if function is async, False otherwise

    Example:
        >>> async def async_fn(): pass
        >>> def sync_fn(): pass
        >>> is_async(async_fn)
        True
        >>> is_async(sync_fn)
        False
    """
    return iscoroutinefunction(func)


def wrap_function(
    func: Func[P, R],
    *,
    before: Callable[[], None] | None = None,
    after: Callable[[R], None] | None = None,
    on_error: Callable[[Exception], None] | None = None,
    transform_result: Callable[[R], R] | None = None,
) -> Func[P, R]:
    """🔄 Wrap a function with before/after hooks (works with sync and async).

    This is a building block for creating decorators that need to work
    with both synchronous and asynchronous functions.

    Args:
        func: Function to wrap
        before: Called before function execution
        after: Called after successful execution with result
        on_error: Called on exception (exception still propagates)
        transform_result: Transform the result before returning

    Returns:
        Wrapped function (same sync/async type as input)

    Example:
        >>> def log_start():
        ...     print("Starting...")
        >>>
        >>> def log_end(result):
        ...     print(f"Done: {result}")
        >>>
        >>> @lambda f: wrap_function(f, before=log_start, after=log_end)
        ... def process(x: int) -> int:
        ...     return x * 2
        >>>
        >>> process(5)
        Starting...
        Done: 10
        10
    """
    if is_async(func):

        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if before:
                before()
            try:
                result = await cast(AsyncFunc[P, R], func)(*args, **kwargs)
                if transform_result:
                    result = transform_result(result)
                if after:
                    after(result)
                return result
            except Exception as e:
                if on_error:
                    on_error(e)
                raise

        return cast(AsyncFunc[P, R], async_wrapper)

    else:

        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if before:
                before()
            try:
                result = cast(SyncFunc[P, R], func)(*args, **kwargs)
                if transform_result:
                    result = transform_result(result)
                if after:
                    after(result)
                return result
            except Exception as e:
                if on_error:
                    on_error(e)
                raise

        return cast(SyncFunc[P, R], sync_wrapper)


def make_sync_async_decorator(
    decorator_logic: Callable[
        [Func[P, R], tuple[Any, ...], dict[str, Any]],
        tuple[Callable[[], None] | None, Callable[[R], R] | None],
    ],
) -> Callable[[Func[P, R]], Func[P, R]]:
    """🎀 Create a decorator that works with both sync and async functions.

    This is a higher-order function for building decorators. You provide
    a function that receives (func, args, kwargs) and returns (before_hook,
    transform_result), and it handles the sync/async wrapping.

    Args:
        decorator_logic: Function that takes (func, args, kwargs) and returns
                        (before_callback, result_transformer)

    Returns:
        A decorator that works with both sync and async functions

    Example:
        >>> def timing_logic(func, args, kwargs):
        ...     start = [0.0]
        ...     def before():
        ...         import time
        ...         start[0] = time.time()
        ...     def transform(result):
        ...         duration = time.time() - start[0]
        ...         print(f"Took {duration:.2f}s")
        ...         return result
        ...     return before, transform
        >>>
        >>> timing = make_sync_async_decorator(timing_logic)
        >>>
        >>> @timing
        ... def slow_function():
        ...     import time
        ...     time.sleep(0.1)
        ...     return "done"
    """

    def decorator(func: Func[P, R]) -> Func[P, R]:
        if is_async(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                before, transform = decorator_logic(func, args, kwargs)
                if before:
                    before()
                result = await cast(AsyncFunc[P, R], func)(*args, **kwargs)
                if transform:
                    result = transform(result)
                return result

            return cast(AsyncFunc[P, R], async_wrapper)
        else:

            @wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                before, transform = decorator_logic(func, args, kwargs)
                if before:
                    before()
                result = cast(SyncFunc[P, R], func)(*args, **kwargs)
                if transform:
                    result = transform(result)
                return result

            return cast(SyncFunc[P, R], sync_wrapper)

    return decorator


# ═══════════════════════════════════════════════════════════════════════════════
# LOCKS (for stampede prevention)
# ═══════════════════════════════════════════════════════════════════════════════


class AsyncLock:
    """🔐 Lock that works in both sync and async contexts.

    Provides a unified interface for locking in mixed sync/async code.
    Uses threading.Lock for sync and asyncio.Lock for async.

    Example:
        >>> lock = AsyncLock()
        >>>
        >>> # Sync context
        >>> with lock.sync():
        ...     do_sync_work()
        >>>
        >>> # Async context
        >>> async with lock.async_():
        ...     await do_async_work()
    """

    def __init__(self) -> None:
        """🚀 Initialize both sync and async locks."""
        self._sync_lock = ThreadingLock()
        self._async_lock: AsyncIOLock | None = None

    @property
    def _async(self) -> AsyncIOLock:
        """Lazily create async lock (must be in async context)."""
        if self._async_lock is None:
            self._async_lock = AsyncIOLock()
        return self._async_lock

    def sync(self) -> ThreadingLock:
        """🔐 Get sync lock for use with `with` statement."""
        return self._sync_lock

    def async_(self) -> AsyncIOLock:
        """🔐 Get async lock for use with `async with` statement."""
        return self._async


class StampedeLock:
    """🔐 Lock manager for cache stampede prevention.

    Creates per-key locks to prevent multiple concurrent computations
    of the same value. Works with both sync and async code.

    Example:
        >>> locks = StampedeLock()
        >>>
        >>> # Sync usage
        >>> with locks.acquire_sync("key123"):
        ...     compute_value()
        >>>
        >>> # Async usage
        >>> async with locks.acquire_async("key123"):
        ...     await compute_value()
    """

    def __init__(self) -> None:
        """🚀 Initialize lock storage."""
        self._sync_locks: dict[str, ThreadingLock] = {}
        self._async_locks: dict[str, AsyncIOLock] = {}
        self._sync_locks_lock = ThreadingLock()
        self._async_locks_lock: AsyncIOLock | None = None

    def acquire_sync(self, key: str) -> ThreadingLock:
        """🔐 Get or create a sync lock for a key.

        Args:
            key: Lock identifier

        Returns:
            threading.Lock for use with `with` statement
        """
        with self._sync_locks_lock:
            if key not in self._sync_locks:
                self._sync_locks[key] = ThreadingLock()
            return self._sync_locks[key]

    async def acquire_async(self, key: str) -> AsyncIOLock:
        """🔐 Get or create an async lock for a key.

        Args:
            key: Lock identifier

        Returns:
            asyncio.Lock for use with `async with` statement
        """
        if self._async_locks_lock is None:
            self._async_locks_lock = AsyncIOLock()

        async with self._async_locks_lock:
            if key not in self._async_locks:
                self._async_locks[key] = AsyncIOLock()
            return self._async_locks[key]

    def cleanup_sync(self, key: str) -> None:
        """🧹 Remove a sync lock (call when no longer needed)."""
        with self._sync_locks_lock:
            self._sync_locks.pop(key, None)

    async def cleanup_async(self, key: str) -> None:
        """🧹 Remove an async lock (call when no longer needed)."""
        if self._async_locks_lock is None:
            return
        async with self._async_locks_lock:
            self._async_locks.pop(key, None)
