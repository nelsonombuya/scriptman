"""🔧 Reusable type definitions for generic programming.

This module provides:
- Type variables: T, P, R, C, BaseModelT
- Function types: Func, AsyncFunc
- Utilities: is_async()
- Locks: AsyncLock, StampedeLock

For decorator patterns using @overload, see docs/sync-async-decorators.md

Usage:
    >>> from scriptman.types import T, P, R, Func, is_async
    >>>
    >>> # Create a decorator using @overload pattern
    >>> # See docs/sync-async-decorators.md for the full pattern
"""

from __future__ import annotations

from asyncio import Lock as AsyncIOLock
from collections.abc import Callable, Coroutine
from inspect import iscoroutinefunction
from threading import Lock as ThreadingLock
from typing import Any, ParamSpec, TypeVar

from pydantic import BaseModel

__all__ = [
    # Type variables
    "T",
    "P",
    "R",
    "C",
    "BaseModelT",
    # Function type aliases
    "Func",
    "AsyncFunc",
    # Utilities
    "is_async",
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

C = TypeVar("C")
"""Context type variable (for context manager return types)."""

BaseModelT = TypeVar("BaseModelT", bound=BaseModel)
"""TypeVar bound to Pydantic BaseModel for model-generic functions."""


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION TYPE ALIASES
# ═══════════════════════════════════════════════════════════════════════════════

Func = Callable[P, R]
"""Type alias for any callable function.

Use this as the PRIMARY type for decorator signatures with @overload.
The type variable R captures the full return type, including Coroutine
for async functions.

Example:
    >>> from typing import overload
    >>> from collections.abc import Callable, Coroutine
    >>>
    >>> @overload
    >>> def my_decorator(
    ...     func: Callable[P, Coroutine[Any, Any, T]]
    ... ) -> Callable[P, Coroutine[Any, Any, T]]: ...
    >>> @overload
    >>> def my_decorator(func: Callable[P, R]) -> Callable[P, R]: ...
    >>> def my_decorator(func: Callable[P, R]) -> Callable[P, R]:
    ...     # Implementation using is_async() for runtime detection
    ...     ...

See docs/sync-async-decorators.md for the complete pattern.
"""

AsyncFunc = Callable[P, Coroutine[Any, Any, R]]
"""Type alias for async functions.

Use for explicit async typing or casting inside decorator implementations.

Note: async def functions ALWAYS return Coroutine, not just Awaitable.
This is why we use Coroutine[Any, Any, R] instead of Awaitable[R].

Example:
    >>> from typing import cast
    >>>
    >>> if is_async(func):
    ...     async_fn = cast(AsyncFunc[P, R], func)
    ...     result = await async_fn(*args, **kwargs)
"""


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════


def is_async(func: Callable[..., Any]) -> bool:
    """🔍 Check if a function is an async def.

    Uses iscoroutinefunction() which detects async def functions.
    Does NOT detect sync functions that return awaitables (Future, Task, etc.).

    Args:
        func: Function to check

    Returns:
        True if func is an async def, False otherwise

    Example:
        >>> async def async_fn(): pass
        >>> def sync_fn(): pass
        >>> is_async(async_fn)
        True
        >>> is_async(sync_fn)
        False
    """
    return iscoroutinefunction(func)


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
        """🔐 Lazily create async lock (must be in async context)."""
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
        >>> lock = await locks.acquire_async("key123")
        >>> async with lock:
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
        # Lazy initialization is safe here: asyncio runs in a single-threaded
        # event loop, so two coroutines cannot truly race on this check.
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

    def clear_all_sync(self) -> None:
        """🧹 Remove all sync locks."""
        with self._sync_locks_lock:
            self._sync_locks.clear()

    async def clear_all_async(self) -> None:
        """🧹 Remove all async locks."""
        if self._async_locks_lock is None:
            return
        async with self._async_locks_lock:
            self._async_locks.clear()
