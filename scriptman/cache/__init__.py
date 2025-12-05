"""🗄️ Scriptman Cache — SQLite-backed caching made simple.

Quick Start:
    >>> from scriptman import cache
    >>>
    >>> # Simple key-value operations
    >>> cache.set("user:123", {"name": "Alice"}, ttl=3600)
    >>> user = cache.get("user:123")
    >>>
    >>> # Decorator for function results
    >>> @cache.result(ttl=3600, tags=["users"])
    ... def get_user(id: int) -> dict:
    ...     return fetch_user_from_db(id)
    >>>
    >>> # Tags for grouped invalidation
    >>> cache.invalidate_tag("users")  # Clear all user caches

Features:
    - SQLite-backed persistent caching
    - TTL expiration (lazy + active)
    - Tags for grouped invalidation
    - LRU eviction with size limits
    - Sharding for performance
    - Stampede prevention
    - Sync/async decorator support
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from functools import wraps
from threading import Lock
from typing import Any, cast

from scriptman._internal import log
from scriptman.cache.backends import CacheBackend, CacheStats
from scriptman.cache.backends.sharded import ShardedSQLiteBackend
from scriptman.cache.backends.sqlite import SQLiteCacheBackend
from scriptman.types import P, R, StampedeLock, is_async

__all__ = [
    "cache",
    "Cache",
    "CacheBackend",
    "CacheStats",
    "SQLiteCacheBackend",
    "ShardedSQLiteBackend",
]


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════


class Cache:
    """🗄️ Scriptman cache manager — simple caching with powerful features.

    Provides a unified interface for caching with:
    - Auto-configured SQLite backend
    - Optional sharding for write performance
    - Tag-based invalidation
    - Function result caching via @cache.result()
    - Full observability integration

    The Cache class is a singleton — all imports share the same instance.

    Attributes:
        backend: The underlying cache backend

    Example:
        >>> from scriptman import cache
        >>>
        >>> # Key-value operations
        >>> cache.set("key", "value", ttl=60)
        >>> cache.get("key")
        'value'
        >>>
        >>> # Decorator
        >>> @cache.result(ttl=3600)
        ... def expensive_function(x: int) -> int:
        ...     return x ** 2
    """

    _instance: Cache | None = None
    _lock: Lock = Lock()
    __initialized: bool = False

    def __new__(cls) -> Cache:
        """🔒 Thread-safe singleton."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """🚀 Initialize cache manager from config."""
        if self.__initialized:
            return

        # Lazy backend initialization
        self._backend: CacheBackend | None = None
        self._stampede_lock = StampedeLock()

        self.__initialized = True

    @property
    def backend(self) -> CacheBackend:
        """🗄️ Get the cache backend (lazy initialization)."""
        if self._backend is None:
            self._backend = self._create_backend()
        return self._backend

    def _create_backend(self) -> CacheBackend:
        """🏗️ Create backend from config."""
        from scriptman import config

        shards = config.get("cache.shards", 1)
        max_size_mb = config.get("cache.max_size_mb")
        eviction_batch_size = config.get("cache.eviction_batch_size", 100)

        # Convert MB to bytes
        max_size_bytes = max_size_mb * 1024 * 1024 if max_size_mb else None

        if shards > 1:
            log.debug(
                f"🗄️ Creating sharded cache backend: {shards} shards",
                shards=shards,
            )
            return ShardedSQLiteBackend(
                path=None,  # use default from config
                shards=shards,
                max_size_bytes=max_size_bytes,
                eviction_batch_size=eviction_batch_size,
            )
        else:
            log.debug("🗄️ Creating SQLite cache backend")
            return SQLiteCacheBackend(
                path=None,  # use default from config
                max_size_bytes=max_size_bytes,
                eviction_batch_size=eviction_batch_size,
            )

    # ─────────────────────────────────────────────────────────────
    # Core Operations (delegate to backend)
    # ─────────────────────────────────────────────────────────────

    def get(self, key: str) -> Any | None:
        """🔍 Retrieve value by key.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired

        Example:
            >>> cache.set("greeting", "Hello")
            >>> cache.get("greeting")
            'Hello'
            >>> cache.get("missing")
            None
        """
        return self.backend.get(key)

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> bool:
        """✍️ Store value with optional TTL and tags.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (None = use config default or forever)
            tags: Tags for grouped invalidation

        Returns:
            True if stored successfully

        Example:
            >>> cache.set("user:123", {"name": "Alice"}, ttl=3600, tags=["users"])
            True
        """
        # Apply default TTL from config if not specified
        if ttl is None:
            from scriptman import config

            ttl = config.get("cache.default_ttl")

        return self.backend.set(key, value, ttl=ttl, tags=tags)

    def delete(self, key: str) -> bool:
        """🧹 Delete value by key.

        Args:
            key: Cache key

        Returns:
            True if key existed and was deleted

        Example:
            >>> cache.set("temp", "value")
            >>> cache.delete("temp")
            True
        """
        return self.backend.delete(key)

    def exists(self, key: str) -> bool:
        """🔍 Check if key exists and is not expired.

        Args:
            key: Cache key

        Returns:
            True if key exists and is valid

        Example:
            >>> cache.set("key", "value")
            >>> cache.exists("key")
            True
        """
        return self.backend.exists(key)

    def clear(self) -> int:
        """🧹 Delete all cached values.

        Returns:
            Number of items deleted

        Example:
            >>> cache.clear()
            42
        """
        return self.backend.clear()

    # ─────────────────────────────────────────────────────────────
    # Bulk Operations
    # ─────────────────────────────────────────────────────────────

    def get_many(self, keys: list[str]) -> dict[str, Any]:
        """🔍 Retrieve multiple values by keys.

        Args:
            keys: List of cache keys

        Returns:
            Dict of key -> value (missing/expired keys omitted)

        Example:
            >>> cache.set_many({"a": 1, "b": 2})
            >>> cache.get_many(["a", "b", "c"])
            {'a': 1, 'b': 2}
        """
        return self.backend.get_many(keys)

    def set_many(
        self,
        items: dict[str, Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """✍️ Store multiple values.

        Args:
            items: Dict of key -> value
            ttl: TTL for all items
            tags: Tags for all items

        Returns:
            Number of items stored

        Example:
            >>> cache.set_many({"x": 1, "y": 2, "z": 3}, ttl=300)
            3
        """
        # Apply default TTL from config if not specified
        if ttl is None:
            from scriptman import config

            ttl = config.get("cache.default_ttl")

        return self.backend.set_many(items, ttl=ttl, tags=tags)

    def delete_many(self, keys: list[str]) -> int:
        """🧹 Delete multiple values.

        Args:
            keys: List of cache keys

        Returns:
            Number of items deleted

        Example:
            >>> cache.delete_many(["x", "y", "z"])
            3
        """
        return self.backend.delete_many(keys)

    # ─────────────────────────────────────────────────────────────
    # Tag Operations
    # ─────────────────────────────────────────────────────────────

    def invalidate_tag(self, tag: str) -> int:
        """🧹 Delete all values with the given tag.

        Args:
            tag: Tag to invalidate

        Returns:
            Number of items deleted

        Example:
            >>> cache.set("user:1", {...}, tags=["users"])
            >>> cache.set("user:2", {...}, tags=["users"])
            >>> cache.invalidate_tag("users")
            2
        """
        return self.backend.invalidate_tag(tag)

    def get_by_tag(self, tag: str) -> dict[str, Any]:
        """🔍 Retrieve all values with the given tag.

        Args:
            tag: Tag to search

        Returns:
            Dict of key -> value for all matching items

        Example:
            >>> cache.get_by_tag("users")
            {'user:1': {...}, 'user:2': {...}}
        """
        return self.backend.get_by_tag(tag)

    # ─────────────────────────────────────────────────────────────
    # Statistics
    # ─────────────────────────────────────────────────────────────

    def stats(self) -> CacheStats:
        """📊 Get cache statistics.

        Returns:
            CacheStats with hits, misses, size, count

        Example:
            >>> stats = cache.stats()
            >>> print(f"Hit ratio: {stats.hit_ratio:.2%}")
            Hit ratio: 85.00%
        """
        return self.backend.stats()

    # ─────────────────────────────────────────────────────────────
    # Maintenance
    # ─────────────────────────────────────────────────────────────

    def cleanup_expired(self) -> int:
        """🧹 Remove all expired entries.

        Call this periodically or via scheduler.

        Returns:
            Number of expired items removed

        Example:
            >>> cache.cleanup_expired()
            15
        """
        return self.backend.cleanup_expired()

    def evict_lru(self, count: int) -> int:
        """🧹 Evict least recently used entries.

        Args:
            count: Number of entries to evict

        Returns:
            Number of items evicted

        Example:
            >>> cache.evict_lru(100)
            100
        """
        return self.backend.evict_lru(count)

    # ─────────────────────────────────────────────────────────────
    # Decorator
    # ─────────────────────────────────────────────────────────────

    def result(
        self,
        ttl: int | None = None,
        tags: list[str] | None = None,
        key_fn: Callable[..., str] | None = None,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """🔄 Decorator to cache function results.

        Automatically caches the return value of a function. Works with
        both sync and async functions. Uses stampede prevention to avoid
        thundering herd.

        Args:
            ttl: Time-to-live in seconds (None = config default or forever)
            tags: Tags for grouped invalidation
            key_fn: Custom function to generate cache key from args/kwargs.
                    If None, generates key from function name + args hash.

        Returns:
            Decorated function

        Example:
            >>> @cache.result(ttl=3600, tags=["users"])
            ... def get_user(id: int) -> dict:
            ...     return database.get_user(id)
            >>>
            >>> user = get_user(123)  # Fetches from DB
            >>> user = get_user(123)  # Returns cached
            >>>
            >>> cache.invalidate_tag("users")  # Clear all user caches

        Async Example:
            >>> @cache.result(ttl=60)
            ... async def fetch_data(url: str) -> dict:
            ...     async with aiohttp.get(url) as resp:
            ...         return await resp.json()
        """

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            if is_async(func):
                return cast(Callable[P, R], self._async_wrapper(func, ttl, tags, key_fn))
            else:
                return self._sync_wrapper(func, ttl, tags, key_fn)

        return decorator

    def _generate_key(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> str:
        """🔑 Generate cache key from function signature.

        Creates a deterministic key based on:
        - Function module and qualified name
        - Args and kwargs (JSON serialized)
        """
        from scriptman.serialization import serialize

        key_parts = [
            func.__module__,
            func.__qualname__,
            json.dumps(serialize(args), sort_keys=True, default=str),
            json.dumps(serialize(kwargs), sort_keys=True, default=str),
        ]
        key_string = ":".join(key_parts)
        return f"fn:{hashlib.md5(key_string.encode()).hexdigest()}"

    def _sync_wrapper(
        self,
        func: Callable[..., Any],
        ttl: int | None,
        tags: list[str] | None,
        key_fn: Callable[..., str] | None,
    ) -> Callable[..., Any]:
        """🔄 Create sync wrapper for cached function."""

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Generate cache key
            if key_fn is not None:
                cache_key = key_fn(*args, **kwargs)
            else:
                cache_key = self._generate_key(func, args, kwargs)

            # Check cache first (fast path)
            cached = self.get(cache_key)
            if cached is not None:
                log.debug(f"🗄️ Cache hit: {func.__qualname__}", key=cache_key)
                return cached

            # Stampede prevention: acquire lock
            with self._stampede_lock.acquire_sync(cache_key):
                # Double-check after acquiring lock
                cached = self.get(cache_key)
                if cached is not None:
                    return cached

                # Compute and cache
                result = func(*args, **kwargs)
                self.set(cache_key, result, ttl=ttl, tags=tags)
                return result

        return wrapper

    def _async_wrapper(
        self,
        func: Callable[..., Any],
        ttl: int | None,
        tags: list[str] | None,
        key_fn: Callable[..., str] | None,
    ) -> Callable[..., Any]:
        """🔄 Create async wrapper for cached function."""

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Generate cache key
            if key_fn is not None:
                cache_key = key_fn(*args, **kwargs)
            else:
                cache_key = self._generate_key(func, args, kwargs)

            # Check cache first (fast path)
            cached = self.get(cache_key)
            if cached is not None:
                log.debug(f"🗄️ Cache hit: {func.__qualname__}", key=cache_key)
                return cached

            # Stampede prevention: acquire async lock
            lock = await self._stampede_lock.acquire_async(cache_key)
            async with lock:
                # Double-check after acquiring lock
                cached = self.get(cache_key)
                if cached is not None:
                    return cached

                # Compute and cache
                result = await func(*args, **kwargs)
                self.set(cache_key, result, ttl=ttl, tags=tags)
                return result

        return wrapper

    # ─────────────────────────────────────────────────────────────
    # Backend Management
    # ─────────────────────────────────────────────────────────────

    def use_backend(self, backend: CacheBackend) -> None:
        """🔧 Switch to a different cache backend.

        Args:
            backend: New backend instance

        Example:
            >>> from scriptman.cache import SQLiteCacheBackend
            >>> cache.use_backend(SQLiteCacheBackend("custom.db"))
        """
        if self._backend is not None:
            self._backend.close()
        self._backend = backend
        log.debug(f"🔧 Switched cache backend: {type(backend).__name__}")

    def close(self) -> None:
        """🔌 Close backend connections."""
        if self._backend is not None:
            self._backend.close()
            self._backend = None


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════

cache = Cache()

# Expose methods at module level for scriptman.cache.get() style access
get = cache.get
set = cache.set
delete = cache.delete
exists = cache.exists
clear = cache.clear
get_many = cache.get_many
set_many = cache.set_many
delete_many = cache.delete_many
invalidate_tag = cache.invalidate_tag
get_by_tag = cache.get_by_tag
stats = cache.stats
cleanup_expired = cache.cleanup_expired
evict_lru = cache.evict_lru
result = cache.result
