"""🗄️ Cache backend abstractions.

This module defines the CacheBackend ABC and supporting dataclasses.
All cache implementations must inherit from CacheBackend.

Usage:
    >>> from scriptman.cache.backends import CacheBackend, CacheStats
    >>>
    >>> class RedisCacheBackend(CacheBackend):
    ...     def get(self, key: str) -> Any | None:
    ...         return self._redis.get(key)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

__all__ = [
    "CacheBackend",
    "CacheStats",
    "CacheEntry",
    "register_backend",
    "unregister_backend",
    "get_backend",
    "list_backends",
]


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass(slots=True)
class CacheStats:
    """📊 Cache statistics.

    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        size_bytes: Total size of cached data in bytes
        key_count: Number of keys in cache

    Example:
        >>> stats = cache.stats()
        >>> print(f"Hit ratio: {stats.hit_ratio:.2%}")
        Hit ratio: 85.00%
    """

    hits: int = 0
    misses: int = 0
    size_bytes: int = 0
    key_count: int = 0

    @property
    def hit_ratio(self) -> float:
        """📊 Calculate hit ratio (0.0 to 1.0)."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


@dataclass(slots=True)
class CacheEntry:
    """📦 Represents a cached item.

    Attributes:
        key: Cache key
        value: Cached value (deserialized)
        tags: List of tags for grouped invalidation
        expires_at: When the entry expires (None = never)
        created_at: When the entry was created
        accessed_at: Last access time (for LRU)

    Example:
        >>> entry = CacheEntry(key="user:123", value={"name": "Alice"})
        >>> entry.is_expired
        False
    """

    key: str
    value: Any
    tags: list[str] = field(default_factory=list)
    expires_at: datetime | None = None
    created_at: datetime | None = None
    accessed_at: datetime | None = None

    @property
    def is_expired(self) -> bool:
        """🔍 Check if entry is expired."""
        if self.expires_at is None:
            return False
        return datetime.now() >= self.expires_at


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE BACKEND ABC
# ═══════════════════════════════════════════════════════════════════════════════


class CacheBackend(ABC):
    """🗄️ Abstract base class for cache implementations.

    All cache backends must implement this interface. The default
    implementation is SQLiteCacheBackend.

    Design Principles:
        - Simple methods for common operations
        - Tag-based grouping for bulk invalidation
        - LRU tracking for size-limited caches
        - Stampede prevention via get_or_set()

    Example:
        >>> class RedisCacheBackend(CacheBackend):
        ...     def get(self, key: str) -> Any | None:
        ...         return self._redis.get(key)
    """

    # ─────────────────────────────────────────────────────────────
    # Core Operations
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """🔍 Retrieve value by key.

        Returns None if key doesn't exist or is expired.
        Updates accessed_at for LRU tracking.

        Args:
            key: Cache key

        Returns:
            Cached value or None

        Example:
            >>> value = cache.get("user:123")
            >>> if value is not None:
            ...     print(f"Found: {value}")
        """
        ...

    @abstractmethod
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
            value: Value to cache (will be serialized)
            ttl: Time-to-live in seconds (None = forever)
            tags: List of tags for grouped invalidation

        Returns:
            True if stored successfully

        Example:
            >>> cache.set("user:123", {"name": "Alice"}, ttl=3600, tags=["users"])
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """🧹 Delete value by key.

        Args:
            key: Cache key

        Returns:
            True if key existed and was deleted

        Example:
            >>> cache.delete("user:123")
            True
        """
        ...

    @abstractmethod
    def exists(self, key: str) -> bool:
        """🔍 Check if key exists and is not expired.

        Args:
            key: Cache key

        Returns:
            True if key exists and is valid

        Example:
            >>> if cache.exists("user:123"):
            ...     print("Key exists")
        """
        ...

    @abstractmethod
    def clear(self) -> int:
        """🧹 Delete all cached values.

        Returns:
            Number of items deleted

        Example:
            >>> deleted = cache.clear()
            >>> print(f"Cleared {deleted} items")
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Bulk Operations
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def get_many(self, keys: list[str]) -> dict[str, Any]:
        """🔍 Retrieve multiple values by keys.

        Args:
            keys: List of cache keys

        Returns:
            Dict of key -> value (missing/expired keys omitted)

        Example:
            >>> values = cache.get_many(["user:1", "user:2", "user:3"])
            >>> for key, value in values.items():
            ...     print(f"{key}: {value}")
        """
        ...

    @abstractmethod
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
            >>> cache.set_many({"a": 1, "b": 2}, ttl=300, tags=["numbers"])
        """
        ...

    @abstractmethod
    def delete_many(self, keys: list[str]) -> int:
        """🧹 Delete multiple values.

        Args:
            keys: List of cache keys

        Returns:
            Number of items deleted

        Example:
            >>> deleted = cache.delete_many(["user:1", "user:2"])
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Tag Operations
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def invalidate_tag(self, tag: str) -> int:
        """🧹 Delete all values with the given tag.

        Args:
            tag: Tag to invalidate

        Returns:
            Number of items deleted

        Example:
            >>> cache.invalidate_tag("users")  # Invalidate all user cache
        """
        ...

    @abstractmethod
    def get_by_tag(self, tag: str) -> dict[str, Any]:
        """🔍 Retrieve all values with the given tag.

        Useful for debugging and dashboard inspection.

        Args:
            tag: Tag to search

        Returns:
            Dict of key -> value for all matching items

        Example:
            >>> users = cache.get_by_tag("users")
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Statistics
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def stats(self) -> CacheStats:
        """📊 Get cache statistics.

        Returns:
            CacheStats with hits, misses, size, count

        Example:
            >>> stats = cache.stats()
            >>> print(f"Hit ratio: {stats.hit_ratio:.2%}")
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Maintenance
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def cleanup_expired(self) -> int:
        """🧹 Remove all expired entries (active expiration).

        Call this periodically or via scheduler.

        Returns:
            Number of expired items removed

        Example:
            >>> removed = cache.cleanup_expired()
            >>> print(f"Removed {removed} expired entries")
        """
        ...

    @abstractmethod
    def evict_lru(self, count: int) -> int:
        """🧹 Evict least recently used entries.

        Called when cache exceeds size limit.

        Args:
            count: Number of entries to evict

        Returns:
            Number of items evicted

        Example:
            >>> evicted = cache.evict_lru(100)
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Stampede Prevention
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        """🔐 Get value or compute and cache it (with stampede prevention).

        If the key doesn't exist, acquires a lock before computing.
        Other threads wait for the first one to finish.

        Args:
            key: Cache key
            factory: Callable that produces the value
            ttl: TTL for the cached value
            tags: Tags for the cached value

        Returns:
            Cached or freshly computed value

        Example:
            >>> def compute_expensive():
            ...     return expensive_calculation()
            >>> value = cache.get_or_set("result", compute_expensive, ttl=300)
        """
        ...

    @abstractmethod
    async def get_or_set_async(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        """🔐 Async version of get_or_set.

        Args:
            key: Cache key
            factory: Async callable that produces the value
            ttl: TTL for the cached value
            tags: Tags for the cached value

        Returns:
            Cached or freshly computed value

        Example:
            >>> async def fetch_data():
            ...     return await api.get("/data")
            >>> value = await cache.get_or_set_async("data", fetch_data, ttl=60)
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def close(self) -> None:
        """🔌 Close backend connections and release resources.

        Override in implementations that need cleanup.
        """
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# BACKEND REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

_backends: dict[str, type[CacheBackend]] = {}


def register_backend(name: str, cls: type[CacheBackend]) -> None:
    """🏷️ Register a cache backend by name.

    Use this function to register custom cache backends. Backends are
    registered manually after class definition to preserve type information.

    Args:
        name: Backend identifier (e.g., "sqlite", "redis")
        cls: Backend class to register

    Raises:
        ValueError: If a backend with the same name is already registered

    Example:
        >>> class RedisCacheBackend(CacheBackend):
        ...     def get(self, key: str) -> Any | None:
        ...         return self._redis.get(key)
        ...     # ... implement other abstract methods
        ...
        >>> register_backend("redis", RedisCacheBackend)
    """
    if name in _backends:
        raise ValueError(
            f"⚠️ Backend '{name}' is already registered. "
            f"Use a different name or unregister the existing backend first."
        )
    _backends[name] = cls


def get_backend(name: str) -> type[CacheBackend]:
    """🔍 Get a registered backend by name.

    Args:
        name: Backend identifier

    Returns:
        Backend class

    Raises:
        ValueError: If backend not found

    Example:
        >>> SQLiteBackend = get_backend("sqlite")
        >>> backend = SQLiteBackend()
    """
    if name not in _backends:
        available = ", ".join(_backends.keys()) or "(none registered)"
        raise ValueError(f"Unknown backend '{name}'. Available: {available}")
    return _backends[name]


def unregister_backend(name: str) -> bool:
    """🧹 Unregister a cache backend by name.

    Args:
        name: Backend identifier to remove

    Returns:
        True if backend was removed, False if not found

    Example:
        >>> unregister_backend("redis")
        True
    """
    if name in _backends:
        del _backends[name]
        return True
    return False


def list_backends() -> list[str]:
    """📋 List all registered backend names.

    Returns:
        List of backend names

    Example:
        >>> list_backends()
        ['sqlite', 'sharded']
    """
    return list(_backends.keys())
