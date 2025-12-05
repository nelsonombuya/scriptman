# 🗄️ Scriptman Cache Module — Implementation Plan

## Overview

This plan guides the implementation of Scriptman's Cache module — a SQLite-backed caching system with TTL, tags, LRU eviction, sharding, and stampede prevention.

**Primary Goal:** Create a production-ready cache that integrates with the Observer for full observability.

---

## 📋 Project Context

**Scriptman** is a Python utilities library for automation and production workloads. The Cache module provides:

- SQLite-backed persistent caching
- TTL expiration (lazy + active)
- Tags for grouped invalidation
- LRU eviction with size limits
- Sharding for performance
- Stampede prevention for parallel workloads
- Full Observer integration

**Location:** `scriptman/cache/`

**Dependencies:**
- `scriptman.database.SQLiteClient` (already implemented)
- `scriptman.observe` (already implemented)
- `scriptman.types` (already implemented - sync/async utilities)
- `scriptman.config` (already implemented)

---

## 📁 File Structure

```
scriptman/
├── cache/
│   ├── __init__.py       # Cache class, singleton, @result decorator
│   ├── backends/
│   │   ├── __init__.py   # CacheBackend ABC, backend registry
│   │   ├── sqlite.py     # SQLiteCacheBackend
│   │   └── sharded.py    # ShardedSQLiteBackend
│   └── stats.py          # CacheStats model
├── types.py              # ✅ Already exists - sync/async utilities
└── __init__.py           # Add lazy import for cache
```

---

## Step 1: CacheBackend ABC and Stats

**File:** `scriptman/cache/backends/__init__.py`

```python
"""🗄️ Cache backend abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class CacheStats:
    """📊 Cache statistics."""

    hits: int = 0
    misses: int = 0
    size_bytes: int = 0
    key_count: int = 0

    @property
    def hit_ratio(self) -> float:
        """📊 Calculate hit ratio (0.0 to 1.0)."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


@dataclass
class CacheEntry:
    """📦 Represents a cached item."""

    key: str
    value: Any
    tags: list[str] = field(default_factory=list)
    expires_at: datetime | None = None
    created_at: datetime | None = None
    accessed_at: datetime | None = None


class CacheBackend(ABC):
    """🗄️ Abstract base class for cache implementations.

    All cache backends must implement this interface. The default
    implementation is SQLiteCacheBackend.

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
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """🧹 Delete value by key.

        Args:
            key: Cache key

        Returns:
            True if key existed and was deleted
        """
        ...

    @abstractmethod
    def exists(self, key: str) -> bool:
        """🔍 Check if key exists and is not expired.

        Args:
            key: Cache key

        Returns:
            True if key exists and is valid
        """
        ...

    @abstractmethod
    def clear(self) -> int:
        """🧹 Delete all cached values.

        Returns:
            Number of items deleted
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
        """
        ...

    @abstractmethod
    def delete_many(self, keys: list[str]) -> int:
        """🧹 Delete multiple values.

        Args:
            keys: List of cache keys

        Returns:
            Number of items deleted
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
            factory: Callable that produces the value (sync or async)
            ttl: TTL for the cached value
            tags: Tags for the cached value

        Returns:
            Cached or freshly computed value
        """
        ...


# Backend registry for auto-discovery
_backends: dict[str, type[CacheBackend]] = {}


def register_backend(name: str, cls: type[CacheBackend]) -> None:
    """🏷️ Register a cache backend by name.

    Note: This is a function call (not a decorator) to preserve type
    information for strict type checkers like basedpyright.
    """
    if name in _backends:
        raise ValueError(f"Backend '{name}' is already registered")
    _backends[name] = cls


def unregister_backend(name: str) -> bool:
    """🧹 Unregister a cache backend by name."""
    if name in _backends:
        del _backends[name]
        return True
    return False


def get_backend(name: str) -> type[CacheBackend]:
    """🔍 Get a registered backend by name."""
    if name not in _backends:
        available = ", ".join(_backends.keys())
        raise ValueError(f"Unknown backend '{name}'. Available: {available}")
    return _backends[name]


__all__ = [
    "CacheBackend",
    "CacheStats",
    "CacheEntry",
    "register_backend",
    "unregister_backend",
    "get_backend",
    "list_backends",
]
```

---

## Step 2: SQLite Cache Backend

**File:** `scriptman/cache/backends/sqlite.py`

Create the main SQLite backend implementation with:

1. **Schema:**
   ```sql
   CREATE TABLE cache (
       key TEXT PRIMARY KEY,
       value BLOB NOT NULL,
       size_bytes INTEGER NOT NULL,
       expires_at REAL,          -- Unix timestamp
       created_at REAL NOT NULL,
       accessed_at REAL NOT NULL  -- For LRU
   );

   CREATE TABLE cache_tags (
       key TEXT NOT NULL,
       tag TEXT NOT NULL,
       PRIMARY KEY (key, tag),
       FOREIGN KEY (key) REFERENCES cache(key) ON DELETE CASCADE
   );

   CREATE TABLE cache_stats (
       id INTEGER PRIMARY KEY CHECK (id = 1),
       hits INTEGER DEFAULT 0,
       misses INTEGER DEFAULT 0
   );

   CREATE INDEX idx_cache_expires ON cache(expires_at);
   CREATE INDEX idx_cache_accessed ON cache(accessed_at);
   CREATE INDEX idx_cache_tags_tag ON cache_tags(tag);
   ```

2. **Key implementation details:**
   - Use `scriptman.database.SQLiteClient` for database operations
   - JSON serialization for values (safe, human-readable)
   - LRU tracking via `accessed_at` column updated on every `get()`
   - Use `UPDATE ... RETURNING` for atomic read+update
   - Thread-safe locks for stampede prevention (use `scriptman.types.StampedeLock`)
   - Observer integration for logging hits/misses

3. **Constructor parameters:**
   ```python
   def __init__(
       self,
       path: str | Path = ".data/cache/cache.db",
       max_size_bytes: int | None = None,
       eviction_batch_size: int = 100,
   ) -> None:
   ```

4. **Important methods:**
   - `get()` - Update accessed_at, check expiration, record hit/miss
   - `set()` - Upsert with tags, check size limit
   - `get_or_set()` - Use `StampedeLock` for stampede prevention
   - `evict_lru()` - Delete oldest by `accessed_at`
   - `cleanup_expired()` - Delete where `expires_at <= now`

---

## Step 3: Sharded Backend

**File:** `scriptman/cache/backends/sharded.py`

Create a sharded backend that distributes keys across multiple SQLite databases:

1. **Key distribution:** Use consistent hashing (`hash(key) % shard_count`)

2. **Constructor:**
   ```python
   def __init__(
       self,
       path: str | Path,
       shards: int = 4,
       max_size_bytes: int | None = None,
   ) -> None:
   ```

3. **Method delegation:**
   - Single-key ops: delegate to appropriate shard
   - Multi-key ops: group by shard, aggregate results
   - Tag ops: fan out to all shards
   - Stats: aggregate from all shards

---

## Step 4: Cache Manager and Decorator

**File:** `scriptman/cache/__init__.py`

Create the main `Cache` class with:

1. **Singleton pattern** (thread-safe)

2. **Auto-configuration from config:**
   ```python
   path = config.get("cache.path", ".data/cache")
   shards = config.get("cache.shards", 1)
   max_size_mb = config.get("cache.max_size_mb")
   ```

3. **Decorator that works with sync AND async:**
   ```python
   @cache.result(ttl=3600, tags=["api"])
   def fetch_sync(id: str) -> dict:
       return api.get(f"/data/{id}")

   @cache.result(ttl=3600, tags=["api"])
   async def fetch_async(id: str) -> dict:
       return await api.async_get(f"/data/{id}")
   ```

4. **Use `scriptman.types` utilities:**
   - `is_async()` to detect function type
   - `StampedeLock` for stampede prevention (sync and async)
   - Separate sync and async wrapper implementations

   ```python
   from functools import wraps
   from scriptman.types import is_async, StampedeLock, Func, P, R

   _stampede_locks = StampedeLock()

   def result(
       self,
       ttl: int | None = None,
       tags: list[str] | None = None,
       key_fn: Callable[..., str] | None = None,
   ) -> Callable[[Func[P, R]], Func[P, R]]:
       """🗄️ Decorator to cache function results."""

       def decorator(func: Func[P, R]) -> Func[P, R]:
           if is_async(func):
               @wraps(func)
               async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                   cache_key = key_fn(*args, **kwargs) if key_fn else self._generate_key(func, args, kwargs)

                   # Check cache first
                   cached = self.get(cache_key)
                   if cached is not None:
                       observe.debug("🗄️ Cache hit", key=cache_key)
                       return cached

                   # Stampede prevention: acquire async lock
                   lock = await _stampede_locks.acquire_async(cache_key)
                   async with lock:
                       # Double-check after acquiring lock
                       cached = self.get(cache_key)
                       if cached is not None:
                           return cached

                       # Compute and cache
                       result = await func(*args, **kwargs)
                       self.set(cache_key, result, ttl=ttl, tags=tags)
                       return result

               return async_wrapper
           else:
               @wraps(func)
               def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                   cache_key = key_fn(*args, **kwargs) if key_fn else self._generate_key(func, args, kwargs)

                   # Check cache first
                   cached = self.get(cache_key)
                   if cached is not None:
                       observe.debug("🗄️ Cache hit", key=cache_key)
                       return cached

                   # Stampede prevention: acquire sync lock
                   with _stampede_locks.acquire_sync(cache_key):
                       # Double-check after acquiring lock
                       cached = self.get(cache_key)
                       if cached is not None:
                           return cached

                       # Compute and cache
                       result = func(*args, **kwargs)
                       self.set(cache_key, result, ttl=ttl, tags=tags)
                       return result

               return sync_wrapper

       return decorator
   ```

5. **Key generation:**
   ```python
   def _generate_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
       """🔑 Generate cache key from function signature."""
       import hashlib
       import json

       key_parts = [
           func.__module__,
           func.__qualname__,
           json.dumps(args, sort_keys=True, default=str),
           json.dumps(kwargs, sort_keys=True, default=str),
       ]
       key_string = ":".join(key_parts)
       return hashlib.md5(key_string.encode()).hexdigest()
   ```

---

## Step 5: Update Main `__init__.py`

**File:** `scriptman/__init__.py`

Add lazy import for cache:

```python
from scriptman import cache
# or
from scriptman.cache import cache

# Usage
cache.set("key", value, ttl=3600)
cache.get("key")

@cache.result(ttl=3600)
def expensive_function(): ...
```

---

## Step 6: Config Schema Update

**File:** `scriptman/config/schema/__init__.py`

Add cache configuration section:

```python
class CacheConfig(BaseModel):
    """🗄️ Cache configuration."""

    path: str = ".data/cache"
    shards: int = 1
    max_size_mb: int | None = None
```

Add to `ConfigSchema`:
```python
cache: CacheConfig = Field(default_factory=CacheConfig)
```

---

## ✅ Testing Checklist

After implementation, verify with these tests:

```python
from scriptman import cache

# 1. Core operations
cache.set("key", "value", ttl=60)
assert cache.get("key") == "value"
assert cache.exists("key") is True
assert cache.delete("key") is True
assert cache.get("key") is None

# 2. Tags
cache.set("a", 1, tags=["group1"])
cache.set("b", 2, tags=["group1"])
cache.set("c", 3, tags=["group2"])
assert cache.invalidate_tag("group1") == 2
assert cache.get("a") is None
assert cache.get("c") == 3
assert cache.get_by_tag("group2") == {"c": 3}

# 3. Bulk operations
cache.set_many({"x": 1, "y": 2, "z": 3})
assert cache.get_many(["x", "y"]) == {"x": 1, "y": 2}
assert cache.delete_many(["x", "y"]) == 2

# 4. Decorator (sync)
call_count = 0

@cache.result(ttl=60, tags=["users"])
def get_user(id: int) -> dict:
    global call_count
    call_count += 1
    return {"id": id, "name": f"User {id}"}

assert get_user(1) == {"id": 1, "name": "User 1"}
assert call_count == 1
assert get_user(1) == {"id": 1, "name": "User 1"}
assert call_count == 1  # Cached, no recompute

# 5. Decorator (async)
import asyncio

async_call_count = 0

@cache.result(ttl=60, tags=["users"])
async def get_user_async(id: int) -> dict:
    global async_call_count
    async_call_count += 1
    await asyncio.sleep(0.01)
    return {"id": id, "name": f"User {id}"}

asyncio.run(get_user_async(2))
assert async_call_count == 1
asyncio.run(get_user_async(2))
assert async_call_count == 1  # Cached

# 6. Stats
stats = cache.stats()
assert stats.hits > 0
assert stats.hit_ratio > 0

# 7. LRU eviction
cache.evict_lru(10)

# 8. Cleanup
cache.cleanup_expired()
cache.clear()
```

---

## 📊 Implementation Summary

| Component                 | Lines (est.) | Complexity |
| ------------------------- | ------------ | ---------- |
| `backends/__init__.py`    | ~120         | Low        |
| `backends/sqlite.py`      | ~350         | Medium     |
| `backends/sharded.py`     | ~150         | Low        |
| `__init__.py`             | ~250         | Medium     |
| Config schema update      | ~15          | Low        |
| Main `__init__.py` update | ~5           | Low        |

**Total:** ~890 lines

---

## 🔗 Dependencies

- `scriptman.database.SQLiteClient` - Database operations
- `scriptman.observe` - Logging and telemetry
- `scriptman.types.StampedeLock` - Stampede prevention
- `scriptman.config` - Configuration access
- `json` - Serialization (stdlib)
- `hashlib` - Key generation (stdlib)
- `threading` - Sync locks (stdlib)
- `asyncio` - Async locks (stdlib)

---

## 📝 Notes

1. **LRU vs FIFO:** This implementation uses LRU (Least Recently Used) for eviction. Every `get()` updates the `accessed_at` timestamp.

2. **Stampede Prevention:** Uses per-key locks to prevent multiple threads from computing the same value simultaneously.

3. **Observer Integration:** All cache operations are logged via the Observer for full visibility.

4. **Sharding:** Optional feature for better write performance. Use when cache size is large or write-heavy.

5. **Size Limits:** When `max_size_bytes` is set, LRU eviction runs automatically after each `set()`.

---

## 💡 Future Backends

The `CacheBackend` ABC enables adding new backends without changing the API:

### MemoryCacheBackend (📋 Planned)

**File:** `scriptman/cache/backends/memory.py`

In-memory cache for testing and ephemeral storage:
- Fast: no disk I/O
- Ephemeral: lost on process exit
- Use case: unit tests, short-lived processes

```python
class MemoryCacheBackend(CacheBackend):
    """🧠 In-memory cache for testing."""

    def __init__(self, max_entries: int | None = None):
        self._store: dict[str, tuple[Any, float | None, list[str]]] = {}
        self._max_entries = max_entries
        # ... implement CacheBackend methods


# Register after class definition (preserves type information)
register_backend("memory", MemoryCacheBackend)
```

### RedisCacheBackend (💡 Future)

**File:** `scriptman/cache/backends/redis.py`

Distributed caching for multi-process/multi-server scenarios:
- Requires: `pip install scriptman[redis]`
- Use case: shared cache across workers

```python
class RedisCacheBackend(CacheBackend):
    """📡 Redis-backed distributed cache."""

    def __init__(self, url: str = "redis://localhost:6379"):
        import redis
        self._client = redis.Redis.from_url(url)
        # ... implement CacheBackend methods


# Register after class definition (preserves type information)
register_backend("redis", RedisCacheBackend)
```

### Backend Selection Strategy

| Scenario                    | Backend           | Why                  |
| --------------------------- | ----------------- | -------------------- |
| Unit tests                  | Memory            | Fast, no cleanup     |
| Local development           | SQLite            | Persistent, no setup |
| Production (single process) | SQLite or Sharded | Reliable, observable |
| Production (multi-process)  | Redis             | Shared state         |
