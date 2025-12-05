"""💾 SQLite cache backend — local file storage made simple.

This module provides a SQLite-backed cache implementation with:
- TTL expiration (lazy + active)
- Tags for grouped invalidation
- LRU eviction with size limits
- Stampede prevention

Usage:
    >>> from scriptman.cache.backends.sqlite import SQLiteCacheBackend
    >>> backend = SQLiteCacheBackend()
    >>> backend.set("key", "value", ttl=3600)
    >>> backend.get("key")
    'value'
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scriptman._internal import log
from scriptman.cache.backends import CacheBackend, CacheStats, register_backend
from scriptman.database.sqlite import SQLiteClient
from scriptman.serialization import serialize
from scriptman.types import StampedeLock

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════

CACHE_SCHEMA = """
-- Main cache table
CREATE TABLE IF NOT EXISTS cache (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL,
    size_bytes INTEGER NOT NULL,
    expires_at REAL,
    created_at REAL NOT NULL,
    accessed_at REAL NOT NULL
);

-- Tags table (many-to-many)
CREATE TABLE IF NOT EXISTS cache_tags (
    key TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (key, tag),
    FOREIGN KEY (key) REFERENCES cache(key) ON DELETE CASCADE
);

-- Stats table (singleton row)
CREATE TABLE IF NOT EXISTS cache_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    hits INTEGER DEFAULT 0,
    misses INTEGER DEFAULT 0
);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_cache_accessed ON cache(accessed_at);
CREATE INDEX IF NOT EXISTS idx_cache_tags_tag ON cache_tags(tag);

-- Initialize stats row
INSERT OR IGNORE INTO cache_stats (id, hits, misses) VALUES (1, 0, 0);
"""


# ═══════════════════════════════════════════════════════════════════════════════
# SQLITE CACHE BACKEND
# ═══════════════════════════════════════════════════════════════════════════════


class SQLiteCacheBackend(CacheBackend):
    """💾 SQLite-backed cache implementation.

    Stores cache data in a local SQLite database with support for:
    - TTL expiration (lazy deletion on access + active cleanup)
    - Tags for grouped invalidation
    - LRU eviction when size limits are reached
    - Stampede prevention via per-key locks

    Args:
        path: Path to SQLite database file (or None for default)
        max_size_bytes: Maximum total cache size (None = unlimited)
        eviction_batch_size: Number of entries to evict per LRU pass

    Example:
        >>> backend = SQLiteCacheBackend()
        >>> backend.set("user:123", {"name": "Alice"}, ttl=3600, tags=["users"])
        True
        >>> backend.get("user:123")
        {'name': 'Alice'}
        >>> backend.invalidate_tag("users")
        1
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        max_size_bytes: int | None = None,
        eviction_batch_size: int = 100,
    ) -> None:
        """🚀 Initialize SQLite cache backend.

        Args:
            path: Database file path (None = use config default)
            max_size_bytes: Max cache size in bytes (None = unlimited)
            eviction_batch_size: Entries to evict per LRU pass
        """
        # Resolve path from config if not provided
        if path is None:
            from scriptman import config

            cache_dir = config.ensure_path("cache")
            path = cache_dir / "cache.db"

        self._db = SQLiteClient(path, name="cache.db")
        self._max_size_bytes = max_size_bytes
        self._eviction_batch_size = eviction_batch_size
        self._stampede_lock = StampedeLock()
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """🔧 Ensure database schema is created."""
        if self._initialized:
            return

        self._db.execute_script(CACHE_SCHEMA)
        # Enable foreign keys for CASCADE delete
        self._db.set_pragma("foreign_keys", "ON")
        self._initialized = True
        log.debug("🗄️ Cache backend initialized", path=self._db.path)

    # ─────────────────────────────────────────────────────────────
    # Serialization Helpers
    # ─────────────────────────────────────────────────────────────

    def _encode_value(self, value: Any) -> bytes:
        """📦 Encode value for storage."""
        serializable = serialize(value)
        return json.dumps(serializable).encode("utf-8")

    def _decode_value(self, data: bytes) -> Any:
        """📦 Decode value from storage."""
        return json.loads(data.decode("utf-8"))

    # ─────────────────────────────────────────────────────────────
    # Core Operations
    # ─────────────────────────────────────────────────────────────

    def get(self, key: str) -> Any | None:
        """🔍 Retrieve value by key."""
        self._ensure_initialized()
        now = time.time()

        # Query with expiration check
        row = self._db.query_one(
            """
            SELECT value, expires_at FROM cache
            WHERE key = :key
            """,
            {"key": key},
        )

        if row is None:
            self._record_miss()
            log.event(f"Cache miss: {key}", "cache.miss", key=key)
            return None

        # Check expiration (lazy deletion)
        expires_at = row["expires_at"]
        if expires_at is not None and expires_at <= now:
            self.delete(key)
            self._record_miss()
            log.event(f"Cache expired: {key}", "cache.expire", key=key)
            return None

        # Update access time for LRU
        self._db.execute(
            "UPDATE cache SET accessed_at = :now WHERE key = :key",
            {"key": key, "now": now},
        )

        self._record_hit()
        log.event(f"Cache hit: {key}", "cache.hit", key=key)
        return self._decode_value(row["value"])

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> bool:
        """✍️ Store value with optional TTL and tags."""
        self._ensure_initialized()
        now = time.time()

        # Encode value
        encoded = self._encode_value(value)
        size_bytes = len(encoded)

        # Calculate expiration
        expires_at = (now + ttl) if ttl is not None else None

        # Upsert cache entry (execute() auto-commits, no transaction wrapper needed)
        self._db.execute(
            """
            INSERT INTO cache (key, value, size_bytes, expires_at, created_at, accessed_at)
            VALUES (:key, :value, :size_bytes, :expires_at, :created_at, :accessed_at)
            ON CONFLICT(key) DO UPDATE SET
                value = :value,
                size_bytes = :size_bytes,
                expires_at = :expires_at,
                accessed_at = :accessed_at
            """,
            {
                "key": key,
                "value": encoded,
                "size_bytes": size_bytes,
                "expires_at": expires_at,
                "created_at": now,
                "accessed_at": now,
            },
        )

        # Handle tags
        if tags:
            # Remove old tags
            self._db.execute(
                "DELETE FROM cache_tags WHERE key = :key",
                {"key": key},
            )
            # Insert new tags
            for tag in tags:
                self._db.execute(
                    "INSERT INTO cache_tags (key, tag) VALUES (:key, :tag)",
                    {"key": key, "tag": tag},
                )

        # Check size limit and evict if needed
        if self._max_size_bytes is not None:
            self._maybe_evict()

        log.event(
            f"Cache set: {key}",
            "cache.set",
            key=key,
            size_bytes=size_bytes,
            ttl=ttl,
            tags=tags,
        )
        return True

    def delete(self, key: str) -> bool:
        """🧹 Delete value by key."""
        self._ensure_initialized()

        # Tags are deleted via CASCADE
        affected = self._db.execute(
            "DELETE FROM cache WHERE key = :key",
            {"key": key},
        )

        if affected > 0:
            log.event(f"Cache delete: {key}", "cache.delete", key=key)

        return affected > 0

    def exists(self, key: str) -> bool:
        """🔍 Check if key exists and is not expired."""
        self._ensure_initialized()
        now = time.time()

        row = self._db.query_one(
            """
            SELECT 1 FROM cache
            WHERE key = :key
            AND (expires_at IS NULL OR expires_at > :now)
            """,
            {"key": key, "now": now},
        )
        return row is not None

    def clear(self) -> int:
        """🧹 Delete all cached values."""
        self._ensure_initialized()

        # Get count before clearing
        count_row = self._db.query_one("SELECT COUNT(*) as count FROM cache")
        count = count_row["count"] if count_row else 0

        # Clear tables
        self._db.execute("DELETE FROM cache_tags")
        self._db.execute("DELETE FROM cache")

        # Reset stats
        self._db.execute("UPDATE cache_stats SET hits = 0, misses = 0 WHERE id = 1")

        log.info(f"🧹 Cache cleared: {count} entries")
        return count

    # ─────────────────────────────────────────────────────────────
    # Bulk Operations
    # ─────────────────────────────────────────────────────────────

    def get_many(self, keys: list[str]) -> dict[str, Any]:
        """🔍 Retrieve multiple values by keys."""
        self._ensure_initialized()
        if not keys:
            return {}

        now = time.time()
        result: dict[str, Any] = {}
        expired_keys: list[str] = []

        # Query all keys at once
        placeholders = ", ".join(f":key{i}" for i in range(len(keys)))
        params: dict[str, Any] = {f"key{i}": key for i, key in enumerate(keys)}
        params["now"] = now

        rows = self._db.query(
            f"""
            SELECT key, value, expires_at FROM cache
            WHERE key IN ({placeholders})
            """,
            params,
        )

        for row in rows:
            key = row["key"]
            expires_at = row["expires_at"]

            if expires_at is not None and expires_at <= now:
                expired_keys.append(key)
                continue

            result[key] = self._decode_value(row["value"])

        # Clean up expired keys
        if expired_keys:
            self.delete_many(expired_keys)

        # Update access times
        if result:
            result_keys = list(result.keys())
            placeholders = ", ".join(f":key{i}" for i in range(len(result_keys)))
            update_params: dict[str, Any] = {
                f"key{i}": key for i, key in enumerate(result_keys)
            }
            update_params["now"] = now
            self._db.execute(
                f"UPDATE cache SET accessed_at = :now WHERE key IN ({placeholders})",
                update_params,
            )

        # Record stats
        hits = len(result)
        misses = len(keys) - hits
        self._record_hits(hits)
        self._record_misses(misses)

        return result

    def set_many(
        self,
        items: dict[str, Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """✍️ Store multiple values."""
        self._ensure_initialized()
        if not items:
            return 0

        count = 0
        for key, value in items.items():
            if self.set(key, value, ttl=ttl, tags=tags):
                count += 1

        return count

    def delete_many(self, keys: list[str]) -> int:
        """🧹 Delete multiple values."""
        self._ensure_initialized()
        if not keys:
            return 0

        placeholders = ", ".join(f":key{i}" for i in range(len(keys)))
        params = {f"key{i}": key for i, key in enumerate(keys)}

        # Tags deleted via CASCADE
        affected = self._db.execute(
            f"DELETE FROM cache WHERE key IN ({placeholders})",
            params,
        )

        if affected > 0:
            log.event(
                f"Cache bulk delete: {affected} keys",
                "cache.delete",
                count=affected,
            )

        return affected

    # ─────────────────────────────────────────────────────────────
    # Tag Operations
    # ─────────────────────────────────────────────────────────────

    def invalidate_tag(self, tag: str) -> int:
        """🧹 Delete all values with the given tag."""
        self._ensure_initialized()

        # Get keys with this tag
        rows = self._db.query(
            "SELECT key FROM cache_tags WHERE tag = :tag",
            {"tag": tag},
        )
        keys = [row["key"] for row in rows]

        if not keys:
            return 0

        affected = self.delete_many(keys)
        log.event(
            f"Cache tag invalidated: {tag}",
            "cache.invalidate",
            tag=tag,
            count=affected,
        )
        return affected

    def get_by_tag(self, tag: str) -> dict[str, Any]:
        """🔍 Retrieve all values with the given tag."""
        self._ensure_initialized()
        now = time.time()

        rows = self._db.query(
            """
            SELECT c.key, c.value, c.expires_at
            FROM cache c
            JOIN cache_tags t ON c.key = t.key
            WHERE t.tag = :tag
            """,
            {"tag": tag},
        )

        result: dict[str, Any] = {}
        for row in rows:
            expires_at = row["expires_at"]
            if expires_at is not None and expires_at <= now:
                continue
            result[row["key"]] = self._decode_value(row["value"])

        return result

    # ─────────────────────────────────────────────────────────────
    # Statistics
    # ─────────────────────────────────────────────────────────────

    def stats(self) -> CacheStats:
        """📊 Get cache statistics."""
        self._ensure_initialized()

        stats_row = self._db.query_one(
            "SELECT hits, misses FROM cache_stats WHERE id = 1"
        )
        size_row = self._db.query_one(
            "SELECT COALESCE(SUM(size_bytes), 0) as total, COUNT(*) as count FROM cache"
        )

        return CacheStats(
            hits=stats_row["hits"] if stats_row else 0,
            misses=stats_row["misses"] if stats_row else 0,
            size_bytes=size_row["total"] if size_row else 0,
            key_count=size_row["count"] if size_row else 0,
        )

    def _record_hit(self) -> None:
        """📊 Record a cache hit."""
        self._db.execute("UPDATE cache_stats SET hits = hits + 1 WHERE id = 1")

    def _record_miss(self) -> None:
        """📊 Record a cache miss."""
        self._db.execute("UPDATE cache_stats SET misses = misses + 1 WHERE id = 1")

    def _record_hits(self, count: int) -> None:
        """📊 Record multiple cache hits."""
        self._db.execute(
            "UPDATE cache_stats SET hits = hits + :count WHERE id = 1",
            {"count": count},
        )

    def _record_misses(self, count: int) -> None:
        """📊 Record multiple cache misses."""
        self._db.execute(
            "UPDATE cache_stats SET misses = misses + :count WHERE id = 1",
            {"count": count},
        )

    # ─────────────────────────────────────────────────────────────
    # Maintenance
    # ─────────────────────────────────────────────────────────────

    def cleanup_expired(self) -> int:
        """🧹 Remove all expired entries."""
        self._ensure_initialized()
        now = time.time()

        # Get expired keys (tags will be deleted via CASCADE)
        rows = self._db.query(
            "SELECT key FROM cache WHERE expires_at IS NOT NULL AND expires_at <= :now",
            {"now": now},
        )

        if not rows:
            return 0

        keys = [row["key"] for row in rows]
        affected = self.delete_many(keys)

        log.event(
            f"Cache cleanup: {affected} expired entries",
            "cache.expire",
            count=affected,
        )
        return affected

    def evict_lru(self, count: int) -> int:
        """🧹 Evict least recently used entries."""
        self._ensure_initialized()

        # Get LRU keys
        rows = self._db.query(
            """
            SELECT key FROM cache
            ORDER BY accessed_at ASC
            LIMIT :count
            """,
            {"count": count},
        )

        if not rows:
            return 0

        keys = [row["key"] for row in rows]
        affected = self.delete_many(keys)

        log.event(
            f"Cache LRU eviction: {affected} entries",
            "cache.evict",
            count=affected,
        )
        return affected

    def _maybe_evict(self) -> None:
        """🧹 Evict if over size limit."""
        if self._max_size_bytes is None:
            return

        size_row = self._db.query_one(
            "SELECT COALESCE(SUM(size_bytes), 0) as total FROM cache"
        )
        current_size = size_row["total"] if size_row else 0

        if current_size > self._max_size_bytes:
            self.evict_lru(self._eviction_batch_size)

    # ─────────────────────────────────────────────────────────────
    # Stampede Prevention
    # ─────────────────────────────────────────────────────────────

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        """🔐 Get value or compute and cache it (sync)."""
        # Fast path: check cache first
        value = self.get(key)
        if value is not None:
            return value

        # Slow path: acquire lock and compute
        with self._stampede_lock.acquire_sync(key):
            # Double-check after acquiring lock
            value = self.get(key)
            if value is not None:
                return value

            # Compute and cache
            value = factory()
            self.set(key, value, ttl=ttl, tags=tags)
            return value

    async def get_or_set_async(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        """🔐 Get value or compute and cache it (async)."""
        # Fast path: check cache first
        value = self.get(key)
        if value is not None:
            return value

        # Slow path: acquire lock and compute
        lock = await self._stampede_lock.acquire_async(key)
        async with lock:
            # Double-check after acquiring lock
            value = self.get(key)
            if value is not None:
                return value

            # Compute and cache (handle both sync and async factories)
            from scriptman.types import is_async

            if is_async(factory):
                value = await factory()
            else:
                value = factory()

            self.set(key, value, ttl=ttl, tags=tags)
            return value

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """🔌 Close database connection."""
        if self._db:
            self._db.close()


# Register the backend
register_backend("sqlite", SQLiteCacheBackend)

__all__ = ["SQLiteCacheBackend"]
