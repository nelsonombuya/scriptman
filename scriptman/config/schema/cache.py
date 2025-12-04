"""🗄️ Cache configuration schema."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CacheConfig(BaseModel):
    """🗄️ Cache module configuration.

    Controls cache behavior including sharding, size limits, and TTL defaults.

    Attributes:
        shards: Number of SQLite shards for write performance (1 = no sharding)
        max_size_mb: Maximum cache size before LRU eviction (None = unlimited)
        default_ttl: Default TTL in seconds for cached values (None = forever)
        eviction_batch_size: Number of entries to evict per LRU pass

    Example:
        >>> config.get("cache.shards")
        1
        >>> config.get("cache.max_size_mb")
        None

        # Override in scriptman.toml:
        # [cache]
        # shards = 4
        # max_size_mb = 100
        # default_ttl = 3600
    """

    shards: int = Field(
        default=1,
        ge=1,
        description="Number of database shards (1 = no sharding)",
    )
    max_size_mb: int | None = Field(
        default=None,
        ge=1,
        description="Maximum cache size in MB (None = unlimited)",
    )
    default_ttl: int | None = Field(
        default=None,
        ge=1,
        description="Default TTL in seconds (None = forever)",
    )
    eviction_batch_size: int = Field(
        default=100,
        ge=1,
        description="Number of entries to evict per LRU pass",
    )


__all__ = ["CacheConfig"]


