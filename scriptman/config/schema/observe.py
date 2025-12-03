"""🔍 Observer configuration schema."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class ObserveStoreConfig(BaseModel):
    """💾 Observer storage configuration."""

    path: Path | None = Field(
        default=None,
        description="Override event store path (None uses data.dir/observe/events.db)",
    )
    max_events: int | None = Field(
        default=100000,
        description="Maximum number of events to retain (None for unlimited)",
    )
    retention_days: int | None = Field(
        default=30,
        description="Number of days to keep events (None for unlimited)",
    )


class ObserveConfig(BaseModel):
    """🔍 Observer configuration.

    Controls telemetry and observability features.

    Example:
        >>> from scriptman import config
        >>> config.get("observe.enabled")
        True
        >>> config.get("observe.store.path")  # None = use DataConfig
        None

        # When None, resolves to data.dir/observe/events.db
        # When set, uses the explicit path
    """

    enabled: bool = Field(
        default=True,
        description="Enable observation and telemetry",
    )
    store: ObserveStoreConfig = Field(
        default_factory=ObserveStoreConfig,
        description="Storage configuration",
    )


__all__ = ["ObserveConfig", "ObserveStoreConfig"]
