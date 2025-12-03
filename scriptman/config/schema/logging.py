"""📋 Logging configuration schema."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class LoggingConfig(BaseModel):
    """📋 Logging configuration.

    Controls how Scriptman logs messages.

    Attributes:
        level: Logging verbosity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        dir: Override log directory (None uses data.dir/data.logs)

    Example:
        >>> config.get("logging.level")
        'INFO'
        >>> config.get("logging.dir")  # None = use DataConfig
        None

        # When None, log directory resolves via DataConfig:
        >>> config.resolve_path("logs")
        PosixPath('.data/logs')

        # When set, overrides DataConfig:
        # [logging]
        # dir = "C:/MyLogs"
    """

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging verbosity level",
    )
    dir: Path | None = Field(
        default=None,
        description="Override log directory (None uses data.dir/data.logs)",
    )
