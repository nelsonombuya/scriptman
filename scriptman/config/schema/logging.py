"""📋 Logging configuration schema."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class LoggingConfig(BaseModel):
    """📋 Logging configuration.

    Controls how Scriptman logs messages. Log file directory is determined
    by the data.dir + data.logs configuration (centralized in DataConfig).

    Attributes:
        level: Logging verbosity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Example:
        >>> config.get("logging.level")
        'INFO'

        # Log directory is resolved via:
        >>> config.resolve_path("logs")
        PosixPath('.data/logs')

    Note:
        Log file location is controlled by data.dir and data.logs settings.
        Use config.resolve_path("logs") or config.ensure_path("logs") to
        get the log directory.
    """

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging verbosity level",
    )
