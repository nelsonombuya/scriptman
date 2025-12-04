"""📋 Logging configuration schema."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class LoggingConfig(BaseModel):
    """📋 Logging configuration.

    Controls how Scriptman logs messages to console and files.

    Attributes:
        level: Logging verbosity level (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL).
            TRACE is most verbose, CRITICAL is least verbose.
        format: Log format preset or custom loguru format string
        colorize: Enable colored console output
        diagnose: Include detailed exception diagnostics
        backtrace: Include full exception backtrace
        dir: Override log directory (None uses data.dir/data.logs)
        file_enabled: Enable file logging
        file_rotation: When to rotate log files (size or time)
        file_retention: How long to keep old log files
        file_compression: Compression format for rotated logs

    Format Presets:
        - "minimal"    : "{level.icon} {message}"
        - "simple"     : "{time:HH:mm:ss} | {level} | {message}" (default)
        - "detailed"   : Full context with module/function/line
        - "json"       : JSON format for log aggregation
        - "production" : Clean format without colors (for files)

    Custom Format Placeholders (loguru):
        - {time}      : Timestamp (e.g., {time:YYYY-MM-DD HH:mm:ss})
        - {level}     : Log level name
        - {level.icon}: Log level emoji
        - {message}   : Log message
        - {name}      : Module name
        - {function}  : Function name
        - {line}      : Line number
        - {file}      : File name
        - {extra}     : Extra data dict

    Raises:
        ValidationError: If level is not one of TRACE, DEBUG, INFO, WARNING,
            ERROR, or CRITICAL.
        ValidationError: If file_compression is not a supported format.

    Example (scriptman.toml):
        [logging]
        level = "DEBUG"
        format = "simple"
        colorize = true

        # File logging
        file_enabled = true
        file_rotation = "10 MB"
        file_retention = "7 days"

        # Or with custom format:
        format = "{time:HH:mm:ss} | {level.icon} | {message}"

    Example (code):
        >>> config.get("logging.level")
        'INFO'
        >>> config.get("logging.format")
        'simple'
    """

    level: Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging verbosity level (TRACE is most verbose)",
    )

    format: str = Field(
        default="simple",
        description=(
            "Log format: 'minimal', 'simple', 'detailed', 'json', 'production', "
            "or custom loguru format string"
        ),
    )

    colorize: bool = Field(
        default=True,
        description="Enable colored console output",
    )

    diagnose: bool = Field(
        default=True,
        description="Include detailed exception diagnostics",
    )

    backtrace: bool = Field(
        default=True,
        description="Include full exception backtrace",
    )

    dir: Path | None = Field(
        default=None,
        description="Override log directory (None uses data.dir/data.logs)",
    )

    file_enabled: bool = Field(
        default=False,
        description="Enable file logging",
    )

    file_rotation: str = Field(
        default="10 MB",
        description="When to rotate log files (size like '10 MB' or time like '1 day')",
    )

    file_retention: str = Field(
        default="7 days",
        description="How long to keep old log files",
    )

    file_compression: str = Field(
        default="zip",
        description="Compression format for rotated logs (zip, gz, bz2, xz, tar)",
    )
