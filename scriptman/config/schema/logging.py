"""📋 Logging configuration schema."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class LoggingConfig(BaseModel):
    """📋 Logging configuration.

    Controls how Scriptman logs messages and where log files are stored.

    Attributes:
        level: Logging verbosity level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        dir: Directory for log files

    Example:
        >>> config.get("logging.level")
        'INFO'
        >>> config.get("logging.dir")
        PosixPath('.logs')
    """

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging verbosity level",
    )
    dir: Path = Field(
        default=Path(".logs"),
        description="Directory for log files",
    )

    @field_validator("dir", mode="before")
    @classmethod
    def _coerce_to_path(cls, v: str | Path) -> Path:
        """📁 Coerce string input to Path (no side effects).

        Args:
            v: Path string or Path object

        Returns:
            Resolved Path object
        """
        return Path(v).resolve()

    def ensure_dir_exists(self) -> Path:
        """📁 Create logs directory if it doesn't exist.

        Call this method at runtime when initializing logging.

        Returns:
            Path to the logs directory (created if needed)

        Example:
            >>> logging_config = LoggingConfig()
            >>> log_dir = logging_config.ensure_dir_exists()
            >>> log_file = log_dir / "app.log"
        """
        self.dir.mkdir(parents=True, exist_ok=True)
        return self.dir
