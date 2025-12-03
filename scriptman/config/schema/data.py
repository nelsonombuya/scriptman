"""📁 Data storage configuration schema."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Type alias for data categories
DataCategory = Literal["logs", "db", "cache", "artifacts"]


class DataConfig(BaseModel):
    """📁 Data storage configuration.

    Controls where Scriptman stores logs, databases, caches, and other
    generated files. Provides a centralized base directory with organized
    subdirectories.

    All paths are resolved relative to the current working directory unless
    absolute paths are provided.

    Attributes:
        dir: Base data directory (default: ".data")
        logs: Subdirectory for log files (default: "logs")
        db: Subdirectory for database files (default: "db")
        cache: Subdirectory for cache files (default: "cache")
        artifacts: Subdirectory for generated artifacts (default: "artifacts")

    Example:
        >>> config.get("data.dir")
        PosixPath('.data')

        # Full path resolution
        >>> config.resolve_path("logs")
        PosixPath('.data/logs')

        # Override in scriptman.toml:
        # [data]
        # dir = "C:/AppData/Scriptman"

    Directory Structure:
        .data/                  # data.dir (root)
        ├── logs/              # Logging output
        ├── db/                # SQLite databases
        ├── cache/             # Runtime caches
        └── artifacts/         # Generated files (reports, exports)
    """

    dir: Path = Field(
        default=Path(".data"),
        description="Base directory for all Scriptman data",
    )
    logs: Path = Field(
        default=Path("logs"),
        description="Subdirectory name for log files",
    )
    db: Path = Field(
        default=Path("db"),
        description="Subdirectory name for database files",
    )
    cache: Path = Field(
        default=Path("cache"),
        description="Subdirectory name for cache files",
    )
    artifacts: Path = Field(
        default=Path("artifacts"),
        description="Subdirectory name for generated artifacts",
    )

    @field_validator("dir", mode="before")
    @classmethod
    def _coerce_to_path(cls, v: str | Path) -> Path:
        """📁 Coerce string input to absolute Path.

        The base directory is resolved to an absolute path so that
        subdirectory paths can be joined correctly.

        Args:
            v: Path string or Path object

        Returns:
            Absolute resolved Path object
        """
        return Path(v).resolve()

    def get_path(self, category: DataCategory) -> Path:
        """📁 Get resolved path for a data category.

        Args:
            category: Data category

        Returns:
            Full resolved path for the category

        Example:
            >>> data_config = DataConfig()
            >>> data_config.get_path("logs")
            PosixPath('.data/logs')
            >>> data_config.get_path("db")
            PosixPath('.data/db')
        """
        subdirs: dict[DataCategory, Path] = {
            "logs": self.logs,
            "db": self.db,
            "cache": self.cache,
            "artifacts": self.artifacts,
        }
        return self.dir / subdirs[category]

    def ensure_path(self, category: DataCategory) -> Path:
        """📁 Get path for a category, creating directory if needed.

        Args:
            category: Data category

        Returns:
            Full resolved path (directory created if needed)

        Example:
            >>> data_config = DataConfig()
            >>> log_dir = data_config.ensure_path("logs")
            >>> log_dir.exists()
            True
        """
        full_path = self.get_path(category)
        full_path.mkdir(parents=True, exist_ok=True)
        return full_path


__all__ = ["DataCategory", "DataConfig"]
