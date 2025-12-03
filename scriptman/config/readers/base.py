"""📖 Abstract base class for configuration readers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic import BaseModel


class ConfigReader(ABC):
    """📖 Base class for reading/writing configuration from any source.

    Subclasses implement format-specific logic (TOML, YAML, JSON, etc.).
    The Config class doesn't care about format - it just receives a dict.

    Built-in readers:
        - TomlReader: Reads .toml files (default)
        - EnvVarReader: Reads SCRIPTMAN_* environment variables

    Contributors can add:
        - YamlReader, JsonReader, RemoteReader, etc.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """🏷️ Reader identifier (e.g., 'toml', 'yaml', 'env')."""
        ...

    @property
    @abstractmethod
    def file_path(self) -> Path | None:
        """📁 Path to config file, or None for non-file readers."""
        ...

    @abstractmethod
    def read(self) -> dict[str, Any]:
        """🔍 Load configuration as a dictionary.

        Returns:
            Configuration data as nested dict.
        """
        ...

    @abstractmethod
    def write(self, data: dict[str, Any]) -> None:
        """✍️ Persist configuration data.

        Args:
            data: Configuration to write.

        Raises:
            NotImplementedError: If reader is read-only.
        """
        ...

    @abstractmethod
    def supports_write(self) -> bool:
        """🔍 Whether this reader supports persistence."""
        ...

    @abstractmethod
    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example configuration in this reader's format.

        Args:
            schema: The configuration schema to generate examples from.

        Returns:
            Example configuration as a string in the reader's format.
        """
        ...

    def exists(self) -> bool:
        """🔍 Whether the config source exists."""
        if self.file_path is None:
            return True  # Non-file readers always "exist"
        return self.file_path.exists()
