from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class FileHandler(ABC):
    """📚 Abstract base class for handling different config file formats."""

    @abstractmethod
    def read(self, file_path: Path) -> dict[str, Any]:
        """
        📖 Read configuration from file.

        Args:
            file_path (Path): Path to the configuration file.

        Returns:
            dict[str, Any]: Configuration data.
        """
        pass

    @abstractmethod
    def write(self, file_path: Path, data: dict[str, Any]) -> None:
        """
        ✍️ Write configuration data to file.

        Args:
            file_path (Path): Path to the configuration file.
            data (dict[str, Any]): Configuration data to write.

        Returns:
            None
        """
        pass
