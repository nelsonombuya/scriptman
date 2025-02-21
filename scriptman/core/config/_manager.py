from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, Optional

from scriptman.core.config._file_handler import FileHandler
from scriptman.core.config._store import ConfigStore
from scriptman.powers.generics import T


class ConfigManager(ConfigStore, ABC, Generic[T]):
    """🏗 Abstract base class for configuration managers with file persistence."""

    def __init__(
        self, file_path: Path, file_handler: FileHandler, section: Optional[str] = None
    ) -> None:
        """
        🚀 Initialize an ConfigManager instance.

        Args:
            file_path (Path): Path to the configuration file.
            file_handler (FileHandler): Handler for reading/writing the config file.
            section (Optional[str]): Section name for the configuration values.
        """
        super().__init__()
        object.__setattr__(self, "section", section)
        object.__setattr__(self, "file_path", file_path)
        object.__setattr__(self, "file_handler", file_handler)
        self._load_from_file()

    def set(self, key: str, value: Any, write_to_file: bool = False) -> None:
        """
        ✍🏾 Set a configuration value and optionally persist it.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to set.
            write_to_file (bool): Whether to write to file.
        """
        super().set(key, value)
        if write_to_file:
            self._write_to_file(key, value)

    def delete(self, key: str, write_to_file: bool = False) -> None:
        """
        🗑️ Delete a configuration value and optionally remove from file.

        Args:
            key (str): The key to delete.
            write_to_file (bool): Whether to remove from file.
        """
        super().delete(key)
        if write_to_file:
            self._remove_from_file(key)

    def reset(self, key: str, write_to_file: bool = False) -> None:
        """
        🔄 Reset a configuration value and optionally reset in file.

        Args:
            key (str): The key to reset.
            write_to_file (bool): Whether to reset in file (removes value from file).
        """
        super().reset(key)
        if write_to_file:
            self._remove_from_file(key)

    def reset_all(self, write_to_file: bool = False) -> None:
        """
        🔄 Reset all configuration values and optionally reset in file.

        Args:
            write_to_file (bool): Whether to reset in file (removes all values from file).
        """
        if write_to_file:
            for key in self.keys():
                self._remove_from_file(key)
        super().reset_all()

    @abstractmethod
    def _load_from_file(self) -> None:
        """📂 Load configuration data from file."""
        pass

    @abstractmethod
    def _write_to_file(self, key: str, value: Any) -> None:
        """📝 Write configuration to file."""
        pass

    @abstractmethod
    def _remove_from_file(self, key: str) -> None:
        """🗑️ Remove configuration from file."""
        pass

    @abstractmethod
    def get_section_data(self, data: T) -> dict[str, Any]:
        """
        📄 Extract section data from the configuration.

        Args:
            data (T): The configuration data.

        Returns:
            dict[str, Any]: The extracted section data.
        """
        pass

    @abstractmethod
    def update_section_data(self, data: T, section_data: dict[str, Any]) -> T:
        """
        📝 Update section data in the configuration.

        Args:
            data (T): The configuration data.
            section_data (dict[str, Any]): The new section data.

        Returns:
            T: The updated configuration data.
        """
        pass
