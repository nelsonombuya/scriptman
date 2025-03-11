from pathlib import Path
from re import sub
from typing import Any, Optional, cast

from loguru import logger
from tomlkit import dumps, parse, table
from tomlkit.items import Table

from scriptman.core.config._file_handler import FileHandler
from scriptman.core.config._manager import ConfigManager


class TomlHandler(FileHandler):
    """📄 Handler for TOML file format."""

    def read(self, file_path: Path) -> dict[str, Any]:
        """
        📖 Read and parse a TOML file.

        Args:
            file_path (Path): The path to the TOML file to be read.

        Returns:
            dict[str, Any]: A dictionary representation of the parsed TOML file.
            Returns an empty dictionary if the file does not exist.
        """
        if not file_path.exists():
            logger.warning(f"File {file_path} does not exist")
            return {}
        return parse(file_path.read_text())

    def write(self, file_path: Path, data: dict[str, Any]) -> None:
        """
        ✍️ Write data to a TOML file.

        Args:
            file_path (Path): The path to the TOML file to be written.
            data (dict[str, Any]): The data to be written to the TOML file.
        """
        if not file_path.exists():
            logger.warning(f"File {file_path} does not exist")
            file_path.touch()
        with file_path.open("w", encoding="utf-8") as f:
            f.write(sub(r"\n{3,}", "\n\n", dumps(data)))


class TOMLConfigManager(ConfigManager[dict[str, Any]]):
    """🏗 TOML-specific configuration manager implementation."""

    def __init__(self, file_path: Path, section: Optional[str] = None):
        """
        🚀 Initialize a TOMLConfigManager instance.

        Args:
            file_path (Path): Path to the TOML configuration file.
            section (Optional[str]): The section name for specific configuration data
                extraction.
        """
        super().__init__(file_path, TomlHandler(), section)

    def _load_from_file(self) -> None:
        """📂 Load configuration data from TOML file."""
        try:
            data = self.file_handler.read(self.file_path)
            if self.section:
                self._store = self.get_section_data(data)
            else:
                self._store = data
        except Exception as e:
            logger.error(f"Failed to load from TOML file: {e}")
            self._store = {}

    def _write_to_file(self, key: str, value: Any) -> None:
        """📝 Write configuration to TOML file."""
        try:
            data = self.file_handler.read(self.file_path)

            # Pickle Path objects
            if isinstance(value, (Path,)):
                value = str(value)

            if self.section:
                section_data = self.get_section_data(data)
                section_data[key] = value
                data = self.update_section_data(data, section_data)
            else:
                data[key] = value

            self.file_handler.write(self.file_path, data)
            logger.debug(f"Written to TOML file: {key} = {value}")
        except Exception as e:
            logger.error(f"Failed to write to TOML file: {e}")

    def _remove_from_file(self, key: str) -> None:
        """🗑️ Remove configuration from TOML file."""
        try:
            if self.file_path.exists():
                data = self.file_handler.read(self.file_path)

                if self.section:
                    section_data = self.get_section_data(data)
                    if key in section_data:
                        del section_data[key]
                    data = self.update_section_data(data, section_data)
                else:
                    if key in data:
                        del data[key]

                self.file_handler.write(self.file_path, data)
        except Exception as e:
            logger.error(f"Failed to remove from TOML file: {e}")

    def get_section_data(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        📄 Extract section data from TOML configuration.

        Args:
            data (dict[str, Any]): The TOML configuration data.

        Returns:
            dict[str, Any]: The extracted section data.
        """
        if "tool" in data:
            tool = cast(Table, data["tool"])
            return dict(tool.get(self.section, table()))
        return {}

    def update_section_data(
        self, data: dict[str, Any], section_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        📝 Update section data in TOML configuration.

        Args:
            data (dict[str, Any]): The TOML configuration data.
            section_data (dict[str, Any]): The new section data.

        Returns:
            dict[str, Any]: The updated TOML configuration data.
        """
        tool = cast(Table, data["tool"]) if "tool" in data else table()
        assert self.section, "Section name must be specified"
        tool[self.section] = section_data
        data["tool"] = tool
        return data
