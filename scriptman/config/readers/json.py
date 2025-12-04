"""📄 JSON configuration reader (default, no dependencies)."""

from __future__ import annotations

from json import JSONDecodeError, dumps, load
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scriptman._internal import log

from .base import ConfigReader

if TYPE_CHECKING:
    from pydantic import BaseModel


class JsonReader(ConfigReader):
    """📄 Read/write JSON configuration files.

    This is the DEFAULT config reader as it requires no extra dependencies.
    Supports both standalone scriptman.json and reading from package.json.

    Args:
        path: Path to JSON file.
        section: Section name for package.json (e.g., "scriptman").
            If None, reads entire file.

    Example:
        >>> reader = JsonReader(Path("scriptman.json"))
        >>> reader = JsonReader(Path("package.json"), section="scriptman")
    """

    def __init__(self, path: Path, section: str | None = None) -> None:
        self._path = path
        self._section = section

    @property
    def name(self) -> str:
        return "json"

    @property
    def file_path(self) -> Path | None:
        return self._path

    def read(self) -> dict[str, Any]:
        """🔍 Read and parse JSON file."""
        if not self._path.exists():
            log.debug(f"📄 Config file not found: {self._path}")
            return {}

        try:
            with self._path.open("r", encoding="utf-8") as f:
                data = load(f)

            if self._section:
                return dict(data.get(self._section, {}))

            return dict(data)
        except JSONDecodeError as e:
            log.exception(f"⚠️ Failed to parse {self._path}: {e}")
            return {}
        except Exception as e:
            log.exception(f"⚠️ Failed to read {self._path}: {e}")
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Write configuration to JSON file."""
        try:
            if self._section:
                # Update section in existing file
                if self._path.exists():
                    with self._path.open("r", encoding="utf-8") as f:
                        full_data = load(f)
                else:
                    full_data = {}

                full_data[self._section] = data
                content = dumps(full_data, indent=2, ensure_ascii=False)
            else:
                content = dumps(data, indent=2, ensure_ascii=False)

            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(content + "\n", encoding="utf-8")
            log.debug(f"✍️ Wrote config to {self._path}")
        except Exception as e:
            log.exception(f"⚠️ Failed to write {self._path}: {e}")
            raise

    def supports_write(self) -> bool:
        return True

    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example JSON configuration from schema.

        Args:
            schema: The configuration schema to generate examples from.

        Returns:
            Example JSON configuration as a string.
        """
        from scriptman.serialization import serialize

        config: dict[str, Any] = {}

        for field_name, field_info in schema.model_fields.items():
            annotation = field_info.annotation
            if annotation and hasattr(annotation, "model_fields"):
                # Nested configuration section
                section: dict[str, Any] = {}
                for sub_name, sub_field in annotation.model_fields.items():
                    value = serialize(sub_field.get_default())
                    section[sub_name] = value
                config[field_name] = section

        if self._section:
            return dumps({self._section: config}, indent=2, ensure_ascii=False)
        return dumps(config, indent=2, ensure_ascii=False)


__all__ = ["JsonReader"]
