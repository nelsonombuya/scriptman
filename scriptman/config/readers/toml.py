"""📄 TOML configuration reader (optional, requires tomlkit)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin

from scriptman._internal import log
from scriptman.serialization import serialize

from .base import ConfigReader

if TYPE_CHECKING:
    from pydantic import BaseModel

# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONAL DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import tomlkit
    from tomlkit.toml_document import TOMLDocument
except ImportError as e:
    raise ImportError(
        "🚫 TOML support requires tomlkit. Install with:\n"
        "    pip install scriptman[toml]\n"
        "    # or\n"
        "    pip install tomlkit"
    ) from e


class TomlReader(ConfigReader):
    """📄 Read/write TOML configuration files.

    Requires the 'toml' extra: pip install scriptman[toml]

    Supports both standalone scriptman.toml and pyproject.toml [tool.scriptman].
    Uses tomlkit to preserve formatting and comments.

    Args:
        path: Path to TOML file.
        section: Section name for pyproject.toml (e.g., "scriptman").
            If None, reads entire file.

    Example:
        >>> reader = TomlReader(Path("scriptman.toml"))
        >>> reader = TomlReader(Path("pyproject.toml"), section="scriptman")
    """

    def __init__(self, path: Path, section: str | None = None) -> None:
        self._path = path
        self.__section = section

    @property
    def name(self) -> str:
        return "toml"

    @property
    def file_path(self) -> Path | None:
        return self._path

    def read(self) -> dict[str, Any]:
        """🔍 Read and parse TOML file."""
        if not self._path.exists():
            log.debug(f"📄 Config file not found: {self._path}")
            return {}

        try:
            data = tomlkit.parse(self._path.read_text(encoding="utf-8"))

            if self.__section:
                # Extract [tool.scriptman] section
                tool = data.get("tool", {})
                return dict(tool.get(self.__section, {}))

            return dict(data)
        except Exception as e:
            log.exception(f"⚠️ Failed to parse {self._path}: {e}")
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Write configuration to TOML file."""
        try:
            if self.__section:
                # Update [tool.scriptman] section in existing file
                if self._path.exists():
                    full_data = tomlkit.parse(self._path.read_text(encoding="utf-8"))
                else:
                    full_data = TOMLDocument()

                if "tool" not in full_data:
                    full_data["tool"] = tomlkit.table()

                # Type ignore for tomlkit's dynamic item assignment
                full_data["tool"][self.__section] = data  # type: ignore[index]
                content = tomlkit.dumps(full_data)
            else:
                content = tomlkit.dumps(data)

            # Clean up excessive newlines
            content = re.sub(r"\n{3,}", "\n\n", content)

            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(content, encoding="utf-8")
            log.debug(f"✍️ Wrote config to {self._path}")
        except Exception as e:
            log.exception(f"⚠️ Failed to write {self._path}: {e}")
            raise

    def supports_write(self) -> bool:
        return True

    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example TOML configuration from schema.

        Args:
            schema: The configuration schema to generate examples from.

        Returns:
            Example TOML configuration as a string.
        """
        doc = tomlkit.document()

        # Add header comments
        doc.add(tomlkit.comment(" Scriptman Configuration"))
        doc.add(
            tomlkit.comment(
                " All settings have sensible defaults - only override what you need"
            )
        )
        doc.add(tomlkit.nl())

        # Handle pyproject.toml format vs standalone
        if self.__section:
            # For pyproject.toml format
            doc.add(tomlkit.comment(" Add this to your pyproject.toml file:"))
            doc.add(tomlkit.nl())
            tool_table = tomlkit.table()
            scriptman_table = self._generate_config_table(schema)
            tool_table[self.__section] = scriptman_table
            doc["tool"] = tool_table
        else:
            # For standalone scriptman.toml
            self._add_sections_to_document(doc, schema)

        result = tomlkit.dumps(doc)
        return str(result) if not isinstance(result, str) else result

    def _generate_config_table(self, schema: type[BaseModel]) -> Any:
        """Generate the configuration table for the schema."""
        config_table = tomlkit.table()

        for field_name, field_info in schema.model_fields.items():
            annotation = field_info.annotation
            if annotation and hasattr(annotation, "model_fields"):
                # Nested configuration section
                section = tomlkit.table()
                for sub_name, sub_field in annotation.model_fields.items():
                    value = serialize(sub_field.get_default())
                    # TOML doesn't support None - skip None values
                    if value is not None:
                        section[sub_name] = value
                config_table[field_name] = section

        return config_table

    def _add_sections_to_document(self, doc: Any, schema: type[BaseModel]) -> None:
        """Add configuration sections to a TOML document."""
        for field_name, field_info in schema.model_fields.items():
            annotation = field_info.annotation

            if annotation and hasattr(annotation, "model_fields"):
                # Add section separator
                doc.add(tomlkit.nl())
                doc.add(tomlkit.comment(" " + "─" * 65))
                section_title = (
                    field_info.description or f"{field_name.title()} Configuration"
                )
                doc.add(tomlkit.comment(f" {section_title}"))
                doc.add(tomlkit.comment(" " + "─" * 65))
                doc.add(tomlkit.nl())
                doc.add(tomlkit.nl())

                # Create the section table
                section = tomlkit.table()
                doc[field_name] = section

                # Add fields to the section
                for sub_name, sub_field in annotation.model_fields.items():
                    # Add field description as comment
                    if sub_field.description:
                        section.add(tomlkit.comment(f" {sub_field.description}"))

                    # Check for Literal types to show options
                    origin = get_origin(sub_field.annotation)
                    if origin is Literal:
                        options = get_args(sub_field.annotation)
                        options_str = ", ".join(str(opt) for opt in options)
                        section.add(tomlkit.comment(f" Options: {options_str}"))

                    # Get default value and serialize for TOML
                    value = serialize(sub_field.get_default())
                    # TOML doesn't support None - skip with comment
                    if value is None:
                        section.add(tomlkit.comment(f" {sub_name} = <not set>"))
                    else:
                        section[sub_name] = value
                    section.add(tomlkit.nl())


def is_toml_available() -> bool:
    """🔍 Check if tomlkit is installed."""
    try:
        import tomlkit  # noqa: F401

        return True
    except ImportError:
        return False


__all__ = ["TomlReader", "is_toml_available"]
