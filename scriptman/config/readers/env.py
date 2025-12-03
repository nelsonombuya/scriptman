"""🌍 Environment variable configuration reader."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin

from scriptman.serialization import serialize

from .base import ConfigReader

if TYPE_CHECKING:
    from pydantic import BaseModel


class EnvVarReader(ConfigReader):
    """🌍 Read configuration from SCRIPTMAN_* environment variables.

    Environment variables are mapped to config keys:
        SCRIPTMAN_LOGGING__LEVEL -> logging.level (double underscore for nesting)
        SCRIPTMAN_EXECUTION__CONCURRENT -> execution.concurrent

    This reader is READ-ONLY - it cannot write to environment.

    Example:
        >>> os.environ["SCRIPTMAN_LOGGING__LEVEL"] = "DEBUG"
        >>> reader = EnvVarReader()
        >>> reader.read()  # {"logging": {"level": "DEBUG"}}
    """

    PREFIX = "SCRIPTMAN_"

    @property
    def name(self) -> str:
        return "env"

    @property
    def file_path(self) -> Path | None:
        return None

    def read(self) -> dict[str, Any]:
        """🔍 Read all SCRIPTMAN_* environment variables."""
        result: dict[str, Any] = {}

        for key, value in os.environ.items():
            if not key.startswith(self.PREFIX):
                continue

            # Convert SCRIPTMAN_LOGGING__LEVEL -> logging.level
            config_key = key[len(self.PREFIX) :].lower().replace("__", ".")

            # Try to parse as appropriate type
            result[config_key] = self._parse_value(value)

        return self._unflatten(result)

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Not supported - environment is read-only."""
        raise NotImplementedError(
            "⚠️ EnvVarReader is read-only. Cannot write to environment variables."
        )

    def supports_write(self) -> bool:
        return False

    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example .env configuration from schema.

        Args:
            schema: The configuration schema to generate examples from.

        Returns:
            Example .env configuration as a string.
        """
        lines: list[str] = []

        # Add header
        lines.append("# Scriptman Environment Variables")
        lines.append("# Set these in your environment or in a .env file")
        lines.append("# Use double underscore (__) for nested values")
        lines.append("")

        # Process each field in the schema
        self._process_model(schema, lines, prefix="")

        return "\n".join(lines)

    def _process_model(
        self,
        model: type[BaseModel],
        lines: list[str],
        prefix: str,
        parent_description: str = "",
    ) -> None:
        """
        ✍🏾 Recursively process a model and its nested fields.

        Args:
            model: The model to process.
            lines: The list of lines to append the examples to.
            prefix: The prefix to use for the environment variable name.
            parent_description: The description of the parent model.

        Returns:
            None
        """
        for field_name, field_info in model.model_fields.items():
            annotation = field_info.annotation

            # Build the environment variable name
            if prefix:
                env_name = f"{prefix}__{field_name.upper()}"
            else:
                env_name = f"{self.PREFIX}{field_name.upper()}"

            # Check if this is a nested model
            if annotation and hasattr(annotation, "model_fields"):
                # Add section comment
                section_desc = (
                    field_info.description or f"{field_name.title()} configuration"
                )
                lines.append("")
                lines.append(f"# {section_desc}")
                # Recurse into nested model
                self._process_model(
                    annotation,
                    lines,
                    prefix=env_name if prefix else f"{self.PREFIX}{field_name.upper()}",
                    parent_description=section_desc,
                )
            else:
                # Regular field - add example
                default = field_info.get_default()

                # Add description as comment
                if field_info.description:
                    lines.append(f"# {field_info.description}")

                # Check for Literal types to show options
                origin = get_origin(annotation)
                if origin is Literal:
                    options = get_args(annotation)
                    options_str = ", ".join(str(opt) for opt in options)
                    lines.append(f"# Options: {options_str}")

                # Serialize and format for env file
                value = self._format_env_value(default)

                # Add the environment variable example
                lines.append(f"{env_name}={value}")
                lines.append("")

    @staticmethod
    def _format_env_value(value: Any) -> str:
        """🔄 Format a value for .env file output.

        Args:
            value: Any Python value

        Returns:
            String formatted for .env file
        """
        # Serialize complex types first (Path, datetime, etc.)
        value = serialize(value)

        # Format for env file
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            # Quote strings if they contain spaces
            return f'"{value}"' if " " in value else value

        return str(value)

    @staticmethod
    def _parse_value(value: str) -> Any:
        """🔄 Parse string value to appropriate Python type."""
        # Boolean
        if value.lower() in ("true", "1", "yes", "on"):
            return True
        if value.lower() in ("false", "0", "no", "off"):
            return False

        # Integer
        try:
            return int(value)
        except ValueError:
            pass

        # Float
        try:
            return float(value)
        except ValueError:
            pass

        # String
        return value

    @staticmethod
    def _unflatten(flat: dict[str, Any]) -> dict[str, Any]:
        """🔄 Convert flat dot-notation dict to nested dict."""
        result: dict[str, Any] = {}

        for key, value in flat.items():
            parts = key.split(".")
            current = result

            for part in parts[:-1]:
                current = current.setdefault(part, {})

            current[parts[-1]] = value

        return result
