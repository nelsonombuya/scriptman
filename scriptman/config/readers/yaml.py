"""📄 YAML configuration reader (optional, requires pyyaml)."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scriptman._internal import log

from .base import ConfigReader

if TYPE_CHECKING:
    from pydantic import BaseModel

# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONAL DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import yaml  # type: ignore[import-untyped]
except ImportError as e:
    raise ImportError(
        "🚫 YAML support requires pyyaml. Install with:\n"
        "    pip install scriptman[yaml]\n"
        "    # or\n"
        "    pip install pyyaml"
    ) from e


class YamlReader(ConfigReader):
    """📄 Read/write YAML configuration files.

    Requires the 'yaml' extra: pip install scriptman[yaml]

    Args:
        path: Path to YAML file.

    Example:
        >>> reader = YamlReader(Path("scriptman.yaml"))
        >>> reader = YamlReader(Path("scriptman.yml"))
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def name(self) -> str:
        return "yaml"

    @property
    def file_path(self) -> Path | None:
        return self._path

    def read(self) -> dict[str, Any]:
        """🔍 Read and parse YAML file."""
        if not self._path.exists():
            log.debug(f"📄 Config file not found: {self._path}")
            return {}

        try:
            with self._path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return dict(data) if data else {}
        except Exception as e:
            log.exception(f"⚠️ Failed to parse {self._path}: {e}")
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Write configuration to YAML file."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("w", encoding="utf-8") as f:
                yaml.dump(
                    data,
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                )
            log.debug(f"✍️ Wrote config to {self._path}")
        except Exception as e:
            log.exception(f"⚠️ Failed to write {self._path}: {e}")
            raise

    def supports_write(self) -> bool:
        return True

    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example YAML configuration from schema.

        Args:
            schema: The configuration schema to generate examples from.

        Returns:
            Example YAML configuration as a string.
        """
        from scriptman.serialization import serialize

        config: dict[str, Any] = {}

        for field_name, field_info in schema.model_fields.items():
            annotation = field_info.annotation
            if annotation and hasattr(annotation, "model_fields"):
                section: dict[str, Any] = {}
                for sub_name, sub_field in annotation.model_fields.items():
                    value = serialize(sub_field.get_default())
                    section[sub_name] = value
                config[field_name] = section

        output = StringIO()
        output.write("# Scriptman Configuration\n")
        output.write(
            "# All settings have sensible defaults - only override what you need\n\n"
        )
        yaml.dump(
            config,
            output,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
        return output.getvalue()


def is_yaml_available() -> bool:
    """🔍 Check if pyyaml is installed."""
    try:
        import yaml  # noqa: F401

        return True
    except ImportError:
        return False


__all__ = ["YamlReader", "is_yaml_available"]
