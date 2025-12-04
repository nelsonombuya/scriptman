"""📖 Configuration readers with auto-discovery."""

from __future__ import annotations

from pathlib import Path

from scriptman._internal import log

from .base import ConfigReader
from .env import EnvVarReader
from .json import JsonReader

__all__ = [
    "ConfigReader",
    "EnvVarReader",
    "JsonReader",
    "auto_discover_reader",
    "auto_discover_secrets_reader",
    "get_reader_for_file",
    "register_reader",
    "is_format_available",
]


# ═══════════════════════════════════════════════════════════════════════════════
# READER REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

# Custom reader registry (for user-registered readers)
_custom_registry: dict[str, type[ConfigReader]] = {}


def _get_reader_registry() -> dict[str, type[ConfigReader]]:
    """🔍 Get reader registry with available readers."""
    registry: dict[str, type[ConfigReader]] = {
        ".json": JsonReader,  # Always available (stdlib)
    }

    # Optional: TOML support
    try:
        from .toml import TomlReader

        registry[".toml"] = TomlReader
    except ImportError:
        pass

    # Optional: YAML support
    try:
        from .yaml import YamlReader

        registry[".yaml"] = YamlReader
        registry[".yml"] = YamlReader
    except ImportError:
        pass

    # Merge custom registrations (custom takes precedence)
    registry.update(_custom_registry)

    return registry


def is_format_available(format_name: str) -> bool:
    """🔍 Check if a config format is available.

    Args:
        format_name: Format name ('json', 'toml', 'yaml', 'dotenv')

    Returns:
        True if the format's dependencies are installed

    Example:
        >>> is_format_available("json")
        True
        >>> is_format_available("toml")  # Only if tomlkit installed
        False
    """
    if format_name == "json":
        return True
    elif format_name == "toml":
        try:
            from .toml import is_toml_available

            return is_toml_available()
        except ImportError:
            return False
    elif format_name in ("yaml", "yml"):
        try:
            from .yaml import is_yaml_available

            return is_yaml_available()
        except ImportError:
            return False
    elif format_name == "dotenv":
        try:
            from .dotenv import is_dotenv_available

            return is_dotenv_available()
        except ImportError:
            return False
    return False


def register_reader(extension: str, reader_cls: type[ConfigReader]) -> None:
    """✍️ Register a custom reader for a file extension.

    Registered readers take precedence over built-in readers for the same extension.
    Registration persists for the lifetime of the process.

    Args:
        extension: File extension including dot (e.g., ".yaml")
        reader_cls: ConfigReader subclass to handle this extension

    Example:
        >>> from scriptman.config.readers import register_reader
        >>> register_reader(".custom", CustomReader)
        >>> # Now .custom files will use CustomReader
    """
    if not extension.startswith("."):
        raise ValueError(f"⚠️ Extension must start with '.': {extension}")

    _custom_registry[extension.lower()] = reader_cls
    log.debug(f"📖 Registered {reader_cls.__name__} for {extension}")


def get_reader_for_file(path: Path, section: str | None = None) -> ConfigReader:
    """🔍 Get appropriate reader for a file based on extension.

    Args:
        path: Path to config file
        section: Optional section name (for pyproject.toml/package.json)

    Returns:
        ConfigReader instance for the file type

    Raises:
        ValueError: If no reader available for extension
    """
    ext = path.suffix.lower()
    registry = _get_reader_registry()

    if ext not in registry:
        available = ", ".join(registry.keys())

        # Provide helpful installation hint
        hints: dict[str, str] = {
            ".toml": "pip install scriptman[toml]",
            ".yaml": "pip install scriptman[yaml]",
            ".yml": "pip install scriptman[yaml]",
        }

        if ext in hints:
            raise ValueError(
                f"⚠️ No reader available for '{ext}' files.\n"
                f"Install support with: {hints[ext]}\n"
                f"Currently available: {available}"
            )

        raise ValueError(
            f"⚠️ No reader registered for '{ext}' files. Available: {available}"
        )

    reader_cls = registry[ext]

    # Handle section parameter for built-in readers
    if ext == ".json":
        return JsonReader(path, section=section)
    elif ext == ".toml":
        from .toml import TomlReader

        return TomlReader(path, section=section)
    elif ext in (".yaml", ".yml"):
        from .yaml import YamlReader

        return YamlReader(path)

    # For custom readers, try to instantiate with section if supported
    # Most readers only take path, so we try that first
    try:
        return reader_cls(path, section=section)  # type: ignore[call-arg]
    except TypeError:
        # Fallback: reader doesn't accept section parameter
        return reader_cls(path)  # type: ignore[call-arg]


def auto_discover_reader() -> ConfigReader:
    """🔍 Auto-discover config file and return appropriate reader.

    Discovery order (uses first found):
        1. scriptman.json (default, no deps needed)
        2. scriptman.toml (if tomlkit installed)
        3. pyproject.toml [tool.scriptman] (if tomlkit installed)
        4. scriptman.yaml / scriptman.yml (if pyyaml installed)

    If no config file exists, returns JsonReader for scriptman.json
    (will be created on first write).

    Returns:
        ConfigReader instance for discovered or default config file
    """
    cwd = Path.cwd()
    registry = _get_reader_registry()

    # Candidates in discovery order (JSON first = default)
    candidates: list[tuple[Path, str | None]] = [
        (cwd / "scriptman.json", None),
    ]

    # Add TOML candidates if available
    if ".toml" in registry:
        candidates.extend(
            [
                (cwd / "scriptman.toml", None),
                (cwd / "pyproject.toml", "scriptman"),
            ]
        )

    # Add YAML candidates if available
    if ".yaml" in registry or ".yml" in registry:
        candidates.extend(
            [
                (cwd / "scriptman.yaml", None),
                (cwd / "scriptman.yml", None),
            ]
        )

    # Find first existing file
    for path, section in candidates:
        if path.exists() and path.suffix.lower() in registry:
            log.debug(f"📖 Auto-discovered config: {path}")
            return get_reader_for_file(path, section)

    # No config found - use default JSON (will create on first write)
    log.debug("📖 No config file found, using default scriptman.json")
    return JsonReader(cwd / "scriptman.json")


def auto_discover_secrets_reader() -> ConfigReader:
    """🔍 Auto-discover secrets file and return appropriate reader.

    Discovery order:
        1. .secrets.json (default, no deps needed)
        2. .secrets.toml (if tomlkit installed)
        3. .secrets.yaml (if pyyaml installed)
        4. .env (if python-dotenv installed)

    Returns:
        ConfigReader instance for discovered or default secrets file
    """
    cwd = Path.cwd()
    registry = _get_reader_registry()

    # Candidates in order
    candidates: list[Path] = [
        cwd / ".secrets.json",
    ]

    if ".toml" in registry:
        candidates.append(cwd / ".secrets.toml")

    if ".yaml" in registry or ".yml" in registry:
        candidates.extend(
            [
                cwd / ".secrets.yaml",
                cwd / ".secrets.yml",
            ]
        )

    # Check for .env file with dotenv support
    dotenv_available = False
    try:
        from .dotenv import DotEnvReader, is_dotenv_available

        dotenv_available = is_dotenv_available()
        if dotenv_available:
            env_file = cwd / ".env"
            if env_file.exists():
                log.debug(f"🔐 Auto-discovered secrets: {env_file}")
                return DotEnvReader(env_file)
    except ImportError:
        pass

    # Find first existing file
    for path in candidates:
        if path.exists() and path.suffix.lower() in registry:
            log.debug(f"🔐 Auto-discovered secrets: {path}")
            return get_reader_for_file(path)

    # Default to .secrets.json
    return JsonReader(cwd / ".secrets.json")
