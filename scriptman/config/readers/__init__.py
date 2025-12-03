"""📖 Configuration readers with auto-discovery."""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from .base import ConfigReader
from .env import EnvVarReader
from .toml import TomlReader

__all__ = [
    "ConfigReader",
    "EnvVarReader",
    "TomlReader",
    "auto_discover_reader",
    "auto_discover_secrets_reader",
    "get_reader_for_file",
    "register_reader",
]

# Registry of file extensions to reader classes
READER_REGISTRY: dict[str, type[ConfigReader]] = {
    ".toml": TomlReader,
}


def register_reader(extension: str, reader_cls: type[ConfigReader]) -> None:
    """✍️ Register a custom reader for a file extension.

    Args:
        extension: File extension including dot (e.g., ".yaml")
        reader_cls: ConfigReader subclass to handle this extension

    Example:
        >>> from scriptman.config.readers import register_reader
        >>> register_reader(".yaml", YamlReader)
    """
    READER_REGISTRY[extension] = reader_cls
    logger.debug(f"📖 Registered {reader_cls.__name__} for {extension}")


def get_reader_for_file(path: Path, section: str | None = None) -> ConfigReader:
    """🔍 Get appropriate reader for a file based on extension.

    Args:
        path: Path to config file
        section: Optional section name (for pyproject.toml)

    Returns:
        ConfigReader instance for the file type

    Raises:
        ValueError: If no reader registered for extension
    """
    ext = path.suffix.lower()

    if ext not in READER_REGISTRY:
        raise ValueError(
            f"⚠️ No reader registered for '{ext}' files. "
            f"Supported: {', '.join(READER_REGISTRY.keys())}"
        )

    # Currently only TomlReader is registered
    # Future readers should also accept (path, section) parameters
    return TomlReader(path, section=section)


def auto_discover_reader() -> ConfigReader:
    """🔍 Auto-discover config file and return appropriate reader.

    Discovery order (uses most recently modified if multiple exist):
        1. scriptman.toml
        2. pyproject.toml [tool.scriptman]
        3. scriptman.yaml (if registered)
        4. scriptman.json (if registered)

    If no config file exists, returns TomlReader for scriptman.toml
    (will be created on first write).

    Returns:
        ConfigReader instance for discovered or default config file
    """
    cwd = Path.cwd()

    # Candidates in discovery order
    candidates: list[tuple[Path, str | None]] = [
        (cwd / "scriptman.toml", None),
        (cwd / "pyproject.toml", "scriptman"),
        (cwd / "scriptman.yaml", None),
        (cwd / "scriptman.json", None),
    ]

    # Find existing files with their modification times
    existing: list[tuple[Path, str | None, float]] = []

    for path, section in candidates:
        if path.exists():
            # Check if extension is supported
            if path.suffix.lower() in READER_REGISTRY:
                mtime = path.stat().st_mtime
                existing.append((path, section, mtime))

    if existing:
        # Sort by modification time (most recent first)
        existing.sort(key=lambda x: x[2], reverse=True)
        path, section, _ = existing[0]

        logger.debug(f"📖 Auto-discovered config: {path}")
        return get_reader_for_file(path, section)

    # No config found - use default (will create on first write)
    logger.debug("📖 No config file found, using default scriptman.toml")
    return TomlReader(cwd / "scriptman.toml")


def auto_discover_secrets_reader() -> ConfigReader:
    """🔍 Auto-discover secrets file and return appropriate reader.

    Discovery order (uses most recently modified if multiple exist):
        1. .secrets.toml
        2. .secrets.yaml (if registered)
        3. .secrets.json (if registered)

    Returns:
        ConfigReader instance for discovered or default secrets file
    """
    cwd = Path.cwd()

    candidates = [
        cwd / ".secrets.toml",
        cwd / ".secrets.yaml",
        cwd / ".secrets.json",
    ]

    existing: list[tuple[Path, float]] = []

    for path in candidates:
        if path.exists() and path.suffix.lower() in READER_REGISTRY:
            existing.append((path, path.stat().st_mtime))

    if existing:
        existing.sort(key=lambda x: x[1], reverse=True)
        path, _ = existing[0]
        logger.debug(f"🔐 Auto-discovered secrets: {path}")
        return get_reader_for_file(path)

    # Default to .secrets.toml
    return TomlReader(cwd / ".secrets.toml")
