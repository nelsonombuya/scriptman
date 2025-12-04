# ⚙️ Scriptman Config Module — Compliance Review & Enhancement Plan

> **Status:** 🚧 In Progress
> **Location:** `scriptman/config/`
> **Tests:** `tests/test_config.py` (TO BE CREATED)
> **Coverage:** 0% → Target 95%+

---

## Overview

The Config module provides Scriptman's configuration management with multi-format support, priority chain resolution, and schema validation. This document reviews compliance with Scriptman development rules and outlines enhancements needed.

**Design Philosophy:** Configuration should work with zero setup for 80% of use cases while allowing full customization for power users.

**Key Changes in This Plan:**
1. **JSON as default** — No extra dependencies needed (stdlib)
2. **Optional format readers** — TOML, YAML, .env as extras
3. **Minimal core dependencies** — Only loguru + pydantic required
4. **Beginner-friendly** — Works out of the box, power users add extras

---

## 📦 Dependency Architecture

### Current State (Problem)
```toml
dependencies = [
    "loguru>=0.7.3",
    "pydantic>=2.0.0",
    "tomlkit>=0.13.2",  # ← Required, makes TOML mandatory
]
```

### Proposed State (Solution)
```toml
dependencies = [
    "loguru>=0.7.3",
    "pydantic>=2.0.0",
    # tomlkit removed from core - JSON is now default
]

[project.optional-dependencies]
# Config format readers
toml = ["tomlkit>=0.13.2"]
yaml = ["pyyaml>=6.0"]
dotenv = ["python-dotenv>=1.0"]

# Bundles
config-all = ["tomlkit>=0.13.2", "pyyaml>=6.0", "python-dotenv>=1.0"]

# Web/API features
api = ["fastapi>=0.100.0", "uvicorn>=0.23.0"]

# All optional features
all = [
    "tomlkit>=0.13.2",
    "pyyaml>=6.0",
    "python-dotenv>=1.0",
    "fastapi>=0.100.0",
    "uvicorn>=0.23.0",
]

# Development (includes toml for bump command)
dev = [
    "mypy>=1.13.0",
    "pytest>=8.3.4",
    "pytest-asyncio>=0.25.0",
    "pytest-cov>=6.0.0",
    "pytest-mock>=3.14.0",
    "ruff>=0.9.8",
    "tomlkit>=0.13.2",  # For dev bump command
]
```

### Installation Examples
```bash
# Minimal (JSON only) - perfect for beginners
pip install scriptman

# With TOML support
pip install scriptman[toml]

# With YAML support
pip install scriptman[yaml]

# With .env file support
pip install scriptman[dotenv]

# All config formats
pip install scriptman[config-all]

# Everything
pip install scriptman[all]
```

---

## 📁 New File Structure

```
scriptman/config/
├── __init__.py           # UPDATE: Thread-safe singleton, reload/dump/__repr__
├── readers/
│   ├── __init__.py       # UPDATE: New discovery order, handle missing deps
│   ├── base.py           # Keep as-is (ABC)
│   ├── json.py           # NEW: Default reader (stdlib, no deps)
│   ├── toml.py           # UPDATE: Lazy import, graceful error
│   ├── yaml.py           # NEW: Optional reader (requires pyyaml)
│   ├── env.py            # UPDATE: Fix emoji
│   └── dotenv.py         # NEW: .env file reader (requires python-dotenv)
└── schema/
    └── __init__.py       # UPDATE: Fix get_all_keys() recursion
```

---

## 📊 Compliance Status

### ✅ Fully Compliant Areas

| Rule                             | Status | Evidence                                                     |
| -------------------------------- | ------ | ------------------------------------------------------------ |
| **Emoji-Driven Docstrings**      | ✅      | Every docstring uses emoji prefixes (⚙️, 🔍, ✍️, 🔄, 📁, 🔐)       |
| **ABCs Over Protocols**          | ✅      | `ConfigReader` is ABC with `@abstractmethod`                 |
| **Variable Visibility**          | ✅      | `_store`, `_reader` (protected), `__section` (private)       |
| **Explicit Access Over Magic**   | ✅      | Uses `config.get("key")` not `config.key`                    |
| **Configuration Priority Chain** | ✅      | Override > Env > File > Schema defaults                      |
| **Pydantic Validation**          | ✅      | All schemas use Pydantic BaseModel                           |
| **Section Headers**              | ✅      | Uses `─` dividers for logical sections                       |
| **Internal Logging**             | ✅      | Uses `from scriptman._internal import log`                   |
| **Context Manager Pattern**      | ✅      | `temporary()` follows the exact pattern                      |
| **Observer Integration**         | ✅      | `_emit_event()` sends events to observe                      |
| **Data Directory Structure**     | ✅      | DataConfig defines `.data/{logs,db,cache,observe,artifacts}` |
| **Modern Type Hints**            | ✅      | Uses `dict[str, Any]`, `str                                  | None`, `Path | None` |
| **Docstring Quality**            | ✅      | Has Args, Returns, Examples, Raises sections                 |

---

## ❌ Non-Compliant / Missing Items

### 1. NOT a Thread-Safe Singleton ❌

**Current Implementation (line 563):**
```python
config = Config()  # Simple module-level instance
```

**Required per Rules:**
```python
from threading import Lock

class Config:
    _instance: "Config | None" = None
    _lock: Lock = Lock()
    __initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "Config":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, ..., _force_reinit: bool = False) -> None:
        if self.__initialized and not _force_reinit:
            return
        # ... initialization ...
        self.__initialized = True
```

**Impact:** Multiple `Config()` instantiations create separate instances with different state.

**Fix Priority:** HIGH

---

### 2. No Tests (0% Coverage) ❌

**Current state:**
```
tests/
├── test_internal_log.py
├── test_serialization.py
├── test_types.py
└── (NO test_config.py!)
```

**Required per Rules:** 95%+ coverage before marking module complete.

**Fix Priority:** CRITICAL

---

### 3. Incomplete `__all__` Export ⚠️

**Current:**
```python
__all__ = ["config", "Config", "ConfigSchema"]
```

**Should include all module-level functions:**
```python
__all__ = [
    # Singleton and classes
    "config",
    "Config",
    "ConfigSchema",
    # Reading & writing
    "get",
    "set",
    "reset",
    "reset_all",
    # Overrides
    "override",
    "temporary",
    "clear_overrides",
    # Reader management
    "use_reader",
    "migrate_to",
    "generate_example",
    # Path resolution
    "resolve_path",
    "ensure_path",
    # New features
    "reload",
    "dump",
]
```

**Fix Priority:** LOW

---

### 4. Schema `get_all_keys()` Only Goes One Level Deep ⚠️

**Problem:** Won't return deeply nested keys like `observe.store.path`.

**Fix Priority:** MEDIUM

---

### 5. Inconsistent Emoji in `env.py` ⚠️

**Location:** `scriptman/config/readers/env.py` line 97

**Current:** `✍🏾` (has skin tone modifier)
**Should be:** `✍️` (standard)

**Fix Priority:** LOW

---

### 6. Missing Helpful Features ⚠️

| Feature                 | Status    | Description                                 |
| ----------------------- | --------- | ------------------------------------------- |
| `reload()` method       | ❌ Missing | No way to hot-reload from file              |
| `dump()` / `to_dict()`  | ❌ Missing | No way to export entire config              |
| `__repr__` / `__str__`  | ❌ Missing | No debug representation                     |
| DataCategory validation | ⚠️ Poor    | Invalid category gives unhelpful `KeyError` |

**Fix Priority:** MEDIUM

---

## 🔧 Implementation Plan

### Phase 1: Dependency Architecture (Required for v3.0.0)

#### 1.1 Update `pyproject.toml`

Move `tomlkit` from core dependencies to optional extras:

```toml
[project]
dependencies = [
    "loguru>=0.7.3",
    "pydantic>=2.0.0",
    # tomlkit removed - JSON is now default
]

[project.optional-dependencies]
# Config format readers
toml = ["tomlkit>=0.13.2"]
yaml = ["pyyaml>=6.0"]
dotenv = ["python-dotenv>=1.0"]

# Bundle all config formats
config-all = ["tomlkit>=0.13.2", "pyyaml>=6.0", "python-dotenv>=1.0"]

# Web/API features
api = ["fastapi>=0.100.0", "uvicorn>=0.23.0"]

# All optional features
all = [
    "tomlkit>=0.13.2",
    "pyyaml>=6.0",
    "python-dotenv>=1.0",
    "fastapi>=0.100.0",
    "uvicorn>=0.23.0",
]

# Development (includes toml for bump command)
dev = [
    "mypy>=1.13.0",
    "pytest>=8.3.4",
    "pytest-asyncio>=0.25.0",
    "pytest-cov>=6.0.0",
    "pytest-mock>=3.14.0",
    "ruff>=0.9.8",
    "tomlkit>=0.13.2",
    "pyyaml>=6.0",
    "python-dotenv>=1.0",
]
```

---

### Phase 2: New Readers

#### Optional Dependency Pattern

**Approach:** Module-level import with try/except (not lazy import functions)

```python
# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONAL DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import yaml
except ImportError as e:
    raise ImportError(
        "🚫 YAML support requires pyyaml. Install with:\n"
        "    pip install scriptman[yaml]"
    ) from e
```

**Why module-level over lazy imports?**

| Lazy Import (`_get_yaml()`)          | Module-Level Import                        |
| ------------------------------------ | ------------------------------------------ |
| Fails at first method call           | **Fails at import** (fast, clear)          |
| Returns `Any` (loses typing)         | **Full typing** (type checker knows all)   |
| Requires helper function per module  | **No helper functions** needed             |
| Error happens deep in stack          | **Error at top** of stack (easier debug)   |

**How it works with auto-discovery:**

The `_get_reader_registry()` function wraps reader imports in `try/except`:

```python
def _get_reader_registry():
    registry = {".json": JsonReader}  # Always available

    try:
        from .yaml import YamlReader  # Fails if pyyaml not installed
        registry[".yaml"] = YamlReader
    except ImportError:
        pass  # Gracefully skip unavailable formats
```

This gives us the best of both worlds:
- **Direct import:** Clear error, full typing
- **Graceful degradation:** Missing formats just aren't registered

---

#### 2.1 Create `scriptman/config/readers/json.py` (Default Reader)

```python
"""📄 JSON configuration reader (default, no dependencies)."""

from __future__ import annotations

import json
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
                data = json.load(f)

            if self._section:
                return dict(data.get(self._section, {}))

            return dict(data)
        except json.JSONDecodeError as e:
            log.exception(f"⚠️ Failed to parse {self._path}: {e}")
            return {}
        except Exception as e:
            log.exception(f"⚠️ Failed to read {self._path}: {e}")
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Write configuration to JSON file."""
        try:
            if self._section:
                if self._path.exists():
                    with self._path.open("r", encoding="utf-8") as f:
                        full_data = json.load(f)
                else:
                    full_data = {}

                full_data[self._section] = data
                content = json.dumps(full_data, indent=2, ensure_ascii=False)
            else:
                content = json.dumps(data, indent=2, ensure_ascii=False)

            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(content + "\n", encoding="utf-8")
            log.debug(f"✍️ Wrote config to {self._path}")
        except Exception as e:
            log.exception(f"⚠️ Failed to write {self._path}: {e}")
            raise

    def supports_write(self) -> bool:
        return True

    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example JSON configuration from schema."""
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

        if self._section:
            return json.dumps({self._section: config}, indent=2, ensure_ascii=False)
        return json.dumps(config, indent=2, ensure_ascii=False)


__all__ = ["JsonReader"]
```

---

#### 2.2 Create `scriptman/config/readers/yaml.py` (Optional)

**Pattern:** Module-level import with try/except for clear error at import time and full typing.

```python
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
    import yaml
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
                data = yaml.safe_load(f)  # ✅ Fully typed!
            return dict(data) if data else {}
        except Exception as e:
            log.exception(f"⚠️ Failed to parse {self._path}: {e}")
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Write configuration to YAML file."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("w", encoding="utf-8") as f:
                yaml.dump(  # ✅ Fully typed!
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
        """📝 Generate example YAML configuration from schema."""
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
        output.write("# All settings have sensible defaults\n\n")
        yaml.dump(config, output, default_flow_style=False, allow_unicode=True, sort_keys=False)
        return output.getvalue()


def is_yaml_available() -> bool:
    """🔍 Check if pyyaml is installed."""
    try:
        import yaml  # noqa: F401
        return True
    except ImportError:
        return False


__all__ = ["YamlReader", "is_yaml_available"]
```

**Why module-level import?**
- **Fails fast:** Error at import time, not at first method call
- **Full typing:** Type checker knows `yaml.safe_load()` exists and its return type
- **Cleaner code:** No need for `_get_yaml()` helper functions
- **Still graceful:** The `_get_reader_registry()` in `__init__.py` catches the ImportError

---

#### 2.3 Create `scriptman/config/readers/dotenv.py` (Optional)

**Pattern:** Module-level import with try/except for clear error at import time and full typing.

```python
"""📄 .env file configuration reader (optional, requires python-dotenv)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from scriptman._internal import log

from .base import ConfigReader
from .env import EnvVarReader

if TYPE_CHECKING:
    from pydantic import BaseModel

# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONAL DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import dotenv
except ImportError as e:
    raise ImportError(
        "🚫 .env file support requires python-dotenv. Install with:\n"
        "    pip install scriptman[dotenv]\n"
        "    # or\n"
        "    pip install python-dotenv"
    ) from e


class DotEnvReader(ConfigReader):
    """📄 Read configuration from .env files.

    Requires the 'dotenv' extra: pip install scriptman[dotenv]

    This reader:
    1. Loads variables from a .env file
    2. Parses SCRIPTMAN_* variables like EnvVarReader
    3. Does NOT modify os.environ by default (safe for testing)

    Args:
        path: Path to .env file (default: .env in cwd).
        load_into_env: If True, loads into os.environ (default: False).

    Example:
        >>> reader = DotEnvReader(Path(".env"))
        >>> reader = DotEnvReader(Path(".secrets.env"))
    """

    def __init__(
        self,
        path: Path | None = None,
        load_into_env: bool = False,
    ) -> None:
        self._path = path or Path.cwd() / ".env"
        self._load_into_env = load_into_env

    @property
    def name(self) -> str:
        return "dotenv"

    @property
    def file_path(self) -> Path | None:
        return self._path

    def read(self) -> dict[str, Any]:
        """🔍 Read and parse .env file."""
        if not self._path.exists():
            log.debug(f"📄 .env file not found: {self._path}")
            return {}

        try:
            values = dotenv.dotenv_values(self._path)  # ✅ Fully typed!

            if self._load_into_env:
                dotenv.load_dotenv(self._path)  # ✅ Fully typed!

            # Filter and parse SCRIPTMAN_* variables
            result: dict[str, Any] = {}
            prefix = EnvVarReader.PREFIX

            for key, value in values.items():
                if not key.startswith(prefix) or value is None:
                    continue

                config_key = key[len(prefix):].lower().replace("__", ".")
                result[config_key] = EnvVarReader._parse_value(value)

            return EnvVarReader._unflatten(result)
        except Exception as e:
            log.exception(f"⚠️ Failed to read {self._path}: {e}")
            return {}

    def write(self, data: dict[str, Any]) -> None:
        """✍️ Write configuration to .env file."""
        try:
            lines: list[str] = [
                "# Scriptman Configuration",
                "# Generated by scriptman",
                "",
            ]

            flat = self._flatten_to_env(data)
            for key, value in flat.items():
                lines.append(f"{key}={value}")

            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            log.debug(f"✍️ Wrote config to {self._path}")
        except Exception as e:
            log.exception(f"⚠️ Failed to write {self._path}: {e}")
            raise

    def supports_write(self) -> bool:
        return True

    def generate_example(self, schema: type[BaseModel]) -> str:
        """📝 Generate example .env configuration."""
        return EnvVarReader().generate_example(schema)

    @staticmethod
    def _flatten_to_env(data: dict[str, Any], prefix: str = "") -> dict[str, str]:
        """🔄 Flatten nested dict to SCRIPTMAN_* env vars."""
        result: dict[str, str] = {}
        base = EnvVarReader.PREFIX

        for key, value in data.items():
            env_key = f"{prefix}__{key.upper()}" if prefix else f"{base}{key.upper()}"

            if isinstance(value, dict):
                result.update(DotEnvReader._flatten_to_env(value, env_key))
            else:
                result[env_key] = EnvVarReader._format_env_value(value)

        return result


def is_dotenv_available() -> bool:
    """🔍 Check if python-dotenv is installed."""
    try:
        import dotenv  # noqa: F401
        return True
    except ImportError:
        return False


__all__ = ["DotEnvReader", "is_dotenv_available"]
```

---

#### 2.4 Update `scriptman/config/readers/toml.py` (Module-Level Import)

**Pattern:** Module-level import with try/except for clear error at import time and full typing.

```python
"""📄 TOML configuration reader (optional, requires tomlkit)."""

from __future__ import annotations

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
    """

    def __init__(self, path: Path, section: str | None = None) -> None:
        self._path = path
        self.__section = section

    # ... methods use `tomlkit` directly with full typing ...

    def read(self) -> dict[str, Any]:
        """🔍 Read and parse TOML file."""
        if not self._path.exists():
            log.debug(f"📄 Config file not found: {self._path}")
            return {}

        try:
            data = tomlkit.parse(self._path.read_text(encoding="utf-8"))  # ✅ Fully typed!
            # ...
        except Exception as e:
            log.exception(f"⚠️ Failed to parse {self._path}: {e}")
            return {}


def is_toml_available() -> bool:
    """🔍 Check if tomlkit is installed."""
    try:
        import tomlkit  # noqa: F401
        return True
    except ImportError:
        return False


__all__ = ["TomlReader", "is_toml_available"]
```

**Note:** The full implementation is already in place; just replace the lazy `_get_tomlkit()` calls with direct `tomlkit` usage.

---

#### 2.5 Update `scriptman/config/readers/__init__.py` (New Discovery Order)

```python
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
            f"⚠️ No reader registered for '{ext}' files. "
            f"Available: {available}"
        )

    if ext == ".json":
        return JsonReader(path, section=section)
    elif ext == ".toml":
        from .toml import TomlReader
        return TomlReader(path, section=section)
    elif ext in (".yaml", ".yml"):
        from .yaml import YamlReader
        return YamlReader(path)

    return registry[ext](path)  # type: ignore[call-arg]


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

    if ".toml" in registry:
        candidates.extend([
            (cwd / "scriptman.toml", None),
            (cwd / "pyproject.toml", "scriptman"),
        ])

    if ".yaml" in registry or ".yml" in registry:
        candidates.extend([
            (cwd / "scriptman.yaml", None),
            (cwd / "scriptman.yml", None),
        ])

    # Find first existing file
    for path, section in candidates:
        if path.exists() and path.suffix.lower() in registry:
            log.debug(f"📖 Auto-discovered config: {path}")
            return get_reader_for_file(path, section)

    # No config found - use default JSON
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

    candidates: list[Path] = [cwd / ".secrets.json"]

    if ".toml" in registry:
        candidates.append(cwd / ".secrets.toml")

    if ".yaml" in registry or ".yml" in registry:
        candidates.extend([cwd / ".secrets.yaml", cwd / ".secrets.yml"])

    # Check for .env file
    try:
        from .dotenv import is_dotenv_available, DotEnvReader
        if is_dotenv_available():
            env_file = cwd / ".env"
            if env_file.exists():
                log.debug(f"🔐 Auto-discovered secrets: {env_file}")
                return DotEnvReader(env_file)
    except ImportError:
        pass

    for path in candidates:
        if path.exists() and path.suffix.lower() in registry:
            log.debug(f"🔐 Auto-discovered secrets: {path}")
            return get_reader_for_file(path)

    return JsonReader(cwd / ".secrets.json")


def register_reader(extension: str, reader_cls: type[ConfigReader]) -> None:
    """✍️ Register a custom reader for a file extension."""
    log.debug(f"📖 Registered {reader_cls.__name__} for {extension}")
```

---

### Phase 3: Core Fixes

#### 3.1 Update `scriptman/config/__init__.py` (Thread-Safe Singleton + Features)

**Key changes:**
1. Thread-safe singleton with `_lock` and `__initialized`
2. Add `reload()` method
3. Add `dump()` method
4. Add `__repr__()` and `__str__()`
5. Expand `__all__` exports

```python
# Add to imports:
from threading import Lock

# Update class:
class Config:
    """⚙️ Scriptman configuration manager."""

    # Thread-Safe Singleton
    _instance: "Config | None" = None
    _lock: Lock = Lock()
    __initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> "Config":
        """🔒 Thread-safe singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        reader: ConfigReader | None = None,
        *,
        _is_secrets: bool = False,
        _force_reinit: bool = False,
    ) -> None:
        """🚀 Initialize configuration manager."""
        if self.__initialized and not _force_reinit:
            return
        # ... existing init code ...
        self.__initialized = True

    def __repr__(self) -> str:
        """🔍 Debug representation."""
        return (
            f"Config(reader={self._reader.name!r}, "
            f"file={self._reader.file_path}, "
            f"keys={len(self.keys())}, "
            f"overrides={len(self._overrides)})"
        )

    def __str__(self) -> str:
        """📝 Human-readable summary."""
        return f"Scriptman Config ({self._reader.name}: {self._reader.file_path})"

    def dump(self) -> dict[str, Any]:
        """📤 Export entire configuration as nested dict."""
        result: dict[str, Any] = {}
        for key in self.keys():
            value = self.get(key)
            self._set_nested(result, key, value)
        return result

    def reload(self) -> None:
        """🔄 Reload configuration from file."""
        self._store = self._reader.read()
        log.debug(f"🔄 Reloaded config from {self._reader.name}")
        self._emit_event(
            "Config reloaded",
            event_type="config.reloaded",
            reader=self._reader.name,
        )


# Update __all__:
__all__ = [
    "config", "Config", "ConfigSchema",
    "get", "set", "reset", "reset_all",
    "override", "temporary", "clear_overrides",
    "use_reader", "migrate_to", "generate_example",
    "resolve_path", "ensure_path",
    "reload", "dump",
]

# Add module-level exports:
reload = config.reload
dump = config.dump
```

---

#### 3.2 Update `scriptman/config/schema/__init__.py` (Deep Key Recursion)

```python
@classmethod
def get_all_keys(cls, prefix: str = "") -> list[str]:
    """🔍 Get all config keys (recursive for deep nesting)."""
    keys: list[str] = []
    for name, field in cls.model_fields.items():
        full_key = f"{prefix}.{name}" if prefix else name
        annotation = field.annotation

        try:
            if annotation is not None and isinstance(annotation, type):
                if issubclass(annotation, BaseModel):
                    # RECURSIVE call
                    nested_keys = _get_all_keys_from_model(annotation, full_key)
                    keys.extend(nested_keys)
                else:
                    keys.append(full_key)
            else:
                keys.append(full_key)
        except TypeError:
            keys.append(full_key)
    return keys


def _get_all_keys_from_model(model: type[BaseModel], prefix: str = "") -> list[str]:
    """🔍 Recursively get all keys from a Pydantic model."""
    keys: list[str] = []
    for name, field in model.model_fields.items():
        full_key = f"{prefix}.{name}" if prefix else name
        annotation = field.annotation

        try:
            if annotation is not None and isinstance(annotation, type):
                if issubclass(annotation, BaseModel):
                    nested_keys = _get_all_keys_from_model(annotation, full_key)
                    keys.extend(nested_keys)
                else:
                    keys.append(full_key)
            else:
                keys.append(full_key)
        except TypeError:
            keys.append(full_key)
    return keys
```

---

#### 3.3 Update `scriptman/config/readers/env.py` (Fix Emoji)

Line 97: Change `✍🏾` to `✍️`

---

#### 3.4 Update `scriptman/cli/dev/bump.py` (Handle Missing tomlkit)

**Note:** Unlike readers, CLI commands use lazy import pattern because they need to:
1. Return a graceful exit code (1) instead of crashing
2. Show a helpful error message via observe
3. Keep the CLI usable even if optional deps aren't installed

```python
def _get_tomlkit() -> tuple[Any, Any]:
    """🔍 Lazy import for tomlkit."""
    try:
        from tomlkit import dumps, parse
        return dumps, parse
    except ImportError:
        raise ImportError(
            "🚫 Version bumping requires tomlkit. Install with:\n"
            "    pip install scriptman[toml]\n"
            "    # or\n"
            "    pip install scriptman[dev]"
        ) from None


def run_bump(level: str) -> int:
    try:
        dumps, parse = _get_tomlkit()
    except ImportError as e:
        observe.error(str(e))
        return 1  # Graceful exit, not crash
    # ... rest of implementation
```

---

## 🧪 Test Plan

### Test File Structure

```python
# tests/test_config.py

class TestConfigSingleton:
    """🔒 Test thread-safe singleton behavior."""
    def test_same_instance_returned(self): ...
    def test_thread_safe_initialization(self): ...
    def test_force_reinit_works(self): ...

class TestConfigGet:
    """🔍 Test config.get() functionality."""
    def test_get_existing_key(self): ...
    def test_get_missing_key_with_default(self): ...
    def test_get_nested_key(self): ...
    def test_get_deeply_nested_key(self): ...

class TestConfigSet:
    """✍️ Test config.set() functionality."""
    def test_set_valid_value(self): ...
    def test_set_invalid_type_raises(self): ...
    def test_set_persists_to_file(self): ...
    def test_set_no_persist_option(self): ...

class TestConfigPriorityChain:
    """⚡ Test configuration priority resolution."""
    def test_override_beats_env(self): ...
    def test_env_beats_file(self): ...
    def test_file_beats_default(self): ...

class TestConfigOverrides:
    """🔄 Test override functionality."""
    def test_override_sets_value(self): ...
    def test_temporary_context_manager(self): ...
    def test_temporary_restores_on_exception(self): ...

class TestConfigNewFeatures:
    """🆕 Test new features."""
    def test_reload_updates_store(self): ...
    def test_dump_returns_nested_dict(self): ...
    def test_repr_shows_state(self): ...

class TestJsonReader:
    """📄 Test JSON reader (default)."""
    def test_read_existing_file(self): ...
    def test_read_missing_file_returns_empty(self): ...
    def test_write_creates_file(self): ...
    def test_section_support(self): ...

class TestOptionalReaders:
    """📖 Test optional format readers."""
    def test_toml_unavailable_error(self): ...
    def test_yaml_unavailable_error(self): ...
    def test_dotenv_unavailable_error(self): ...

class TestAutoDiscovery:
    """🔍 Test auto-discovery logic."""
    def test_discovers_json_first(self): ...
    def test_discovers_toml_if_available(self): ...
    def test_defaults_to_json_when_none_exist(self): ...

class TestConfigSchema:
    """📋 Test schema functionality."""
    def test_get_all_keys_deeply_nested(self): ...
    def test_get_field_info(self): ...

class TestEdgeCases:
    """🔬 Test edge cases."""
    def test_unicode_values(self): ...
    def test_empty_string_value(self): ...
    def test_special_characters(self): ...
```

### Fixtures

```python
@pytest.fixture
def temp_json_config(tmp_path):
    """Create a temporary JSON config file."""
    config_path = tmp_path / "scriptman.json"
    config_path.write_text('{"logging": {"level": "DEBUG"}}')
    return config_path

@pytest.fixture
def fresh_config(tmp_path, monkeypatch):
    """Create a fresh Config instance for testing."""
    monkeypatch.chdir(tmp_path)
    # Reset singleton
    Config._instance = None
    Config._Config__initialized = False
    return Config(_force_reinit=True)
```

---

## 📋 Summary of All Changes

| File                   | Change Type | Description                                                                 |
| ---------------------- | ----------- | --------------------------------------------------------------------------- |
| `pyproject.toml`       | **UPDATE**  | Move tomlkit to optional, add yaml/dotenv extras                            |
| `readers/json.py`      | **NEW**     | Default JSON reader (stdlib)                                                |
| `readers/yaml.py`      | **NEW**     | Optional YAML reader                                                        |
| `readers/dotenv.py`    | **NEW**     | Optional .env file reader                                                   |
| `readers/toml.py`      | **UPDATE**  | Lazy import, graceful error                                                 |
| `readers/__init__.py`  | **UPDATE**  | New discovery order, handle missing deps                                    |
| `readers/env.py`       | **UPDATE**  | Fix emoji (✍🏾 → ✍️)                                                           |
| `config/__init__.py`   | **UPDATE**  | Thread-safe singleton, `reload()`, `dump()`, `__repr__`, expanded `__all__` |
| `schema/__init__.py`   | **UPDATE**  | Fix `get_all_keys()` deep recursion                                         |
| `cli/dev/bump.py`      | **UPDATE**  | Handle missing tomlkit                                                      |
| `tests/test_config.py` | **NEW**     | Comprehensive test suite (95%+ coverage)                                    |

---

## 🎯 User Experience

### Beginner (Zero Config)
```python
from scriptman import config

# Works immediately with JSON (no extra deps)
config.set("logging.level", "DEBUG")
# Creates scriptman.json automatically
```

### Power User (TOML)
```bash
pip install scriptman[toml]
```
```python
# Now auto-discovers scriptman.toml or pyproject.toml
config.get("logging.level")
```

### Enterprise (Full Stack)
```bash
pip install scriptman[config-all]
```
```python
# Supports JSON, TOML, YAML, and .env files
# Auto-discovers based on what exists
```

---

## ✅ Compliance Checklist

After implementation, verify:

- [ ] Thread-safe singleton pattern implemented
- [ ] JSON reader as default (no extra deps)
- [ ] TOML/YAML/dotenv as optional extras
- [ ] Graceful error messages when optional deps missing
- [ ] Zero-config usage works for common case
- [ ] Type hints complete (modern 3.12+ syntax)
- [ ] Docstring with emoji, example, and error docs
- [ ] Error handling with helpful messages
- [ ] Tests written with 95%+ coverage
- [ ] Edge cases handled (unicode, empty, long values)
- [ ] Observer integration for telemetry
- [ ] `reload()`, `dump()`, `__repr__` implemented
- [ ] Deep key enumeration in `get_all_keys()`
- [ ] Update `.migration/FEATURE_GUIDE.md` with status

---

## 📝 Changelog

### v3.0.0 (Planned)

**Added:**
- JSON reader as default (no dependencies)
- YAML reader (optional, requires scriptman[yaml])
- DotEnv reader (optional, requires scriptman[dotenv])
- `config.reload()` for hot-reloading
- `config.dump()` for config export
- `config.__repr__()` for debugging
- `is_format_available()` utility
- Thread-safe singleton pattern

**Changed:**
- TOML now optional (requires scriptman[toml])
- Auto-discovery prefers JSON first
- Deep key enumeration in `get_all_keys()`

**Fixed:**
- Emoji inconsistency in env.py
- Incomplete `__all__` exports

---

## 🔗 References

- **Module:** `scriptman/config/`
- **Schema:** `scriptman/config/schema/`
- **Readers:** `scriptman/config/readers/`
- **Tests:** `tests/test_config.py` (TO BE CREATED)
- **Feature Guide:** `.migration/FEATURE_GUIDE.md`
- **Rules:** `.cursor/rules/scriptman.mdc`
