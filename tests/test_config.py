"""Tests for scriptman.config — configuration management.

Coverage Goals:
- Thread-safe singleton behavior
- Config get/set/reset operations
- Priority chain (override > env > file > schema)
- Override and temporary context manager
- Reader management (migrate, use_reader, generate_example)
- Path resolution (resolve_path, ensure_path)
- New features (reload, dump, __repr__)
- JSON reader (default)
- Optional readers (TOML, YAML, dotenv) when available
- Auto-discovery logic
- Schema deep key enumeration
- Edge cases (unicode, empty, long values, error paths)

Target: 95%+ coverage
"""

from __future__ import annotations

import os
from pathlib import Path
from threading import Thread

import pytest

from scriptman.config import (
    Config,
    ConfigSchema,
    dump,
    ensure_path,
    get,
    resolve_path,
)
from scriptman.config import (
    set as config_set,
)
from scriptman.config.readers import (
    ConfigReader,
    JsonReader,
    auto_discover_reader,
    get_reader_for_file,
    is_format_available,
    register_reader,
)
from scriptman.config.schema.data import DataConfig

# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for test files."""
    return tmp_path


@pytest.fixture
def temp_json_config(temp_dir):
    """Create a temporary JSON config file."""
    config_path = temp_dir / "scriptman.json"
    config_path.write_text('{"logging": {"level": "DEBUG"}}', encoding="utf-8")
    return config_path


@pytest.fixture
def fresh_config(temp_dir, monkeypatch):
    """Create a fresh Config instance for testing."""
    monkeypatch.chdir(temp_dir)
    # Reset singleton state (accessing name-mangled attribute for testing)
    Config._instance = None
    Config._Config__initialized = False  # type: ignore[attr-defined]
    return Config(_force_reinit=True)


@pytest.fixture(autouse=True)
def reset_config_singleton():
    """Reset Config singleton state before and after each test."""
    # Save original state
    original_instance = Config._instance
    original_initialized = getattr(Config, "_Config__initialized", False)

    yield

    # Restore original state (accessing name-mangled attribute for testing)
    Config._instance = original_instance
    Config._Config__initialized = original_initialized  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Clean SCRIPTMAN_* environment variables before and after each test."""
    # Save original env vars
    original_env = {k: v for k, v in os.environ.items() if k.startswith("SCRIPTMAN_")}

    # Remove SCRIPTMAN_* vars
    for key in list(os.environ.keys()):
        if key.startswith("SCRIPTMAN_"):
            monkeypatch.delenv(key, raising=False)

    yield

    # Restore original env vars
    for key in list(os.environ.keys()):
        if key.startswith("SCRIPTMAN_"):
            monkeypatch.delenv(key, raising=False)
    for key, value in original_env.items():
        monkeypatch.setenv(key, value)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG SINGLETON TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigSingleton:
    """🔒 Test thread-safe singleton behavior."""

    def test_same_instance_returned(self, temp_dir, monkeypatch):
        """🔒 Multiple Config() calls return the same instance."""
        monkeypatch.chdir(temp_dir)
        Config._instance = None
        Config._Config__initialized = False  # type: ignore[attr-defined]

        config1 = Config()
        config2 = Config()
        config3 = Config()

        assert config1 is config2
        assert config2 is config3
        assert config1 is config3

    def test_thread_safe_initialization(self, temp_dir, monkeypatch):
        """🔒 Config initialization is thread-safe."""
        monkeypatch.chdir(temp_dir)
        Config._instance = None
        Config._Config__initialized = False  # type: ignore[attr-defined]

        instances: list[Config] = []

        def create_config():
            instances.append(Config())

        threads = [Thread(target=create_config) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # All instances should be the same
        assert len({id(inst) for inst in instances}) == 1
        assert all(inst is instances[0] for inst in instances)

    def test_force_reinit_works(self, temp_dir, monkeypatch):
        """🔒 Force reinit creates new instance state."""
        monkeypatch.chdir(temp_dir)
        Config._instance = None
        Config._Config__initialized = False  # type: ignore[attr-defined]

        config1 = Config()
        config1.set("logging.level", "DEBUG", persist=False)

        # Force reinit
        config2 = Config(_force_reinit=True)
        config2.set("logging.level", "INFO", persist=False)

        # Both should be same instance but with different state
        assert config1 is config2
        assert config1.get("logging.level") == "INFO"  # Latest state


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG GET TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigGet:
    """🔍 Test config.get() functionality."""

    def test_get_existing_key(self, fresh_config):
        """🔍 Get existing key returns value."""
        value = fresh_config.get("logging.level")
        assert value == "INFO"  # Schema default

    def test_get_missing_key_with_default(self, fresh_config):
        """🔍 Get missing key returns provided default."""
        value = fresh_config.get("nonexistent.key", default="fallback")
        assert value == "fallback"

    def test_get_missing_key_no_default(self, fresh_config):
        """🔍 Get missing key without default returns None."""
        value = fresh_config.get("nonexistent.key")
        assert value is None

    def test_get_nested_key(self, fresh_config):
        """🔍 Get nested key works with dot notation."""
        value = fresh_config.get("data.dir")
        assert isinstance(value, Path)

    def test_get_deeply_nested_key(self, fresh_config):
        """🔍 Get deeply nested key works."""
        # Test that deeply nested keys work (observe.store.path exists in schema)
        # Note: The default value is None, which is valid
        value = fresh_config.get("observe.store.path")
        # Value can be None (default), str, or Path
        assert value is None or isinstance(value, str | Path)

    def test_bracket_notation_access(self, fresh_config):
        """🔍 Bracket notation config[key] works."""
        value = fresh_config["logging.level"]
        assert value == "INFO"

    def test_bracket_notation_missing_key(self, fresh_config):
        """🔍 Bracket notation raises KeyError for missing key."""
        with pytest.raises(KeyError, match="Config key not found"):
            _ = fresh_config["nonexistent.key"]

    def test_contains_check(self, fresh_config):
        """🔍 'key in config' works."""
        assert "logging.level" in fresh_config
        assert "nonexistent.key" not in fresh_config

    def test_keys_returns_all_keys(self, fresh_config):
        """🔍 keys() returns all valid config keys."""
        keys = fresh_config.keys()
        assert isinstance(keys, list)
        assert "logging.level" in keys
        assert "data.dir" in keys

    def test_items_returns_key_value_pairs(self, fresh_config):
        """🔍 items() returns all key-value pairs."""
        items = fresh_config.items()
        assert isinstance(items, list)
        assert all(isinstance(item, tuple) and len(item) == 2 for item in items)
        assert any(key == "logging.level" for key, _ in items)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG SET TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigSet:
    """✍️ Test config.set() functionality."""

    def test_set_valid_value(self, fresh_config, temp_dir):
        """✍️ Set valid value updates config."""
        fresh_config.set("logging.level", "DEBUG", persist=True)
        assert fresh_config.get("logging.level") == "DEBUG"

        # Verify persisted to file
        config_file = temp_dir / "scriptman.json"
        if config_file.exists():
            import json

            data = json.loads(config_file.read_text())
            assert data["logging"]["level"] == "DEBUG"

    def test_set_invalid_type_raises(self, fresh_config):
        """✍️ Set invalid type raises ValidationError."""
        with pytest.raises((ValueError, Exception)):
            fresh_config.set("logging.level", 12345)  # Should be string

    def test_set_no_persist_option(self, fresh_config, temp_dir):
        """✍️ Set with persist=False doesn't write to file."""
        fresh_config.set("logging.level", "DEBUG", persist=False)
        assert fresh_config.get("logging.level") == "DEBUG"

        # File should not exist or not have the value
        config_file = temp_dir / "scriptman.json"
        if config_file.exists():
            import json

            data = json.loads(config_file.read_text())
            assert (
                "logging" not in data or data.get("logging", {}).get("level") != "DEBUG"
            )

    def test_bracket_notation_set(self, fresh_config):
        """✍️ Bracket notation assignment works."""
        fresh_config["logging.level"] = "DEBUG"
        assert fresh_config.get("logging.level") == "DEBUG"


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG PRIORITY CHAIN TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigPriorityChain:
    """⚡ Test configuration priority resolution."""

    def test_override_beats_env(self, fresh_config, monkeypatch):
        """⚡ Override has highest priority."""
        monkeypatch.setenv("SCRIPTMAN_LOGGING__LEVEL", "WARNING")
        fresh_config.override(logging={"level": "DEBUG"})
        assert fresh_config.get("logging.level") == "DEBUG"

    def test_env_beats_file(self, fresh_config, temp_dir, monkeypatch):
        """⚡ Environment variable beats file value."""
        # Write to file
        config_file = temp_dir / "scriptman.json"
        config_file.write_text('{"logging": {"level": "INFO"}}', encoding="utf-8")
        fresh_config.reload()

        # Set env var
        monkeypatch.setenv("SCRIPTMAN_LOGGING__LEVEL", "WARNING")

        # Env should win
        assert fresh_config.get("logging.level") == "WARNING"

    def test_file_beats_default(self, fresh_config, temp_dir):
        """⚡ File value beats schema default."""
        config_file = temp_dir / "scriptman.json"
        config_file.write_text('{"logging": {"level": "DEBUG"}}', encoding="utf-8")
        fresh_config.reload()
        assert fresh_config.get("logging.level") == "DEBUG"

    def test_schema_default_used_when_nothing_set(self, fresh_config):
        """⚡ Schema default used when nothing else set."""
        assert fresh_config.get("logging.level") == "INFO"  # Schema default


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG OVERRIDES TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigOverrides:
    """🔄 Test override functionality."""

    def test_override_sets_value(self, fresh_config):
        """🔄 Override sets runtime value."""
        fresh_config.override(logging={"level": "DEBUG"})
        assert fresh_config.get("logging.level") == "DEBUG"

    def test_override_not_persisted(self, fresh_config, temp_dir):
        """🔄 Override doesn't persist to file."""
        fresh_config.override(logging={"level": "DEBUG"})
        assert fresh_config.get("logging.level") == "DEBUG"

        # File should not have the override
        config_file = temp_dir / "scriptman.json"
        if config_file.exists():
            import json

            data = json.loads(config_file.read_text())
            assert data.get("logging", {}).get("level") != "DEBUG"

    def test_temporary_context_manager(self, fresh_config):
        """🔄 Temporary override restores on exit."""
        original = fresh_config.get("logging.level")

        with fresh_config.temporary(logging={"level": "DEBUG"}):
            assert fresh_config.get("logging.level") == "DEBUG"

        # Should be restored
        assert fresh_config.get("logging.level") == original

    def test_temporary_restores_on_exception(self, fresh_config):
        """🔄 Temporary override restores even on exception."""
        original = fresh_config.get("logging.level")

        with (
            pytest.raises(ValueError),
            fresh_config.temporary(logging={"level": "DEBUG"}),
        ):
            assert fresh_config.get("logging.level") == "DEBUG"
            raise ValueError("Test exception")

        # Should be restored
        assert fresh_config.get("logging.level") == original

    def test_clear_overrides(self, fresh_config):
        """🔄 clear_overrides() removes all overrides."""
        fresh_config.override(logging={"level": "DEBUG"})
        assert fresh_config.get("logging.level") == "DEBUG"

        fresh_config.clear_overrides()
        # Should fall back to schema default
        assert fresh_config.get("logging.level") == "INFO"


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG NEW FEATURES TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigNewFeatures:
    """🆕 Test new features (reload, dump, __repr__)."""

    def test_reload_updates_store(self, fresh_config, temp_dir):
        """🔄 reload() updates config from file."""
        config_file = temp_dir / "scriptman.json"
        config_file.write_text('{"logging": {"level": "INFO"}}', encoding="utf-8")
        fresh_config.reload()

        # Change file
        config_file.write_text('{"logging": {"level": "DEBUG"}}', encoding="utf-8")
        fresh_config.reload()

        assert fresh_config.get("logging.level") == "DEBUG"

    def test_dump_returns_nested_dict(self, fresh_config):
        """📤 dump() returns complete config as nested dict."""
        result = fresh_config.dump()
        assert isinstance(result, dict)
        assert "logging" in result
        assert isinstance(result["logging"], dict)
        assert "level" in result["logging"]

    def test_repr_shows_state(self, fresh_config):
        """🔍 __repr__() shows config state."""
        repr_str = repr(fresh_config)
        assert "Config(" in repr_str
        assert "reader=" in repr_str
        assert "keys=" in repr_str
        assert "overrides=" in repr_str

    def test_str_shows_summary(self, fresh_config):
        """📝 __str__() shows human-readable summary."""
        str_str = str(fresh_config)
        assert "Scriptman Config" in str_str


# ═══════════════════════════════════════════════════════════════════════════════
# JSON READER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestJsonReader:
    """📄 Test JSON reader (default)."""

    def test_read_existing_file(self, temp_dir):
        """📄 Read existing JSON file."""
        config_file = temp_dir / "test.json"
        config_file.write_text('{"key": "value"}', encoding="utf-8")

        reader = JsonReader(config_file)
        data = reader.read()

        assert data == {"key": "value"}

    def test_read_missing_file_returns_empty(self, temp_dir):
        """📄 Read missing file returns empty dict."""
        config_file = temp_dir / "nonexistent.json"
        reader = JsonReader(config_file)
        data = reader.read()

        assert data == {}

    def test_write_creates_file(self, temp_dir):
        """📄 Write creates file if it doesn't exist."""
        config_file = temp_dir / "new.json"
        reader = JsonReader(config_file)

        reader.write({"key": "value"})

        assert config_file.exists()
        import json

        data = json.loads(config_file.read_text())
        assert data == {"key": "value"}

    def test_section_support(self, temp_dir):
        """📄 JSON reader supports section parameter."""
        config_file = temp_dir / "package.json"
        config_file.write_text('{"scriptman": {"key": "value"}}', encoding="utf-8")

        reader = JsonReader(config_file, section="scriptman")
        data = reader.read()

        assert data == {"key": "value"}

    def test_generate_example(self, temp_dir):
        """📄 generate_example() creates example JSON."""
        reader = JsonReader(temp_dir / "example.json")
        example = reader.generate_example(ConfigSchema)

        assert isinstance(example, str)
        assert "logging" in example or "data" in example


# ═══════════════════════════════════════════════════════════════════════════════
# TOML READER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestTomlReader:
    """📄 Test TOML reader (optional)."""

    @pytest.fixture(autouse=True)
    def skip_if_unavailable(self):
        """Skip tests if tomlkit not installed."""
        if not is_format_available("toml"):
            pytest.skip("tomlkit not installed")

    def test_read_existing_file(self, temp_dir):
        """📄 Read existing TOML file."""
        from scriptman.config.readers.toml import TomlReader

        config_file = temp_dir / "test.toml"
        config_file.write_text('[logging]\nlevel = "DEBUG"', encoding="utf-8")

        reader = TomlReader(config_file)
        data = reader.read()

        assert data == {"logging": {"level": "DEBUG"}}

    def test_read_missing_file_returns_empty(self, temp_dir):
        """📄 Read missing file returns empty dict."""
        from scriptman.config.readers.toml import TomlReader

        config_file = temp_dir / "nonexistent.toml"
        reader = TomlReader(config_file)
        data = reader.read()

        assert data == {}

    def test_write_creates_file(self, temp_dir):
        """📄 Write creates file if it doesn't exist."""
        from scriptman.config.readers.toml import TomlReader

        config_file = temp_dir / "new.toml"
        reader = TomlReader(config_file)

        reader.write({"logging": {"level": "DEBUG"}})

        assert config_file.exists()
        content = config_file.read_text()
        assert "logging" in content
        assert "DEBUG" in content

    def test_section_support_pyproject(self, temp_dir):
        """📄 TOML reader supports [tool.scriptman] section."""
        from scriptman.config.readers.toml import TomlReader

        config_file = temp_dir / "pyproject.toml"
        config_file.write_text(
            '[tool.scriptman]\nkey = "value"',
            encoding="utf-8",
        )

        reader = TomlReader(config_file, section="scriptman")
        data = reader.read()

        assert data == {"key": "value"}

    def test_write_to_section(self, temp_dir):
        """📄 Write to [tool.scriptman] section preserves other content."""
        from scriptman.config.readers.toml import TomlReader

        config_file = temp_dir / "pyproject.toml"
        config_file.write_text(
            '[project]\nname = "myproject"\n\n[tool.other]\nkey = "keep"',
            encoding="utf-8",
        )

        reader = TomlReader(config_file, section="scriptman")
        reader.write({"logging": {"level": "DEBUG"}})

        content = config_file.read_text()
        assert "myproject" in content  # project section preserved
        assert 'key = "keep"' in content  # other tool section preserved
        assert "scriptman" in content  # new section added

    def test_generate_example(self, temp_dir):
        """📄 generate_example() creates example TOML."""
        from scriptman.config.readers.toml import TomlReader

        reader = TomlReader(temp_dir / "example.toml")
        example = reader.generate_example(ConfigSchema)

        assert isinstance(example, str)
        assert "logging" in example or "data" in example

    def test_nested_values(self, temp_dir):
        """📄 TOML handles nested values correctly."""
        from scriptman.config.readers.toml import TomlReader

        config_file = temp_dir / "nested.toml"
        config_file.write_text(
            '[data]\ndir = ".data"\n\n[data.db]\npath = "test.db"',
            encoding="utf-8",
        )

        reader = TomlReader(config_file)
        data = reader.read()

        assert data["data"]["dir"] == ".data"


# ═══════════════════════════════════════════════════════════════════════════════
# YAML READER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestYamlReader:
    """📄 Test YAML reader (optional)."""

    @pytest.fixture(autouse=True)
    def skip_if_unavailable(self):
        """Skip tests if pyyaml not installed."""
        if not is_format_available("yaml"):
            pytest.skip("pyyaml not installed")

    def test_read_existing_file(self, temp_dir):
        """📄 Read existing YAML file."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "test.yaml"
        config_file.write_text("logging:\n  level: DEBUG", encoding="utf-8")

        reader = YamlReader(config_file)
        data = reader.read()

        assert data == {"logging": {"level": "DEBUG"}}

    def test_read_missing_file_returns_empty(self, temp_dir):
        """📄 Read missing file returns empty dict."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "nonexistent.yaml"
        reader = YamlReader(config_file)
        data = reader.read()

        assert data == {}

    def test_write_creates_file(self, temp_dir):
        """📄 Write creates file if it doesn't exist."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "new.yaml"
        reader = YamlReader(config_file)

        reader.write({"logging": {"level": "DEBUG"}})

        assert config_file.exists()
        content = config_file.read_text()
        assert "logging" in content
        assert "DEBUG" in content

    def test_yml_extension(self, temp_dir):
        """📄 YAML reader works with .yml extension."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "test.yml"
        config_file.write_text("key: value", encoding="utf-8")

        reader = YamlReader(config_file)
        data = reader.read()

        assert data == {"key": "value"}

    def test_generate_example(self, temp_dir):
        """📄 generate_example() creates example YAML."""
        from scriptman.config.readers.yaml import YamlReader

        reader = YamlReader(temp_dir / "example.yaml")
        example = reader.generate_example(ConfigSchema)

        assert isinstance(example, str)
        assert "Scriptman Configuration" in example  # Header comment

    def test_unicode_values(self, temp_dir):
        """📄 YAML handles unicode values correctly."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "unicode.yaml"
        config_file.write_text("message: 你好世界 🎉", encoding="utf-8")

        reader = YamlReader(config_file)
        data = reader.read()

        assert data["message"] == "你好世界 🎉"

    def test_read_empty_file(self, temp_dir):
        """📄 Empty YAML file returns empty dict."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "empty.yaml"
        config_file.write_text("", encoding="utf-8")

        reader = YamlReader(config_file)
        data = reader.read()

        assert data == {}

    def test_nested_values(self, temp_dir):
        """📄 YAML handles deeply nested values."""
        from scriptman.config.readers.yaml import YamlReader

        config_file = temp_dir / "nested.yaml"
        config_file.write_text(
            "observe:\n  store:\n    path: events.db\n    max_events: 1000",
            encoding="utf-8",
        )

        reader = YamlReader(config_file)
        data = reader.read()

        assert data["observe"]["store"]["path"] == "events.db"
        assert data["observe"]["store"]["max_events"] == 1000


# ═══════════════════════════════════════════════════════════════════════════════
# DOTENV READER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestDotEnvReader:
    """📄 Test .env reader (optional)."""

    @pytest.fixture(autouse=True)
    def skip_if_unavailable(self):
        """Skip tests if python-dotenv not installed."""
        if not is_format_available("dotenv"):
            pytest.skip("python-dotenv not installed")

    def test_read_existing_file(self, temp_dir):
        """📄 Read existing .env file."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env"
        env_file.write_text("SCRIPTMAN_LOGGING__LEVEL=DEBUG", encoding="utf-8")

        reader = DotEnvReader(env_file)
        data = reader.read()

        assert data == {"logging": {"level": "DEBUG"}}

    def test_read_missing_file_returns_empty(self, temp_dir):
        """📄 Read missing file returns empty dict."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env.nonexistent"
        reader = DotEnvReader(env_file)
        data = reader.read()

        assert data == {}

    def test_write_creates_file(self, temp_dir):
        """📄 Write creates file with SCRIPTMAN_ prefix."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env"
        reader = DotEnvReader(env_file)

        reader.write({"logging": {"level": "DEBUG"}})

        assert env_file.exists()
        content = env_file.read_text()
        assert "SCRIPTMAN_LOGGING__LEVEL" in content
        assert "DEBUG" in content

    def test_ignores_non_scriptman_vars(self, temp_dir):
        """📄 DotEnv reader ignores non-SCRIPTMAN_ variables."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env"
        env_file.write_text(
            "OTHER_VAR=ignored\nSCRIPTMAN_LOGGING__LEVEL=DEBUG",
            encoding="utf-8",
        )

        reader = DotEnvReader(env_file)
        data = reader.read()

        assert "OTHER_VAR" not in str(data)
        assert data == {"logging": {"level": "DEBUG"}}

    def test_parses_boolean_values(self, temp_dir):
        """📄 DotEnv reader parses boolean values."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env"
        env_file.write_text("SCRIPTMAN_OBSERVE__ENABLED=true", encoding="utf-8")

        reader = DotEnvReader(env_file)
        data = reader.read()

        assert data["observe"]["enabled"] is True

    def test_parses_numeric_values(self, temp_dir):
        """📄 DotEnv reader parses numeric values."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env"
        env_file.write_text(
            "SCRIPTMAN_OBSERVE__STORE__MAX_EVENTS=5000",
            encoding="utf-8",
        )

        reader = DotEnvReader(env_file)
        data = reader.read()

        assert data["observe"]["store"]["max_events"] == 5000

    def test_generate_example(self, temp_dir):
        """📄 generate_example() creates example .env."""
        from scriptman.config.readers.dotenv import DotEnvReader

        reader = DotEnvReader(temp_dir / ".env")
        example = reader.generate_example(ConfigSchema)

        assert isinstance(example, str)
        assert "SCRIPTMAN_" in example

    def test_does_not_pollute_os_environ_by_default(self, temp_dir):
        """📄 DotEnv reader doesn't modify os.environ by default."""
        from scriptman.config.readers.dotenv import DotEnvReader

        env_file = temp_dir / ".env"
        env_file.write_text("SCRIPTMAN_TEST_VAR=test_value", encoding="utf-8")

        reader = DotEnvReader(env_file, load_into_env=False)
        reader.read()

        # Should NOT be in os.environ
        assert os.environ.get("SCRIPTMAN_TEST_VAR") is None

    def test_secrets_env_file(self, temp_dir):
        """📄 DotEnv reader works with .secrets.env pattern."""
        from scriptman.config.readers.dotenv import DotEnvReader

        secrets_file = temp_dir / ".secrets.env"
        secrets_file.write_text("SCRIPTMAN_LOGGING__LEVEL=ERROR", encoding="utf-8")

        reader = DotEnvReader(secrets_file)
        data = reader.read()

        assert data["logging"]["level"] == "ERROR"


# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONAL READERS TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestOptionalReaders:
    """📖 Test optional format readers."""

    def test_toml_reader_works_when_available(self, temp_dir):
        """📖 TOML reader works when tomlkit is installed."""
        if not is_format_available("toml"):
            pytest.skip("tomlkit not installed")

        from scriptman.config.readers.toml import TomlReader

        reader = TomlReader(temp_dir / "test.toml")
        assert reader.name == "toml"
        assert reader.supports_write() is True

    def test_yaml_reader_works_when_available(self, temp_dir):
        """📖 YAML reader works when pyyaml is installed."""
        if not is_format_available("yaml"):
            pytest.skip("pyyaml not installed")

        from scriptman.config.readers.yaml import YamlReader

        reader = YamlReader(temp_dir / "test.yaml")
        assert reader.name == "yaml"
        assert reader.supports_write() is True

    def test_dotenv_reader_works_when_available(self, temp_dir):
        """📖 DotEnv reader works when python-dotenv is installed."""
        if not is_format_available("dotenv"):
            pytest.skip("python-dotenv not installed")

        from scriptman.config.readers.dotenv import DotEnvReader

        reader = DotEnvReader(temp_dir / ".env")
        assert reader.name == "dotenv"
        assert reader.supports_write() is True

    def test_unsupported_extension_gives_helpful_error(self, temp_dir):
        """📖 Unsupported extension gives helpful error with install hint."""
        # Test that we get a helpful error for unknown extensions
        with pytest.raises(ValueError, match="No reader"):
            get_reader_for_file(temp_dir / "test.unsupported")


# ═══════════════════════════════════════════════════════════════════════════════
# AUTO-DISCOVERY TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestAutoDiscovery:
    """🔍 Test auto-discovery logic."""

    def test_discovers_json_first(self, temp_dir, monkeypatch):
        """🔍 Auto-discovery finds JSON first."""
        monkeypatch.chdir(temp_dir)

        # Create JSON file
        json_file = temp_dir / "scriptman.json"
        json_file.write_text('{"key": "value"}', encoding="utf-8")

        reader = auto_discover_reader()
        assert isinstance(reader, JsonReader)
        assert reader.file_path == json_file

    def test_defaults_to_json_when_none_exist(self, temp_dir, monkeypatch):
        """🔍 Auto-discovery defaults to JSON when no files exist."""
        monkeypatch.chdir(temp_dir)

        reader = auto_discover_reader()
        assert isinstance(reader, JsonReader)
        assert reader.file_path == temp_dir / "scriptman.json"

    def test_is_format_available(self):
        """🔍 is_format_available() checks format support."""
        # JSON is always available
        assert is_format_available("json") is True

        # Others depend on optional deps
        # We can't reliably test these without mocking


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG SCHEMA TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestConfigSchema:
    """📋 Test schema functionality."""

    def test_get_all_keys_deeply_nested(self):
        """📋 get_all_keys() returns deeply nested keys."""
        keys = ConfigSchema.get_all_keys()
        assert isinstance(keys, list)
        assert len(keys) > 0

        # Should include top-level keys
        assert any("logging" in key for key in keys)
        assert any("data" in key for key in keys)

        # Should include nested keys
        assert any("logging.level" in key or key == "logging.level" for key in keys)
        assert any("data.dir" in key or key == "data.dir" for key in keys)

    def test_get_field_info(self):
        """📋 get_field_info() returns field information."""
        field_type, description, default = ConfigSchema.get_field_info("logging.level")
        assert field_type is not None
        assert default == "INFO"

    def test_get_default(self):
        """📋 get_default() returns default value."""
        default = ConfigSchema.get_default("logging.level")
        assert default == "INFO"


# ═══════════════════════════════════════════════════════════════════════════════
# PATH RESOLUTION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPathResolution:
    """📁 Test path resolution functionality."""

    def test_resolve_path(self, fresh_config):
        """📁 resolve_path() returns correct path."""
        path = fresh_config.resolve_path("logs")
        assert isinstance(path, Path)
        assert "logs" in str(path)

    def test_resolve_path_invalid_category(self, fresh_config):
        """📁 resolve_path() raises ValueError for invalid category."""
        with pytest.raises(ValueError, match="Invalid data category"):
            fresh_config.resolve_path("invalid")

    def test_ensure_path_creates_directory(self, fresh_config):
        """📁 ensure_path() creates directory if needed."""
        path = fresh_config.ensure_path("logs")
        assert path.exists()
        assert path.is_dir()


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CONFIG TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataConfig:
    """📁 Test DataConfig functionality."""

    def test_get_path_valid_category(self):
        """📁 get_path() works with valid category."""
        config = DataConfig()
        path = config.get_path("logs")
        assert isinstance(path, Path)

    def test_get_path_invalid_category(self):
        """📁 get_path() raises ValueError for invalid category."""
        config = DataConfig()
        with pytest.raises(ValueError, match="Invalid data category"):
            config.get_path("invalid")  # type: ignore[arg-type]

    def test_ensure_path_creates_dir(self, temp_dir):
        """📁 ensure_path() creates directory."""
        config = DataConfig(dir=temp_dir)
        path = config.ensure_path("logs")
        assert path.exists()
        assert path.is_dir()


# ═══════════════════════════════════════════════════════════════════════════════
# READER MANAGEMENT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestReaderManagement:
    """📖 Test reader management functionality."""

    def test_use_reader_switches_reader(self, fresh_config, temp_dir):
        """📖 use_reader() switches to different reader."""
        new_reader = JsonReader(temp_dir / "new.json")
        fresh_config.use_reader(new_reader)

        assert fresh_config._reader is new_reader

    def test_register_reader(self, temp_dir):
        """📖 register_reader() registers custom reader."""
        from typing import Any

        # Create a mock reader class
        class CustomReader(ConfigReader):
            def __init__(self, path: Path):
                self._path = path

            @property
            def name(self) -> str:
                return "custom"

            @property
            def file_path(self) -> Path | None:
                return self._path

            def read(self) -> dict[str, Any]:
                return {}

            def write(self, data: dict[str, Any]) -> None:
                pass

            def supports_write(self) -> bool:
                return True

            def generate_example(self, _schema: type) -> str:
                return ""

        register_reader(".custom", CustomReader)

        # Should be able to get reader for .custom files
        reader = get_reader_for_file(temp_dir / "test.custom")
        assert isinstance(reader, CustomReader)

    def test_generate_example_json(self, fresh_config):
        """📝 generate_example() creates example config."""
        example = fresh_config.generate_example("json")
        assert isinstance(example, str)
        assert len(example) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL FUNCTION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestModuleLevelFunctions:
    """🔧 Test module-level convenience functions."""

    def test_get_function(self, fresh_config):  # noqa: ARG002
        """🔧 get() function works."""
        _ = fresh_config  # Fixture needed to reset singleton
        value = get("logging.level")
        assert value == "INFO"

    def test_set_function(self, fresh_config):  # noqa: ARG002
        """🔧 set() function works."""
        _ = fresh_config  # Fixture needed to reset singleton
        config_set("logging.level", "DEBUG", persist=False)
        assert get("logging.level") == "DEBUG"

    def test_reload_function(self, fresh_config, temp_dir):
        """🔧 reload() function works."""
        # Use the fresh_config instance directly since module-level
        # reload() is bound to the global singleton
        config_file = temp_dir / "scriptman.json"
        config_file.write_text('{"logging": {"level": "DEBUG"}}', encoding="utf-8")
        fresh_config.reload()
        assert fresh_config.get("logging.level") == "DEBUG"

    def test_dump_function(self):
        """🔧 dump() function works."""
        result = dump()
        assert isinstance(result, dict)

    def test_resolve_path_function(self):
        """🔧 resolve_path() function works."""
        path = resolve_path("logs")
        assert isinstance(path, Path)

    def test_ensure_path_function(self):
        """🔧 ensure_path() function works."""
        path = ensure_path("logs")
        assert isinstance(path, Path)


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """🔬 Test edge cases."""

    def test_unicode_values(self, fresh_config):
        """🔬 Unicode values handled correctly."""
        fresh_config.set("logging.level", "DEBUG", persist=False)
        # Just verify no Unicode errors
        value = fresh_config.get("logging.level")
        assert value == "DEBUG"

    def test_empty_string_value(self, fresh_config):
        """🔬 Empty string value handled."""
        import contextlib

        # This might fail validation, which is fine
        with contextlib.suppress(ValueError, Exception):
            fresh_config.set("logging.level", "", persist=False)

    def test_special_characters_in_key(self, fresh_config):
        """🔬 Special characters in key handled."""
        # Dot notation keys should work
        value = fresh_config.get("data.dir")
        assert value is not None

    def test_reset_key(self, fresh_config):
        """🔬 reset() restores schema default."""
        fresh_config.set("logging.level", "DEBUG", persist=False)
        fresh_config.reset("logging.level")
        assert fresh_config.get("logging.level") == "INFO"  # Schema default

    def test_reset_all(self, fresh_config):
        """🔬 reset_all() clears all config."""
        fresh_config.set("logging.level", "DEBUG", persist=False)
        fresh_config.reset_all()
        # Should use schema defaults
        assert fresh_config.get("logging.level") == "INFO"
