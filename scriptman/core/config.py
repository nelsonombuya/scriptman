import sys
from datetime import datetime
from pathlib import Path
from re import sub
from subprocess import run
from typing import Any, Callable, Optional, cast

from dynaconf import Dynaconf
from loguru import logger
from pydantic import ValidationError
from tomlkit import dumps, parse, table
from tomlkit.items import Table

from scriptman.core.defaults import ConfigModel
from scriptman.core.version import Version


class Config:
    """
    📂 ConfigHandler Singleton Class

    Manages configuration, versioning, logging, and package management.
    """

    __initialized: bool = False
    __instance: Optional["Config"] = None
    __callback_function: Optional[Callable[[Exception], None]] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "Config":
        """
        🔒 Singleton implementation ensuring a single instance.
        """
        if cls.__instance is None:
            cls.__instance = super(Config, cls).__new__(cls, *args, **kwargs)
            cls.__instance.__initialized = False
        return cls.__instance

    def __init__(
        self, config: ConfigModel = ConfigModel(), version: Version = Version()
    ) -> None:
        """
        📝 Initialize configurations and create necessary directories.

        Args:
            config (ConfigModel): Configuration settings.
            version (Version): Version information.
        """
        if self.__initialized:
            return

        self.env = Dynaconf(
            root_path=str(config.cwd),
            settings_files=["pyproject.toml", "scriptman.toml", ".secrets.toml"],
        )
        self.__version = version
        self.__prefix: str = "tool.scriptman." if config.use_pyproject else ""
        self.__initialize_defaults(config)
        self.__initialize_logging()
        self.__initialized = True

    @property
    def version(self) -> str:
        """
        🎯 Retrieve the version information.

        Returns:
            str: The version information.
        """
        return str(self.__version)

    @property
    def callback_function(self) -> Callable[[Exception], None] | None:
        """
        📞 Retrieve the callback function to handle exceptions.

        Returns:
            Callable[[Exception], None] | None: The callback function to handle exceptions
                or None if no function has been set.
        """
        return self.__callback_function

    @property
    def current_settings(self) -> dict[str, Any]:
        """
        🔍 Retrieve the current configuration settings.

        Returns:
            dict[str, Any]: The current configuration settings.
        """
        return self.env[self.__prefix.rstrip(".")] if self.__prefix else dict(self.env)

    def __getitem__(self, key: str) -> Any:
        """
        🔍 Retrieve a configuration value from the underlying Dynaconf
        settings.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            Any: The value associated with the given key.

        Raises:
            KeyError: If the key is not present in the configuration.
        """
        if value := self.get(key):
            return value
        raise KeyError(f"Config not found: {key}")

    def __setitem__(self, key: str, value: Any) -> None:
        """
        💻 Set a configuration value in the underlying Dynaconf settings.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to set for the given key.
        """
        return self.set(key, value)

    def get(self, key: str, default: Any = None) -> Any:
        """
        🔍 Retrieve a configuration value from the underlying Dynaconf
        settings.

        Args:
            key (str): The key to retrieve the value for.
            default (Any): The default value to return if the key is not
                present in the configuration.

        Returns:
            Any: The configuration value for the given key or the default
                value if the key is not present.
        """
        try:
            return self.env[self.__prefix + key if self.__prefix else key]
        except KeyError:
            logger.debug(f"Config not found: {key}")
            return default

    def set(self, key: str, value: Any) -> None:
        """
        💻 Set a configuration value in the underlying Dynaconf settings.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to set for the given key.
        """
        self.env[self.__prefix + key if self.__prefix else key] = value
        logger.debug(f"Config updated: {key} = {value}")

    def __initialize_defaults(self, config: ConfigModel = ConfigModel()) -> None:
        """
        🚀 Initialize configuration defaults only for missing values.
        """
        for field_name, field in config.model_fields.items():
            try:
                self.env[self.__prefix + field_name]  # Check if field exists
            except KeyError:
                self.env[self.__prefix + field_name] = field.default  # Set to default

    def __initialize_logging(self) -> None:
        """
        📝 Initialize logging for the CLI handler.
        """
        logger.remove()
        log_level = str(self.get("log_level", "INFO"))
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Console handler
        logger.add(
            sys.stdout,
            colorize=True,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level:<8}</level> | "
            "<level>{message}</level>",
        )

        # File handler
        logger.add(
            Path(str(self.get("logs_dir", "logs"))) / f"{timestamp}.log",
            level=log_level,
            rotation="1 day",
            compression="zip",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        )

        logger.debug("✅ Logging initialized")

    def reset_configuration(self, param: str) -> bool:
        """
        🔄 Resets a configuration parameter to its default value.

        Args:
            param (str): The parameter name to reset.
        """
        try:
            default = ConfigModel.model_fields[param].default
            self.set(param, default)
            self._remove_config_from_file(param)
            logger.info(f"Config reset: {param} = {default}")
            return True
        except KeyError:
            logger.error(f"Invalid configuration parameter: {param}")
            return False

    def reset_all_configurations(self) -> None:
        """
        🔄 Resets the configuration to its default values and removes the
        `scriptman.toml` file.
        """
        Path(str(self.get("cwd", "."))).joinpath("scriptman.toml").unlink(missing_ok=True)
        self._remove_config_from_file("tool.scriptman")
        self.__initialize_defaults(ConfigModel())
        logger.info("🔄 Configuration reset and deleted scriptman.toml file")

    def validate_and_update_configuration(self, param: str, value: Any) -> bool:
        """
        📝 Validates and updates a configuration parameter.

        Args:
            param (str): The parameter name to update.
            value (Any): The new value to set.

        Returns:
            bool: True if the configuration was updated successfully, False otherwise.
        """
        logger.debug(f"Updating configuration: {param} = {value}")

        (
            is_valid,
            error_message,
            validated_value,
        ) = self._validate_config_value(param, value)

        if not is_valid:
            logger.error(error_message)
            return False

        param = param.lower()
        self.set(param, validated_value)

        try:
            self._save_config_to_file(param, validated_value)
            logger.info(f"Config updated successfully: {param} = {validated_value}")
            return True
        except Exception as e:
            logger.error(f"Failed to update configuration file: {e}")
            return False

    def _validate_config_value(
        self, param: str, value: Any
    ) -> tuple[bool, Optional[str], Any]:
        """
        🔍 Validates a configuration value based on the schema.

        Args:
            param (str): The parameter name.
            value (Any): The value to validate.

        Returns:
            tuple: A tuple containing:
                validation status,
                error message,
                and validated value.
        """
        if param not in ConfigModel.model_fields:
            return False, f"Invalid configuration parameter: {param}", None

        try:
            field = ConfigModel.model_fields[param]

            # Convert string "true"/"false" to bool if the field is boolean
            if field.annotation is bool and isinstance(value, str):
                value = value.lower() == "true"

            # Convert string paths to Path objects if needed
            elif field.annotation is Path and isinstance(value, str):
                value = Path(value)

            # Handle other type conversions
            else:
                try:
                    assert field.annotation is not None
                    value = field.annotation(value)
                except (ValueError, TypeError, AssertionError):
                    return (
                        False,
                        f"Invalid type for {param}: expected {field.annotation}",
                        None,
                    )
            return True, None, value
        except ValidationError as e:
            return False, f"Validation error for {param}: {e}", None
        except Exception as e:
            return False, f"Error validating {param}: {e}", None

    def _save_config_to_file(self, param: str, value: Any) -> None:
        """
        🔧 Save updated configuration to the TOML file.

        Args:
            param (str): The configuration parameter.
            value (Any): The value to save.
        """
        settings_file = Path(self.get("settings_file", "scriptman.toml"))
        config_data: dict[str, Any] = {}

        if settings_file.exists():
            with settings_file.open("r", encoding="utf-8") as f:
                config_data = parse(f.read())

        if isinstance(value, Path):  # Serializing Paths Correctly
            value = str(value)

        if self.get("use_pyproject", False):
            # Update the settings on pyproject.toml under tool.scriptman
            tool = cast(Table, config_data["tool"]) if "tool" in config_data else table()
            scriptman_section = tool.get("scriptman", table())
            scriptman_section[param] = value
            tool["scriptman"] = scriptman_section
            config_data["tool"] = tool
        else:
            # Write settings to scriptman.toml
            config_data[param] = value

        with settings_file.open("w", encoding="utf-8") as f:
            f.write(sub(r"\n{3,}", "\n\n", dumps(config_data)))

    def _remove_config_from_file(self, param: str) -> None:
        """
        🧹 Remove a configuration parameter from the TOML file.

        Args:
            param (str): The parameter name to remove.
        """
        settings_file = Path(self.get("settings_file", "scriptman.toml"))
        config_data: dict[str, Any] = {}

        if settings_file.exists():
            with settings_file.open("r", encoding="utf-8") as f:
                config_data = parse(f.read())

            del config_data[param]

            with settings_file.open("w", encoding="utf-8") as f:
                f.write(sub(r"\n{3,}", "\n\n", dumps(config_data)))

    def add_callback_function(
        self, callback_function: Callable[[Exception], None]
    ) -> bool:
        """
        🔄 Add a callback function to handle exceptions.

        Args:
            callback_function (Callable): A function to call on exceptions.

        Returns:
            bool: True if the function was added successfully, False otherwise.
        """
        if not callable(callback_function):
            raise ValueError("Callback function must be callable")

        self.__callback_function = callback_function
        return True

    def update_package(self, version: str = "latest") -> None:
        """
        📦 Update the scriptman package from GitHub.
        """
        try:
            if version == "latest" or version == "next":
                major, minor, commit = [
                    int(v)
                    for v in str(self.__version.read_version_from_pyproject()).split(".")
                ]

                commit = self.__version.get_commit_count()
                commit += 1 if version == "next" else 0
            else:
                major, minor, commit = [int(v) for v in str(version).split(".")]
        except ValueError:
            raise ValueError(
                f'Invalid version format: "{version}" '
                "Please provide a valid major.minor.commit format with all integers."
            )

        self.__version.major, self.__version.minor, self.__version.commit = (
            major,
            minor,
            commit,
        )

        run(["poetry", "update"], check=True)  # Update Dependencies
        self.__version.update_version_in_file("major", self.__version.major)
        self.__version.update_version_in_file("minor", self.__version.minor)
        self.__version.update_version_in_file("commit", self.__version.commit)
        run(["poetry", "version", str(self.__version)])  # Update Package Version
        logger.info("📦 Package updated successfully")

    def publish_package(self) -> None:
        """
        📦 Publish the scriptman package to PyPI.
        """
        run(["poetry", "publish", "--build"], check=True)

    def lint(self) -> None:
        """
        ⚡ Lint and typecheck the project files.
        """
        run(["isort", "."], check=True)  # Sort imports
        run(["black", "."], check=True)  # Format
        run(["mypy", "."], check=True)  # Typecheck


# Singleton instance
config: Config = Config()
__all__ = ["config"]
