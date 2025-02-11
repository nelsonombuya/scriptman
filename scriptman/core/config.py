import sys
from datetime import datetime
from pathlib import Path
from subprocess import run
from typing import Any, Callable, Optional

from dynaconf import Dynaconf
from loguru import logger
from pydantic import ValidationError
from tomlkit import dumps, parse

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
            settings_files=["scriptman.toml", ".secrets.toml"],
        )
        self._configs = config
        self._version = version
        self.__initialize_defaults()
        self.__initialize_directories()
        self.__initialize_scriptman_logging()
        self.__initialized = True

    @property
    def callback_function(self) -> Callable[[Exception], None] | None:
        """
        📞 Retrieve the callback function to handle exceptions.

        Returns:
            Callable[[Exception], None] | None: The callback function to handle exceptions
                or None if no function has been set.
        """
        return self.__callback_function

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
            # logger.debug(f"Retrieving config: {key}") # TODO: Verbose logging
            return self.env[key]
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
        self.env[key] = value
        logger.debug(f"Config updated: {key} = {value}")

    def __initialize_defaults(self) -> None:
        """
        🔄 Initialize configuration defaults.

        Ensures all configuration parameters have default values.
        """
        for field_name, field in self._configs.model_fields.items():
            if field_name not in self.env:
                self.env[field_name] = field.default

    def __initialize_directories(self) -> None:
        """
        📁 Initialize directories for the scriptman package.
        """
        for _, field in self._configs.model_fields.items():
            if field.annotation is Path:
                Path(field.default).mkdir(parents=True, exist_ok=True)

    def __initialize_scriptman_logging(self) -> None:
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
            default = self._configs.model_fields[param].default
            self.set(param, default)
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
        self.__initialize_defaults()
        Path(str(self.get("cwd", "."))).joinpath("scriptman.toml").unlink(missing_ok=True)
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

        param = param.upper()
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
        if param not in self._configs.model_fields.keys():
            return False, f"Invalid configuration parameter: {param}", None

        try:
            current_settings = self._configs.model_dump()
            value_type = type(current_settings[param])

            if value_type is bool and value.lower() in ["true", "false"]:
                value = value.lower() == "true"
            else:
                value = value_type(value)

            current_settings.update({param: value})
            self._configs = self._configs.model_validate(current_settings)
            return True, None, value
        except ValidationError as e:
            # TODO: Reformat pydantic validation errors
            return False, f"Invalid configuration value for {param}: {e}", None
        except Exception as e:
            return False, f"Invalid configuration value for {param}: {e}", None

    def _save_config_to_file(self, param: str, value: Any) -> None:
        """
        🔧 Save updated configuration to the TOML file.

        Args:
            param (str): The configuration parameter.
            value (Any): The value to save.
        """
        settings_file = Path(str(self.get("cwd", "."))).joinpath("scriptman.toml")
        config_data: dict[str, Any] = {}

        if settings_file.exists():
            with settings_file.open("r", encoding="utf-8") as f:
                config_data = parse(f.read())

        config_data[param] = value
        with settings_file.open("w", encoding="utf-8") as f:
            f.write(dumps(config_data))

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
                    for v in str(self._version.read_version_from_pyproject()).split(".")
                ]

                commit = self._version.get_commit_count()
                commit += 1 if version == "next" else 0
            else:
                major, minor, commit = [int(v) for v in str(version).split(".")]
        except ValueError:
            raise ValueError(
                f'Invalid version format: "{version}" '
                "Please provide a valid major.minor.commit format with all integers."
            )

        self._version.major, self._version.minor, self._version.commit = (
            major,
            minor,
            commit,
        )

        run(["poetry", "update"], check=True)  # Update Dependencies
        self._version.update_version_in_file("major", self._version.major)
        self._version.update_version_in_file("minor", self._version.minor)
        self._version.update_version_in_file("commit", self._version.commit)
        run(["poetry", "version", str(self._version)])  # Update Package Version
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
