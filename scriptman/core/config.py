import sys
from datetime import datetime
from pathlib import Path
from subprocess import run
from typing import Any, Callable, Optional

from dynaconf import Dynaconf
from loguru import logger
from tomlkit import dumps, parse

from scriptman.core.defaults import ConfigModel
from scriptman.core.version import Version


class Config:
    """
    📂 ConfigHandler Singleton Class

    Manages configuration, versioning, logging, and package management.
    """

    _instance: Optional["Config"] = None

    def __new__(cls):
        """
        🔒 Singleton implementation ensuring a single instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(
        self, config: ConfigModel = ConfigModel(), version: Version = Version()
    ) -> None:
        """
        📝 Initialize configurations and create necessary directories.

        Args:
            config (ConfigModel): Configuration settings.
            version (Version): Version information.
        """
        self.env = Dynaconf(
            root_path=config.cwd,
            settings_files=["scriptman.toml", ".secrets.toml"],
        )
        self._configs = config
        self._version = version
        self._initialize_defaults()
        self._initialize_logging()
        self._initialize_directories()
        self.callback_function: Optional[Callable[[Exception, dict], None]] = None

    def _initialize_defaults(self) -> None:
        """
        🔄 Initialize configuration defaults.

        Ensures all configuration parameters have default values.
        """
        for param, config in self._configs:
            if param not in self.env:
                self.env.set(param, config)

    def _initialize_logging(self, verbose: bool = False) -> None:
        """
        📝 Initialize logging for the CLI handler.
        """
        logger.remove()
        log_level = self.env.log_level
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
            Path(self.env.logs_dir) / f"{timestamp}.log",
            level=log_level,
            rotation="1 day",
            compression="zip",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        )

        logger.debug("✅ Logging initialized")

    def _initialize_directories(self) -> None:
        """
        📁 Initialize directories for the scriptman package.
        """
        Path(self.env.logs_dir).mkdir(parents=True, exist_ok=True)
        Path(self.env.scripts_dir).mkdir(parents=True, exist_ok=True)
        Path(self.env.downloads_dir).mkdir(parents=True, exist_ok=True)

    def reset_configuration(self, param: str) -> bool:
        """
        🔄 Resets a configuration parameter to its default value.

        Args:
            param (str): The parameter name to reset.
        """
        try:
            default = self._configs.model_fields[param].default
            self.env.set(param, default)
            logger.info(f"Config reset: {param} = {default}")
            return True
        except KeyError:
            logger.error(f"Invalid configuration parameter: {param}")
            return False

    def reset_all_configurations(self) -> None:
        """
        🔄 Resets the configuration to its default values and removes the
        `scriptman.toml` file.

        This method is useful for resetting the configuration to its original
        state, for example, after testing or when the user wants to start fresh.
        """
        self._initialize_defaults()
        (Path(self.env.cwd) / "scriptman.toml").unlink(missing_ok=True)
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
        self.env.set(param, validated_value)

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

            if value_type is bool and value in ["true", "false"]:
                value = value.lower() == "true"
            else:
                value = value_type(value)
            # TODO: Reformat pydantic validation errors
            current_settings.update({param: value})
            self._configs = self._configs.model_validate(current_settings)
            return True, None, value
        except Exception as e:
            return False, f"Invalid configuration value for {param}: {e}", None

    def _save_config_to_file(self, param: str, value: Any) -> None:
        """
        🔧 Save updated configuration to the TOML file.

        Args:
            param (str): The configuration parameter.
            value (Any): The value to save.
        """
        settings_file = Path(self.env.cwd) / "scriptman.toml"
        config_data = {}

        if settings_file.exists():
            with settings_file.open("r", encoding="utf-8") as f:
                config_data = parse(f.read())

        config_data[param] = value
        with settings_file.open("w", encoding="utf-8") as f:
            f.write(dumps(config_data))

    def add_callback_function(
        self, callback_function: Callable[[Exception, dict], None]
    ) -> bool:
        """
        🔄 Add a callback function to handle exceptions.

        Args:
            callback_function (Callable): A function to call on exceptions.

        Returns:
            bool: True if the function was added successfully, False otherwise.
        """
        if not callable(callback_function):
            logger.error("Invalid callback function provided.")
            return False

        self.callback_function = callback_function
        return True

    def update_package(self, version: str = "latest") -> None:
        """
        📦 Update the scriptman package from GitHub.

        Args:
            version (str, optional): The version to update to. Defaults to "latest".
        """

        try:
            if version == "latest":
                major, minor, commit = str(
                    self._version.read_version_from_pyproject()
                ).split(".")

                commit = self._version.get_commit_count()
            else:
                major, minor, commit = str(version).split(".")

            major, minor, commit = int(major), int(minor), int(commit)
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


# Singleton instance
config: Config = Config()
__all__ = ["config"]
