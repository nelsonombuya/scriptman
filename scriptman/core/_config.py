from dataclasses import dataclass
from os import getcwd
from pathlib import Path
from subprocess import PIPE, run
from typing import Any, Callable, Literal, Optional, get_args, get_origin

from dynaconf import Dynaconf
from loguru import logger
from tomlkit import dumps, parse


@dataclass
class Version:
    """
    🌐 Version Class

    Represents the version of the application as major, minor, and commit.
    """

    major: int = 2
    minor: int = 0
    commit: int = 0

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.commit}"


class ConfigHandler:
    """
    📂 ConfigHandler Singleton Class

    Manages configuration, versioning, logging, and package management.
    """

    _instance: Optional["ConfigHandler"] = None
    _version_file: Path = Path(__file__).parent.parent / "version.toml"
    _version: Version = Version()

    def __new__(cls):
        """
        🔒 Singleton implementation ensuring a single instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """
        📝 Initialize configurations and create necessary directories.
        """
        self.config = Dynaconf(
            root_path=self.cwd,
            settings_files=["scriptman.toml", ".secrets.toml"],
        )
        self._initialize_defaults()
        self.callback_function: Optional[Callable[[Exception, dict], None]] = None
        Path(self.config.logs_dir).mkdir(parents=True, exist_ok=True)  # Create logs dir

    @property
    def cwd(self) -> Path:
        """
        🏡 Returns the current working directory as a Path object.
        """
        return Path(getcwd())

    @property
    def configs(self) -> dict[str, dict]:
        """
        🔧 Configuration schema defining all configurable parameters.
        """
        return {
            "logs_dir": {
                "type": str,
                "description": "Path to the logs directory",
                "default": str(Path(__file__).parent.parent / "logs"),
            },
            "concurrent": {
                "type": bool,
                "description": "Enable concurrent script execution",
                "default": True,
            },
            "log_level": {
                "type": Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "OFF"],
                "description": "Set the logging level",
                "default": "INFO",
            },
            "retries": {
                "type": int,
                "description": "Number of retries for failed scripts",
                "default": 0,
            },
            "log_for_each_script": {
                "type": bool,
                "description": "Enable logging for each script instance",
                "default": False,
            },
        }

    def _initialize_defaults(self) -> None:
        """
        🔄 Initialize configuration defaults.

        Ensures all configuration parameters have default values.
        """
        for param, config in self.configs.items():
            if param not in self.config:
                self.config.set(param, config["default"])

    def _load_version_from_file(self) -> Optional[str]:
        """
        🌐 Load the version from the version file.

        Returns:
            Optional[str]: The version string if found, otherwise None.
        """
        if self._version_file.exists():
            with self._version_file.open("r", encoding="utf-8") as f:
                version_data = parse(f.read())
                return version_data.get("version")
        return None

    def _save_version_to_file(self, version: str) -> None:
        """
        📂 Save the current version to the version file.

        Args:
            version (str): The version string to save.
        """
        with self._version_file.open("w", encoding="utf-8") as f:
            f.write(dumps({"version": version}))
        logger.debug(f"Updated Scriptman version to {version}")

    def _get_commit_count(self) -> int:
        """
        🎮 Retrieve the commit count from Git.

        Returns:
            int: The number of commits in the repository (default 0 on failure).
        """
        try:
            result = run(
                ["git", "rev-list", "--count", "HEAD"],
                stdout=PIPE,
                stderr=PIPE,
                check=True,
                text=True,
            )
            return int(result.stdout.strip())
        except Exception as e:
            logger.error(f"Failed to get commit count: {e}")
            return 0

    @property
    def version(self) -> str:
        """
        🌐 Returns the current application version.

        Determines the version from the file or generates it dynamically.
        """
        try:
            if version := self._load_version_from_file():
                major, minor, commit = map(int, version.split("."))
                self._version = Version(major, minor, commit)
                return str(self._version)

            self._version.commit = self._get_commit_count()
            version_str = str(self._version)
            self._save_version_to_file(version_str)
            return version_str

        except Exception as e:
            logger.error(f"Failed to determine or update version: {e}")
            return str(self._version)

    def validate_and_update_config(self, param: str, value: Any) -> bool:
        """
        📝 Validates and updates a configuration parameter.

        Args:
            param (str): The parameter name to update.
            value (Any): The new value to set.

        Returns:
            bool: True if the configuration was updated successfully, False otherwise.
        """
        logger.debug(f"Updating configuration: {param} = {value}")

        is_valid, error_message, validated_value = self._validate_config_value(
            param, value
        )

        if not is_valid:
            logger.error(error_message)
            return False

        param = param.upper()
        self.config.set(param, validated_value)

        try:
            self._save_config_to_file(param, validated_value)
            logger.info(
                f"Configuration updated successfully: {param} = {validated_value}"
            )
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
            tuple: A tuple containing validation status, error message, and validated
                value.
        """
        if param not in self.configs:
            allowed_params = ", ".join(self.configs.keys())
            return (
                False,
                f"Invalid parameter: {param}. Allowed parameters: {allowed_params}",
                None,
            )

        expected_type = self.configs[param]["type"]

        try:
            if expected_type == bool:
                if str(value).lower() not in ["true", "false"]:
                    return (
                        False,
                        f"Invalid value for {param}. Expected: True or False",
                        None,
                    )
                validated_value = str(value).lower() == "true"

            elif get_origin(expected_type) == Literal:
                if value not in get_args(expected_type):
                    return (
                        False,
                        f"Invalid value for {param}. "
                        f"Allowed values: {', '.join(map(str, get_args(expected_type)))}",
                        None,
                    )
                validated_value = value

            else:
                validated_value = expected_type(value)

            return True, None, validated_value
        except (ValueError, TypeError):
            return (
                False,
                f"Invalid type for {param}. Expected: {expected_type.__name__}",
                None,
            )

    def _save_config_to_file(self, param: str, value: Any) -> None:
        """
        🔧 Save updated configuration to the TOML file.

        Args:
            param (str): The configuration parameter.
            value (Any): The value to save.
        """
        settings_file = self.cwd / "scriptman.toml"
        config_data = {}

        if settings_file.exists():
            with settings_file.open("r", encoding="utf-8") as f:
                config_data = parse(f.read())

        config_data[param] = value
        with settings_file.open("w", encoding="utf-8") as f:
            f.write(dumps(config_data))

    def reset_scriptman_settings(self) -> None:
        self._initialize_defaults()
        Path(self.cwd / "scriptman.toml").unlink(missing_ok=True)
        logger.info("🔄 Configuration reset and deleted scriptman.toml file")

    def _update_version_on_pyproject(self):
        """
        Updates the version of the 'scriptman' project in the pyproject.toml file.
        This method checks if the pyproject.toml file exists in the current working
        directory.

        If the file exists, it reads the file and parses its content. If the project name
        in the pyproject.toml file is 'scriptman', it updates the version to the value of
        `self.version`.

        The updated content is then written back to the pyproject.toml file.

        Raises:
            FileNotFoundError: If the pyproject.toml file is not found in the current
                working directory.
            RuntimeError: If the project name in the pyproject.toml file is not scriptman.
        """
        pyproject_file = self.cwd / "pyproject.toml"

        if not pyproject_file.exists():
            raise FileNotFoundError(f"The pyproject.toml not found at {pyproject_file}")

        with pyproject_file.open("r", encoding="utf-8") as f:
            pyproject_data = parse(f.read())

        if pyproject_data.get("tool", {}).get("poetry", {}).get("name") == "scriptman":
            pyproject_data["tool"]["poetry"]["version"] = self.version  # type:ignore

            with pyproject_file.open("w", encoding="utf-8") as f:
                f.write(dumps(pyproject_data))
        else:
            raise RuntimeError("The current project is not scriptman!")

    def _update_scriptman_dependencies(self) -> bool:
        """
        ⚙ Updates the dependencies of the 'scriptman' project in the pyproject.toml file.
        """
        pyproject_file = self.cwd / "pyproject.toml"

        if not pyproject_file.exists():
            logger.error(
                f"The pyproject.toml not found at {pyproject_file}. "
                "Are you using poetry?"
            )
            raise FileNotFoundError(f"The pyproject.toml not found at {pyproject_file}")

        with pyproject_file.open("r", encoding="utf-8") as f:
            pyproject_data = parse(f.read())

        if pyproject_data.get("tool", {}).get("poetry", {}).get("name") == "scriptman":
            logger.info("🔄 Updating ScriptMan dependencies...")
            result = run(["poetry", "update"], check=False)
            if result.returncode == 0:
                logger.info("✅ Dependencies updated successfully.")
                return True
            else:
                logger.error("❌ Failed to update dependencies.")
                return False
        else:
            logger.warning("⚠ The current project is not scriptman!")
            return False

    def _update_scriptman(self) -> bool:
        """
        ⬆ Updates the 'scriptman' package to the latest version.
        """
        logger.info("🔄 Updating ScriptMan package...")
        result = run(["poetry", "install"], check=False)
        if result.returncode == 0:
            logger.info("✅ ScriptMan package updated successfully.")
            return True
        else:
            logger.error("❌ Failed to update ScriptMan package.")
            return False

    def _manage_scriptman_package(
        self,
        build: bool = False,
        update: bool = True,
        publish: bool = False,
    ) -> bool:
        """
        📦 Manage the 'scriptman' package.

        Args:
            build (bool): Build the package (default: False).
            update (bool): Update the package and its dependencies (default: True).
            publish (bool): Publish the package (default: False).

        Returns:
            bool: True if succeeded, False otherwise.
        """
        if update:
            self._update_scriptman_dependencies()
            self._update_version_on_pyproject()
            self._update_scriptman()

        return True

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


# Singleton instance
config_handler: ConfigHandler = ConfigHandler()
__all__ = ["config_handler"]
