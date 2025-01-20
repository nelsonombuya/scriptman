from os import getcwd
from pathlib import Path
from subprocess import PIPE, run
from typing import Callable, Literal, Optional, get_args, get_origin

from dynaconf import Dynaconf
from loguru import logger
from tomlkit import dumps, parse


class ConfigHandler:
    _instance: Optional["ConfigHandler"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    @property
    def cwd(self) -> Path:
        """Get the path to the current working directory for the application."""
        return Path(getcwd())

    @property
    def version(self) -> str:
        """Determine the current version of the Scriptman package."""
        try:
            version_data = {}
            version_file = Path(__file__).parent.parent / "version.toml"

            if version_file.exists():
                with version_file.open("r", encoding="utf-8") as f:
                    version_data = parse(f.read())

            major_version = version_data.get("version", "2.~")
            major_version_parts = major_version.split(".")

            # Calculate minor version based on the number of commits
            minor_version = run(
                ["git", "rev-list", "--count", "HEAD"],
                stdout=PIPE,
                stderr=PIPE,
                check=True,
                text=True,
            ).stdout.strip()

            full_version = (
                f"{major_version_parts[0]}.{major_version_parts[1]}.{minor_version}"
            )

            with version_file.open("w", encoding="utf-8") as f:
                version_data["version"] = full_version
                f.write(dumps(version_data))

            logger.debug(f"Scriptman v{full_version}")
            return full_version

        except Exception as e:
            logger.error(f"Failed to determine or update version: {e}")
            return "2.~.~"

    @property
    def configs(self) -> dict[str, dict]:
        """Define the allowable configuration parameters and their expected types."""
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
        }

    def _initialize(self) -> None:
        """Initialize the configuration object and required directories."""
        self.config = Dynaconf(
            root_path=self.cwd,
            settings_files=["scriptman.toml", ".secrets.toml"],
        )
        self._initialize_defaults()
        self._initialize_logs_dir()

    def _initialize_defaults(self) -> None:
        """Initialize the default configuration parameters."""
        for param, config in self.configs.items():
            if param not in self.config:
                self.config.set(param, config["default"])

        # Initialize class variables
        self.callback_function: Optional[Callable[[Exception, dict], None]] = None

    def _initialize_logs_dir(self) -> None:
        """Initialize the logs directory for the application."""
        Path(self.config.logs_dir).mkdir(parents=True, exist_ok=True)

    def _update_configuration(self, param: str, value) -> bool:
        """Update a configuration parameter in memory and save it to the settings file."""
        logger.debug(f"Updating configuration: {param} = {value}")

        if param not in self.configs:
            allowed_params = "\n\t".join(
                f"{parameter}: {details['type'].__qualname__}"
                for parameter, details in self.configs.items()
            )
            logger.error(
                f"Invalid configuration parameter: {param}.\n"
                f"Allowed parameters are:\n\t{allowed_params}"
            )
            return False

        if param.lower() == "retry" and value < 0:
            logger.error("Invalid value for retries: Must be >= 0.")
            return False

        expected_type = self.configs[param]["type"]

        if get_origin(expected_type) == Literal:
            allowed_values = get_args(expected_type)
            if value not in allowed_values:
                logger.error(
                    f"Invalid value for {param}: {value}.\n"
                    f"Allowed values are: {', '.join(map(str, allowed_values))}"
                )
                return False

        try:
            if (
                expected_type is bool
                and isinstance(value, str)
                and value.lower() in ["true", "false"]
            ):
                value = value.lower() == "true"
            elif get_origin(expected_type) != Literal:
                value = expected_type(value)
            else:
                raise ValueError

        except (ValueError, TypeError):
            logger.error(
                f"Invalid value type for {param}: {value}.\n"
                f"Expected type: {expected_type.__qualname__}"
            )
            return False

        # Update in-memory configuration
        param = param.upper()
        self.config.set(param, value)

        # Persist the update to the TOML file
        try:
            settings_file = self.cwd / "scriptman.toml"
            config_data = self.config.to_dict()

            if settings_file.exists():
                with settings_file.open("r", encoding="utf-8") as f:
                    config_data = parse(f.read())

            config_data[param] = value
            with settings_file.open("w", encoding="utf-8") as f:
                f.write(dumps(config_data))

            logger.info(f"Configuration updated successfully: {param} = {value}")
            return True
        except Exception as e:
            logger.error(f"Failed to update configuration file: {e}")
            return False

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
        Updates the dependencies of the 'scriptman' project in the pyproject.toml file.
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
        Updates the 'scriptman' package to the latest version.
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
        if update:
            self._update_scriptman_dependencies()
            self._update_version_on_pyproject()
            self._update_scriptman()

        return True

    def add_callback_function(
        self, callback_function: Callable[[Exception, dict], None]
    ) -> bool:
        """
        Add a callback function to the ConfigHandler instance.
        It will be called when a script fails to execute.
        """
        if not callable(callback_function):
            logger.error("Invalid callback function provided.")
            return False

        self.callback_function = callback_function
        return True


# Singleton instance of ConfigHandler
config_handler: ConfigHandler = ConfigHandler()
__all__ = ["config_handler"]
