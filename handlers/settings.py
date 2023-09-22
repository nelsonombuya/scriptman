import json
from typing import Any, Optional


class SettingsHandler:
    """
    Singleton class for managing ScriptManager Application settings.

    This class provides methods to manage various settings for the ScriptManager application,
    including logging, debugging, Selenium optimizations, custom driver settings, and more.

    Attributes:
        app_dir (str): The root directory of the application.
        log_mode (bool): Whether logging is enabled.
        debug_mode (bool): Whether debugging is enabled.
        clean_up_logs_after_n_days (int): Number of days after which log files should be cleaned up.
        selenium_optimizations_mode (bool): Whether Selenium optimizations are enabled.
        selenium_custom_driver_mode (bool): Whether custom Selenium driver mode is enabled.
        selenium_custom_driver_version (int): The major version of Chrome to use with custom Selenium driver.
        selenium_chrome_url (str): The URL for downloading Chrome binaries/drivers.

    Methods:
        init(app_dir: str, log_mode: bool, debug_mode: bool) -> None:
            Initialize the application settings.
        enable_logging() -> None:
            Enable logging mode.
        disable_logging() -> None:
            Disable logging mode.
        enable_debugging() -> None:
            Enable debugging mode.
        disable_debugging() -> None:
            Disable debugging mode.
        enable_selenium_optimizations_mode() -> None:
            Enable Selenium optimizations.
        disable_selenium_optimizations_mode() -> None:
            Disable Selenium optimizations.
        enable_selenium_custom_driver_mode() -> None:
            Enable custom Selenium driver mode.
        disable_selenium_custom_driver_mode() -> None:
            Disable custom Selenium driver mode.
        set_selenium_custom_driver_version(version: int) -> None:
            Set the version of Chrome to use with custom Selenium driver.
        set_selenium_chrome_url(url: str) -> None:
            Set the URL for downloading Chrome binaries/drivers.
        set_app_dir(directory: str) -> None:
            Set the main app's directory.
        set_clean_up_logs_after_n_days(days: int) -> None:
            Set the number of days after which log files should be cleaned up.
        __str__() -> str:
            Get a string representation of the current settings.

    """

    _instance = None

    def __new__(cls) -> "SettingsHandler":
        """
        Create a single instance of SettingsManager if it doesn't exist.
        """
        if cls._instance is None:
            cls._instance = super(SettingsHandler, cls).__new__(cls)
            cls._instance.initialize_defaults()
        return cls._instance

    def initialize_defaults(self) -> None:
        """
        Initialize default settings.
        """
        self.app_dir: str = ""
        self.log_mode: bool = True
        self.debug_mode: bool = False
        self.clean_up_logs_after_n_days: int = 7
        self.selenium_optimizations_mode: bool = True
        self.selenium_custom_driver_mode: bool = False
        self.selenium_custom_driver_version: int = 116
        self.selenium_chrome_url: str = (
            "https://googlechromelabs.github.io/chrome-for-testing/"
            "known-good-versions-with-downloads.json"
        )

    def init(self, app_dir: str, log_mode: bool, debug_mode: bool) -> None:
        """
        Initialize the application settings.

        Args:
            app_dir (str): The root directory of the application.
            log_mode (bool): Whether logging is enabled.
            debug_mode (bool): Whether debugging is enabled.
        """
        self.app_dir = app_dir
        self.log_mode = log_mode
        self.debug_mode = debug_mode
        log_msg = {"app_dir": app_dir, "log": log_mode, "debug": debug_mode}
        self._log_change("Script Manager", json.dumps(log_msg, indent=4))

    def enable_logging(self) -> None:
        """
        Enable logging mode.
        """
        self.log_mode = True
        self._log_change("log_mode", True)

    def disable_logging(self) -> None:
        """
        Disable logging mode.
        """
        self.log_mode = False
        self._log_change("log_mode", False)

    def enable_debugging(self) -> None:
        """
        Enable debugging mode.
        """
        self.debug_mode = True
        self._log_change("debug_mode", True)

    def disable_debugging(self) -> None:
        """
        Disable debugging mode.
        """
        self.debug_mode = False
        self._log_change("debug_mode", False)

    def enable_selenium_optimizations_mode(self) -> None:
        """
        Enable Selenium optimizations.
        """
        self.selenium_optimizations_mode = True
        self._log_change("selenium_optimizations_mode", True)

    def disable_selenium_optimizations_mode(self) -> None:
        """
        Disable Selenium optimizations.
        """
        self.selenium_optimizations_mode = False
        self._log_change("selenium_optimizations_mode", False)

    def enable_selenium_custom_driver_mode(self) -> None:
        """
        Enable custom Selenium driver mode.
        """
        self.selenium_custom_driver_mode = True
        self._log_change("selenium_custom_driver_mode", True)

    def disable_selenium_custom_driver_mode(self) -> None:
        """
        Disable custom Selenium driver mode.
        """
        self.selenium_custom_driver_mode = False
        self._log_change("selenium_custom_driver_mode", False)

    def set_selenium_custom_driver_version(self, version: int) -> None:
        """
        Set the version of Chrome to use with custom Selenium driver.

        Args:
            version (int): The major version of Chrome to use.
        """
        self.selenium_custom_driver_version = version
        self._log_change("selenium_custom_driver_version", version)

    def set_selenium_chrome_url(self, url: str) -> None:
        """
        Set the URL to use when downloading Chrome binaries/drivers.

        Args:
            url (str): The URL to use when downloading Chrome browser/drivers.
        """
        self.selenium_chrome_url = url
        self._log_change("selenium_chrome_url", self.selenium_chrome_url)

    def set_app_dir(self, directory: str) -> None:
        """
        Set the main app's directory.

        Args:
            directory (str): The directory path to set as the app's root dir.
        """
        self.app_dir = directory
        self._log_change("app_dir", directory)

    def set_clean_up_logs_after_n_days(self, days: int) -> None:
        """
        Set the number of days after which log files should be cleaned up.

        This method allows you to configure the number of days after which log
        files should be automatically cleaned up by the application. Log files
        older than the specified number of days will be deleted during the
        cleanup process.

        Args:
            days (int): The number of days after which log files should be
            cleaned up.

        Example:
            To set log cleanup after 14 days:
            >>> settings.set_clean_up_logs_after_n_days(14)
        """
        self.clean_up_logs_after_n_days = days
        self._log_change("clean_up_logs_after_n_days", days)

    def _log_change(self, name: str, value: Optional[Any]) -> None:
        """
        Log changes to settings.

        Args:
            name (str): The name of the setting being changed.
            value: The new value of the setting.
        """
        from .logs import LogHandler

        LogHandler("Settings Handler").message(f"{name} updated to {value}")

    def __str__(self) -> str:
        """
        Get a string representation of the current settings.

        Returns:
            str: A string representation of the current settings.
        """
        return json.dumps(vars(self), indent=4)


# Create a single instance of SettingsHandler
settings = SettingsHandler()
