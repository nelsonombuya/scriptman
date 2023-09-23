import json
from typing import Any, Dict, List, Optional


class SettingsHandler:
    """
    Singleton class for managing ScriptManager Application settings.

    This class provides methods to manage various settings for the
    ScriptManager application, including logging, debugging, Selenium
    optimizations, custom driver settings, and more.

    Attributes:
        app_dir (str): The root directory of the application.
        log_mode (bool): Whether logging is enabled.
        debug_mode (bool): Whether debugging is enabled.
        clean_up_logs_after_n_days (int): Number of days after which log files
            should be cleaned up.
        selenium_optimizations_mode (bool): Whether Selenium optimizations are
            enabled.
        selenium_custom_driver_mode (bool): Whether custom Selenium driver
            mode is enabled.
        selenium_custom_driver_version (int): The major version of Chrome to
            use with custom Selenium driver.
        selenium_keep_downloaded_custom_driver (bool): Whether to keep the
            downloaded custom Selenium driver.
        selenium_chrome_url (str): The URL for downloading Chrome
            binaries/drivers.
        clean_up_folders (List[str]): List of folders to be cleaned up.

    Methods:
        init(app_dir: str, logging: bool = True) -> None:
            Initialize the application settings.
        get_setting(setting: str, default: Any = None) -> Any:
            Get the value of a specific setting and return the default value if
            the setting is not found.
        enable_logging() -> None:
            Enable logging mode.
        disable_logging() -> None:
            Disable logging mode.
        enable_debugging() -> None:
            Enable debugging mode.
        disable_debugging() -> None:
            Disable debugging mode.
        add_folders_for_cleanup(folders: List[str]) -> None:
            Add a list of folders to be cleaned up when the ScriptManager is
            done.
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
        keep_selenium_custom_driver_after_use() -> None:
            Keep the Selenium custom driver once it has been downloaded and
            used.
        delete_selenium_custom_driver_after_use() -> None:
            Delete the Selenium custom driver once it has been downloaded and
            used.
        set_selenium_chrome_url(url: str) -> None:
            Set the URL to use when downloading Chrome binaries/drivers.
        set_app_dir(directory: str) -> None:
            Set the main app's directory.
        set_clean_up_logs_after_n_days(days: int) -> None:
            Set the number of days after which log files should be cleaned up.
        add_default_database_connection_string(
            connection_string: Dict[str, str]
        ) -> None:
            Add a default database connection string.
        view_default_database_connection_strings() -> None:
            View the default database connection strings.
        remove_default_database_connection_string(key: str) -> None:
            Remove a default database connection string.

        Private Methods:
        _log_change(name: str, value: Optional[Any]) -> None:
            Log changes to settings.
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
        self.clean_up_folders: List[str] = []
        self.clean_up_logs_after_n_days: int = 7
        self.default_database_connection_strings = {}
        self.selenium_optimizations_mode: bool = True
        self.selenium_custom_driver_mode: bool = False
        self.selenium_custom_driver_version: int = 116
        self.selenium_keep_downloaded_custom_driver: bool = True
        self.selenium_chrome_url: str = (
            "https://googlechromelabs.github.io/chrome-for-testing/"
            "known-good-versions-with-downloads.json"
        )

    def init(self, app_dir: str, logging: bool = True) -> None:
        """
        Initialize the application settings.

        Args:
            app_dir (str): The root directory of the application.
            logging (bool): Flag for enabling logging for the session.
                Default is True.
        """
        self.log_mode = logging
        self.set_app_dir(app_dir)

        from script_manager.handlers.directories import DirectoryHandler
        from script_manager.handlers.logs import LogHandler

        directory_handler = DirectoryHandler()
        self.add_folders_for_cleanup(
            [
                directory_handler.root_dir,
                directory_handler.script_man_dir,
            ]
        )
        LogHandler("Script Manager").message(
            details=vars(self),
            print_to_terminal=self.debug_mode,
            message="The application has been initialized as follows:",
        )

    def get_setting(self, setting: str, default: Any = None) -> Any:
        """
        Retrieve the value of a specific setting from the object.

        Args:
            setting (str): The name of the setting to retrieve.
            default (Any): The default value to return if the setting doesn't
                exist (default is None).

        Returns:
            The value of the requested setting, or the default value if the
            setting is not found.
        """
        try:
            return getattr(self, setting)
        except AttributeError:
            return default

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

    def add_folders_for_cleanup(self, folders: List[str]) -> None:
        """
        Add a list of folders to be cleaned up when the ScriptManager is done.
        """
        self.clean_up_folders.extend(folders)
        self._log_change("Folders to be cleaned:", self.clean_up_folders)

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

    def keep_selenium_custom_driver_after_use(self) -> None:
        """
        Keep the selenium custom driver once it has been downloaded and used.
        """
        self.selenium_keep_downloaded_custom_driver = True
        self._log_change("selenium_keep_downloaded_custom_driver", True)

    def delete_selenium_custom_driver_after_use(self) -> None:
        """
        Delete the selenium custom driver once it has been downloaded and used.
        """
        self.selenium_keep_downloaded_custom_driver = False
        self._log_change("selenium_keep_downloaded_custom_driver", False)

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

    def add_default_database_connection_string(
        self, connection_string: Dict[str, str]
    ) -> None:
        """
        Add or update default database connection strings.

        This method allows you to add or update default database connection
        strings. You can provide a dictionary where keys represent connection
        names, and values represent connection strings. If a connection name
        already exists, its connection string will be updated.

        Args:
            connection_string (Dict[str, str]): A dictionary containing
                database connection information, where keys represent
                connection names and values represent connection strings.

        Example:
            settings = SettingsHandler()
            settings.add_default_database_connection_string(
                {"Connection1": "mysql://user:password@localhost/db1"}
            )
        """
        self.default_database_connection_strings.update(connection_string)
        self._log_change(
            "default_database_connection_strings",
            self.default_database_connection_strings,
        )

    def generate_and_add_default_database_connection_string(
        self,
        driver: str,
        server: str,
        database: str,
        username: str,
        password: str,
    ) -> None:
        """
        Generate and add or update a default database connection string.

        This method generates a connection string based on the provided
        parameters and adds or updates it to the default database connection
        strings dictionary.

        Args:
            driver (str): The database driver name (e.g., "SQL Server").
            server (str): The database server address (e.g., "localhost").
            database (str): The database name.
            username (str): The username for authentication.
            password (str): The password for authentication.

        Example:
            settings = SettingsHandler()
            settings.generate_and_add_default_database_connection_string(
                driver="SQL Server",
                server="localhost",
                database="db",
                username="user",
                password="password"
            )
        """
        connection_string = (
            f"Driver={driver};"
            f"Server={server};"
            f"Database={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        self.default_database_connection_strings[database] = connection_string
        self._log_change(
            "default_database_connection_strings",
            self.default_database_connection_strings,
        )

    def view_default_database_connection_strings(self):
        """
        View the default database connection strings.

        This method displays the currently stored default database connection
        strings in a readable JSON format with an indentation of 4 spaces.

        Example:
            settings = SettingsHandler()
            settings.view_default_database_connection_strings()
        """
        print(json.dumps(self.default_database_connection_strings, indent=4))

    def remove_default_database_connection_string(self, key: str) -> None:
        """
        Remove a default database connection string.

        This method allows you to remove a default database connection string
        by providing its key (connection name).

        Args:
            key (str): The key (connection name) of the connection string to be
                removed.

        Example:
            settings = SettingsHandler()
            settings.remove_default_database_connection_string("Connection1")
        """
        removed_value = self.default_database_connection_strings.pop(key)
        self._log_change(
            f"default_database_connection_strings[{key}]",
            removed_value,
        )

    def _log_change(self, name: str, value: Optional[Any]) -> None:
        """
        Log changes to settings.

        Args:
            name (str): The name of the setting being changed.
            value: The new value of the setting.
        """
        from script_manager.handlers.logs import LogHandler

        LogHandler("Settings Handler").message(
            print_to_terminal=self.debug_mode,
            message=f"{name} updated to {value}",
        )

    def __str__(self) -> str:
        """
        Get a string representation of the current settings.

        Returns:
            str: A string representation of the current settings.
        """
        return json.dumps(vars(self), indent=4)


# Create a single instance of SettingsHandler
settings = SettingsHandler()
