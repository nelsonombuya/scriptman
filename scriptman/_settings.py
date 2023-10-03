"""
ScriptMan - SettingsHandler

This module provides the SettingsHandler class for managing ScriptManager
application settings.

Usage:
- Import the SettingsHandler class from this module.
- Initialize the SettingsHandler class to manage application settings.
- Use the provided methods to enable or disable settings and customize the
application behavior.

Class:
- `SettingsHandler`: Manages ScriptManager application settings.

Attributes:
- None

Methods:
- `init(
    self,
    app_dir: str,
    logging: bool = True,
    debugging: bool = False
) -> None`: Initialize the application settings.
- `get_setting(self, setting: str, default: Any = None) -> Any`: Retrieve the
value of a specific setting.
- `enable_logging(self) -> None`: Enable logging mode.
- `disable_logging(self) -> None`: Disable logging mode.
- `enable_printing_logs_to_terminal(self) -> None`: Enable printing logs to the
terminal.
- `disable_printing_logs_to_terminal(self) -> None`: Disable printing logs to
the terminal.
- `enable_debugging(self) -> None`: Enable debugging mode.
- `disable_debugging(self) -> None`: Disable debugging mode.
- `add_folders_for_cleanup(self, folders: List[str]) -> None`: Add folders to
be cleaned up when the application is done.
- `enable_selenium_optimizations_mode(self) -> None`: Enable Selenium
optimizations.
- `disable_selenium_optimizations_mode(self) -> None`: Disable Selenium
optimizations.
- `enable_selenium_custom_driver_mode(self) -> None`: Enable custom Selenium
driver mode.
- `disable_selenium_custom_driver_mode(self) -> None`: Disable custom Selenium
driver mode.
- `set_selenium_custom_driver_version(self, version: int) -> None`: Set the
version of Chrome to use with custom Selenium driver.
- `keep_selenium_custom_driver_after_use(self) -> None`: Keep the Selenium
custom driver after it's downloaded and used.
- `delete_selenium_custom_driver_after_use(self) -> None`: Delete the Selenium
custom driver after it's downloaded and used.
- `set_selenium_chrome_url(self, url: str) -> None`: Set the URL to use when
downloading Chrome binaries/drivers.
- `set_app_dir(self, directory: str) -> None`: Set the main app's directory.
- `set_clean_up_logs_after_n_days(self, days: int) -> None`: Set the number of
days after which log files should be cleaned up.
- `add_csv_filename_to_ignore_during_cleanup(
    self,
    filename: str
) -> None`: Add a CSV filename to ignore during cleanup.
- `add_db_connection_string(
    self,
    connection_string: Dict[str, str]
) -> None`: Add or update a database connection string.
- `gen_and_add_db_connection_string(
    self,
    driver: str,
    server: str,
    database: str,
    username: str,
    password: str,
    port: Optional[str] = None
) -> None`: Generate and add or update a database connection string.
- `view_database_connection_strings(self) -> None`: View the default database
connection strings.
- `remove_database_connection_string(self, key: str) -> None`: Remove a
database connection string.
- `upgrade_scriptman(self) -> None`: Upgrade the ScriptMan application.
- `update_scripts(self) -> None`: Update application scripts from a Git
repository.

Private Methods:
- `_log_change(self, name: str, value: Optional[Any]) -> None`: Log changes to
settings.
- `__str__(self) -> str`: Get a string representation of the current settings.

Singleton Instance:
- `Settings`: Singleton instance of the SettingsHandler class.
"""

import json
import subprocess
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
        add_db_connection_string(connection_string: Dict[str, str]) -> None:
            Add a default database connection string.
        gen_and_add_db_connection_string(
            driver: str,
            server: str,
            database: str,
            username: str,
            password: str,
        ) -> None:
            Generate and add a default database connection string.
        view_database_connection_strings() -> None:
            View the default database connection strings.
        remove_database_connection_string(key: str) -> None:
            Remove a default database connection string.
        def upgrade_scriptman(self) -> None:
            Upgrade the ScriptMan application to the latest version.
        def update_scripts(self) -> None:
            Update application scripts from a Git repository.
        def use_venv(self) -> None:
            Activate the virtual environment.
        Private Methods:
        _log_change(name: str, value: Optional[Any]) -> None:
            Log changes to settings.
        __str__() -> str:
            Get a string representation of the current settings.
        __del__() -> None:
            Disables virtual environment once the object is deleted.
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
        self.venv_name: str = ""
        self.log_mode: bool = True
        self.debug_mode: bool = False
        self.clean_up_folders: List[str] = []
        self.database_connection_strings = {}
        self.clean_up_logs_after_n_days: int = 7
        self.print_logs_to_terminal: bool = True
        self.selenium_optimizations_mode: bool = True
        self.selenium_custom_driver_mode: bool = False
        self.selenium_custom_driver_version: int = 116
        self.ignore_csv_filename_during_cleanup: set = set()
        self.selenium_keep_downloaded_custom_driver: bool = True
        self.selenium_chrome_url: str = (
            "https://googlechromelabs.github.io/chrome-for-testing/"
            "known-good-versions-with-downloads.json"
        )

    def init(
        self,
        app_dir: str,
        logging: bool = True,
        debugging: bool = False,
    ) -> None:
        """
        Initialize the application settings.

        Args:
            app_dir (str): The root directory of the application.
            logging (bool): Flag for enabling logging for the session.
                Default is True.
            debugging (bool): Flag for enabling debugging mode for the session.
                Default is False.
        """
        self.log_mode = logging
        self.debug_mode = debugging
        self.set_app_dir(app_dir)

        from scriptman._directories import DirectoryHandler
        from scriptman._logs import LogHandler, LogLevel

        directory_handler = DirectoryHandler()
        self.add_folders_for_cleanup(
            [
                directory_handler.root_dir,
                directory_handler.script_man_dir,
            ]
        )
        LogHandler("Script Manager").message(
            details=vars(self),
            level=LogLevel.DEBUG,
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

    def enable_printing_logs_to_terminal(self) -> None:
        """
        Enable printing logs to terminal.
        """
        self.print_logs_to_terminal = True
        self._log_change("print_logs_to_terminal", True)

    def disable_printing_logs_to_terminal(self) -> None:
        """
        Disable printing logs to terminal.
        """
        self.print_logs_to_terminal = False
        self._log_change("print_logs_to_terminal", False)

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

    def add_csv_filename_to_ignore_during_cleanup(self, filename: str) -> None:
        self.ignore_csv_filename_during_cleanup.add(filename)
        self._log_change(
            "ignore_csv_filename_during_cleanup",
            self.ignore_csv_filename_during_cleanup,
        )

    def add_db_connection_string(
        self,
        connection_string: Dict[str, str],
    ) -> None:
        """
        Add or update a database connection strings.

        This method allows you to add or update database connection strings.
        You can provide a dictionary where keys represent connection names, and
        values represent connection strings. If a connection name already
        exists, its connection string will be updated.

        Args:
            connection_string (Dict[str, str]): A dictionary containing
                database connection information, where keys represent
                connection names and values represent connection strings.

        Example:
            settings = SettingsHandler()
            settings.add_db_connection_string(
                {"Connection1": "mysql://user:password@localhost/db1"}
            )
        """
        self.database_connection_strings.update(connection_string)
        self._log_change(
            "default_database_connection_strings",
            self.database_connection_strings,
        )

    def gen_and_add_db_connection_string(
        self,
        driver: str,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[str] = None,
    ) -> None:
        """
        Generate and add or update a database connection string.

        This method generates a connection string based on the provided
        parameters and adds or updates it to the database connection
        strings dictionary.

        Args:
            driver (str): The database driver name (e.g., "SQL Server").
            server (str): The database server address (e.g., "localhost").
            database (str): The database name.
            username (str): The username for authentication.
            password (str): The password for authentication.
            port (optional, str): The database server port.

        Example:
            settings = SettingsHandler()
            settings.gen_and_add_db_connection_string(
                driver="SQL Server",
                server="localhost",
                database="db",
                username="user",
                password="password"
            )
        """
        connection_string = (
            (
                f"Driver={driver};"
                f"Server={server};"
                f"Port={port};"
                f"Database={database};"
                f"UID={username};"
                f"PWD={password}"
            )
            if port
            else (
                f"Driver={driver};"
                f"Server={server};"
                f"Database={database};"
                f"UID={username};"
                f"PWD={password}"
            )
        )
        self.database_connection_strings[database] = connection_string
        self._log_change(
            "default_database_connection_strings",
            self.database_connection_strings,
        )

    def view_database_connection_strings(self):
        """
        View the database connection strings.

        This method displays the currently stored default database connection
        strings in a readable JSON format with an indentation of 4 spaces.

        Example:
            settings = SettingsHandler() or Settings
            settings.view_database_connection_strings()
        """
        print(json.dumps(self.database_connection_strings, indent=4))

    def remove_database_connection_string(self, key: str) -> None:
        """
        Remove a database connection string.

        This method allows you to remove a database connection string by
        providing its key (connection name).

        Args:
            key (str): The key (connection name) of the connection string to be
                removed.

        Example:
            settings = SettingsHandler() or Settings
            settings.remove_database_connection_string("Connection1")
        """
        removed_value = self.database_connection_strings.pop(key)
        self._log_change(
            f"default_database_connection_strings[{key}]",
            removed_value,
        )

    def use_venv(self, venv_name: str = ".venv") -> None:
        """
        Activate the virtual environment.

        This method activates the virtual environment associated with the
        ScriptManager application. It sets the `venv_name` attribute to True
        and runs the activation script for the virtual environment located in
        the `Scripts` directory. This allows the application to work within the
        isolated virtual environment, which may contain specific dependencies
        and packages.

        Args:
            venv_name (str): The name of the virtual environment folder.
                Defaults to '.venv'.

        Example:
            settings = SettingsHandler()
            settings.use_venv()

        Note:
            To use this method successfully, ensure that the virtual
            environment (venv) has been created and configured correctly for
            the ScriptManager application.

        """
        if self.app_dir:
            self.venv_name = venv_name
            subprocess.run([rf"{self.app_dir}\{venv_name}\Scripts\activate"])
            self._log_change("Virtual Environment", "Activated")
        else:
            self._log_change("Virtual Environment", "APP DIR is undefined.")

    def upgrade_scriptman(self) -> None:
        """
        Upgrade the ScriptMan application to the latest version.

        This method uses the Python package manager `pip` to upgrade the
        ScriptMan application to the latest available version. It runs the
        following command to perform the upgrade:

        ```
        python -m pip install scriptman --upgrade
        ```

        Example:
            settings = SettingsHandler() or Settings
            settings.upgrade_scriptman()

        Note:
            This method requires an active internet connection to fetch the
            latest version of the ScriptMan application from the Python Package
            Index (PyPI). Ensure that the `pip` tool is properly installed and
            configured in your Python environment.

        """
        subprocess.run(
            [
                "python",
                "-m",
                "pip",
                "install",
                "scriptman",
                "--upgrade",
            ]
        )
        self._log_change("ScriptMan", "Latest Version")

    def update_scripts(self) -> None:
        """
        Update application scripts from a Git repository.

        This method updates the application scripts by performing a `git pull`
        operation within the application's root directory. It assumes that the
        application's source code is stored in a Git repository, and this
        method pulls the latest changes from the repository.

        Example:
            settings = SettingsHandler()
            settings.update_scripts()

        Note:
            This method is designed for applications that use Git for version
            control. Ensure that the Git repository is properly configured and
            accessible as the APP DIR.

        """
        if self.app_dir:
            subprocess.run(["cd", self.app_dir])
            subprocess.run(["git", "pull"])
            self._log_change("Scripts", "Latest Commit on Repository")
        else:
            self._log_change("Scripts", "APP DIR is undefined.")

    def _log_change(self, name: str, value: Optional[Any]) -> None:
        """
        Log changes to settings.

        Args:
            name (str): The name of the setting being changed.
            value: The new value of the setting.
        """
        from scriptman._logs import LogHandler, LogLevel

        LogHandler("Settings Handler").message(
            level=LogLevel.DEBUG,
            print_to_terminal=self.debug_mode,
            message=f"{name} updated to {json.dumps(value, indent=4)}",
        )

    def __str__(self) -> str:
        """
        Get a string representation of the current settings.

        Returns:
            str: A string representation of the current settings.
        """
        return json.dumps(vars(self), indent=4)

    def __del__(self) -> None:
        """
        Deactivate the virtual environment upon object deletion.

        This method is automatically called when an instance of the
        `SettingsHandler` class is deleted. If the virtual environment (`venv`)
        was activated using the `use_venv` method, this method deactivates it
        to return to the system's default Python environment. Deactivating the
        virtual environment is a good practice to ensure that it doesn't affect
        other Python processes or environments after its usage.

        Example:
            settings = SettingsHandler() or Settings
            settings.use_venv()
            # ... perform operations within the virtual environment ...
            del settings  # Deactivate the virtual environment upon deletion.

        Note:
            Ensure that the virtual environment was previously activated using
            the `use_venv` method before calling this method. If the virtual
            environment was not activated, calling this method will have no
            effect.

        """
        if self.venv_name:
            command = [rf"{self.app_dir}\{self.venv_name}\Scripts\deactivate"]
            subprocess.run(command)


class SeleniumBrowserIndex:
    """
    Singleton class for managing Selenium browser index.

    This class provides methods for managing the index used to track Selenium
    browser instances. It ensures that there is only one instance of the index
    within the application.

    Attributes:
        index (int): The current index value.

    Methods:
        get_index() -> int:
            Get the current index value.

        set_index(index: int) -> None:
            Set the index value to the specified integer.

        max_index() -> int:
            Get the maximum index value based on the length of the Selenium
            browser queue.

    """

    _instance = None

    def __new__(cls):
        """
        Create a single instance of SeleniumBrowserIndex if it doesn't exist.
        """
        if cls._instance is None:
            cls._instance = super(SeleniumBrowserIndex, cls).__new__(cls)
            cls._instance.index = 0
        return cls._instance

    def get_index(self) -> int:
        """
        Get the current index value.

        Returns:
            int: The current index value.
        """
        return self.index

    def set_index(self, index: int) -> None:
        """
        Set the index value to the specified integer.

        Args:
            index (int): The new index value to set.
        """
        self.index = index

    def max_index(self) -> int:
        """
        Get the maximum index value based on the length of the Selenium browser
        queue.

        Returns:
            int: The maximum index value.
        """
        from scriptman._selenium import BROWSER_QUEUE

        return len(BROWSER_QUEUE)


# Singleton Instances
Settings = SettingsHandler()
SBI = SeleniumBrowserIndex()
