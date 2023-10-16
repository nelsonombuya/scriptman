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
- `enable_selenium_optimizations(self) -> None`: Enable Selenium optimizations.
- `disable_selenium_optimizations(self) -> None`: Disable Selenium
optimizations.
- `enable_selenium_custom_driver(self) -> None`: Enable custom Selenium
driver.
- `disable_selenium_custom_driver(self) -> None`: Disable custom Selenium
driver.
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
- `add_csv_filename_to_ignore_during_maintenance(
    self,
    filename: str
) -> None`: Add a CSV filename to ignore during maintenance.
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
- `view_database_connection_strings(self) -> None`: View the added database
connection strings.
- `remove_database_connection_string(self, key: str) -> None`: Remove a
database connection string.
- `update_sagerun_code (self, int)`: Update code to use when running
Disk Cleanup during system maintenance.
(Check scriptman.Cleanup.run_system_maintenance for more info).
- `enable_system_maintenance (self)`: Enable scriptman's system
maintenance scripts to run. These include sfc, dism, disk cleanup and
defragmentation.
- `disable_system_maintenance (self)`: Disable scriptman's system maintenance
scripts from running.
- `update_system_maintenance_day (self, int)`: Update the monthly date to run
the system maintenance scripts.
    Note:
        Dates 30 and 31 will automatically pick the last date of the
        month in February (28th or 29th in leap years).
        Date 31 will automatically pick the last date of the month for
        months with 30 days.
- `enable_system_restart (self)`: Enable system restart after the maintenance
scripts are completed.
- `disable_system_restart (self)`: Disables system restart after the
maintenance scripts are completed.

Private Methods:
- `_log_change(self, name: str, value: Optional[Any]) -> None`: Log changes to
settings.
- `__str__(self) -> str`: Get a string representation of the current settings.

Singleton Instance:
- `Settings`: Singleton instance of the SettingsHandler class.
"""

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
        selenium_optimizations (bool): Whether Selenium optimizations are
            enabled.
        selenium_custom_driver (bool): Whether custom Selenium driver is
            enabled.
        selenium_custom_driver_version (int): The major version of Chrome to
            use with custom Selenium driver.
        selenium_keep_downloaded_custom_driver (bool): Whether to keep the
            downloaded custom Selenium driver.
        selenium_chrome_url (str): The URL for downloading Chrome
            binaries/drivers.
        maintenance_folders (List[str]): Custom list of folders to be cleaned
            up (Remove __pycache__ folders).
        sagerun_code (int): Code to use when running Disk Cleanup during
            system maintenance. Defaults to 11.
            (Check scriptman.Maintenance.run_system_maintenance for more info).
        system_maintenance (bool): Enable scriptman's system maintenance
            scripts to run. These include sfc, dism, disk cleanup, disk
            defragmentation, and custom scripts to clean up downloaded files
            and __pycache__ folders. Defaults to False.
        system_maintenance_day (int): The monthly day to run the system
            maintenance scripts. Defaults to 31.

            Note:
                Dates 30 and 31 will automatically pick the last date of the
                month in February (28th or 29th in leap years).
                Date 31 will automatically pick the last date of the month for
                months with 30 days.
        restart_system_after_maintenance (bool): Restart the system 5 minutes
            after system maintenance is complete. Defaults to False.


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
        enable_selenium_optimizations() -> None:
            Enable Selenium optimizations.
        disable_selenium_optimizations() -> None:
            Disable Selenium optimizations.
        enable_selenium_custom_driver() -> None:
            Enable custom Selenium driver mode.
        disable_selenium_custom_driver() -> None:
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
        def update_sagerun_code (self, int): Update code to use when running
            Disk Cleanup during system maintenance.
            (Check scriptman.Cleanup.run_system_maintenance for more info).
        def enable_system_maintenance (self): Enable scriptman's system
            maintenance scripts to run. These include sfc, dism, disk cleanup
            and defragmentation.
        def disable_system_maintenance (self): Disable scriptman's system
            maintenance scripts from running.
        def update_system_maintenance_day (self, int): Update the monthly date
            to run the system maintenance scripts.

            Note:
                Dates 30 and 31 will automatically pick the last date of the
                month in February (28th or 29th in leap years).
                Date 31 will automatically pick the last date of the month for
                months with 30 days.
        def enable_system_restart (self): Enable system restart after the
            maintenance scripts are completed.
        def disable_system_restart (self): Disables system restart after the
            maintenance scripts are completed.

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
        self.sagerun_code: int = 11
        self.debug_mode: bool = False
        self.system_maintenance: bool = False
        self.system_maintenance_day: int = 31
        self.maintenance_folders: List[str] = []
        self.print_logs_to_terminal: bool = True
        self.selenium_optimizations: bool = True
        self.selenium_custom_driver: bool = False
        self.clean_up_logs_after_n_days: int = 30
        self.selenium_custom_driver_version: int = 116
        self.restart_system_after_maintenance: bool = False
        self.database_connection_strings: Dict[str, str] = {}
        self.ignore_csv_filename_during_maintenance: set = set()
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

        from shutil import copy2
        from os.path import join
        from scriptman._logs import LogHandler, LogLevel
        from scriptman._directories import DirectoryHandler

        self.log_mode = logging
        self.debug_mode = debugging
        self.set_app_dir(app_dir)
        directory_handler = DirectoryHandler()
        self.add_folders_for_cleanup(
            [
                directory_handler.root_dir,
                directory_handler.script_man_dir,
            ]
        )

        copy2(
            join(directory_handler.script_man_dir, "_scriptman.bat"),
            join(app_dir, "scriptman.bat"),
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
        self.maintenance_folders.extend(folders)
        self._log_change("Folders to be cleaned:", self.maintenance_folders)

    def enable_selenium_optimizations(self) -> None:
        """
        Enable Selenium optimizations.
        """
        self.selenium_optimizations = True
        self._log_change("selenium_optimizations", True)

    def disable_selenium_optimizations(self) -> None:
        """
        Disable Selenium optimizations.
        """
        self.selenium_optimizations = False
        self._log_change("selenium_optimizations", False)

    def enable_selenium_custom_driver(self) -> None:
        """
        Enable custom Selenium driver mode.
        """
        self.selenium_custom_driver = True
        self._log_change("selenium_custom_driver", True)

    def disable_selenium_custom_driver(self) -> None:
        """
        Disable custom Selenium driver mode.
        """
        self.selenium_custom_driver = False
        self._log_change("selenium_custom_driver", False)

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

    def add_csv_filename_to_ignore_during_maintenance(
        self,
        filename: str,
    ) -> None:
        """
        Add a csv filename to ignore during system maintenance.
        For example: "tickets", "transactions".

        Args:
            filename (str): The word included in the filename to ignore.
        """
        self.ignore_csv_filename_during_maintenance.add(filename)
        self._log_change(
            "ignore_csv_filename_during_maintenance",
            self.ignore_csv_filename_during_maintenance,
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
        name: Optional[str] = None,
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
            name (optional, str): The identifier for the connection string.
                If not specified, will use the database's name instead.

        Example:
            settings = SettingsHandler()
            settings.gen_and_add_db_connection_string(
                driver="SQL Server",
                server="localhost",
                database="db",
                username="user",
                password="password",
                port="123",
                name="Test DB"
            )
        """
        connection_string = (
            f"Driver={driver};"
            + f"Server={server};"
            + (f"Port={port};" if port else "")
            + f"Database={database};"
            + f"UID={username};"
            + f"PWD={password}"
        )
        self.database_connection_strings[name or database] = connection_string
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

    def update_sagerun_code(self, code: int) -> None:
        """
        Updates code to use when running Disk Cleanup during system
        maintenance.

        (Check scriptman.Cleanup.run_system_maintenance for more info).

        Args:
            code (int): The code to use.

        Note:
            Ensure to run your main script as Admin in order for the system
            maintenance to run correctly.
        """
        self.sagerun_code = code
        self._log_change("sagerun_code", code)

    def enable_system_maintenance(
        self,
        sagerun_code: int = 11,
        enable_restart: bool = False,
        cleanup_folders: List[str] = [],
        system_maintenance_day: int = 31,
    ) -> None:
        """
        Enable scriptman's system maintenance scripts to run. These include
        sfc, dism, disk cleanup and defragmentation.

        Note:
            The main script needs to be run as Admin in order for the scripts
            to work effectively.
        """
        self.system_maintenance = True
        self.update_sagerun_code(sagerun_code)
        self.update_system_maintenance_day(system_maintenance_day)

        if cleanup_folders:
            self.add_folders_for_cleanup(cleanup_folders)

        if enable_restart:
            self.enable_system_restart()

        self._log_change("system_maintenance", True)

    def disable_system_maintenance(self) -> None:
        """
        Disable scriptman's system maintenance scripts to run. These include
        sfc, dism, disk cleanup and defragmentation.
        """
        self.system_maintenance = False
        self._log_change("system_maintenance", False)

    def enable_system_restart(self) -> None:
        """
        Enable system restart after the maintenance scripts are completed.
        """
        self.restart_system_after_maintenance = True
        self._log_change("restart_system_after_maintenance", True)

    def disable_system_restart(self) -> None:
        """
        Disable system restart after the maintenance scripts are completed.
        """
        self.restart_system_after_maintenance = False
        self._log_change("restart_system_after_maintenance", False)

    def update_system_maintenance_day(self, day: int) -> None:
        """
        Update the monthly day to run the system maintenance scripts.

        Args:
            day (int): The day to run the scripts. Must be between 1 and 31.

        Note:
            Dates 30 and 31 will automatically pick the last date of the
            month in February (28th or 29th in leap years).
            Date 31 will automatically pick the last date of the month for
            months with 30 days.
        """
        if day >= 1 and day <= 31:
            self.system_maintenance_day = day
            self._log_change("System Maintenance Date", day)
        else:
            raise ValueError(f"({day}) is not within the correct range!")

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
