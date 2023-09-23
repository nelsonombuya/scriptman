from typing import Callable

from script_manager.handlers.cleanup import CleanUpHandler
from script_manager.handlers.cli import CLIHandler
from script_manager.handlers.directories import DirectoryHandler
from script_manager.handlers.etl import ETLHandler
from script_manager.handlers.logs import LogHandler
from script_manager.handlers.scripts import ScriptsHandler
from script_manager.handlers.selenium import SeleniumHandler
from script_manager.handlers.settings import SettingsHandler
from script_manager.handlers.settings import settings as settings_handler


class HandlerManager:
    """
    A manager class for handling various functionalities and operations using
    handler classes.

    This class provides access to different handlers such as LogHandler,
    CleanUpHandler, SeleniumHandler, DirectoryHandler, and SettingsHandler.

    Attributes:
        cli (Callable): A callable reference to the CLIHandler class, which
            can be used to run scripts from the CLI.
        etl (Callable): A callable reference to the ETLHandler class, which
            can be used to run etl processes for CSV, JSON and Pandas DataFrame
            data when needed.
        logs (Callable): A callable reference to the LogHandler class, which
            can be used to manage logs.
        cleanup (Callable): A callable reference to the CleanUpHandler class,
            which can be used to manage cleanup tasks.
        scripts (Callable): A callable reference to the ScriptHandler class,
            which can be used to manage and run python scripts.
        selenium (Callable): A callable reference to the SeleniumHandler class,
            which can be used to interact with Selenium.
        directories (Callable): A callable reference to the DirectoryHandler
            class, which can be used to manage directories.
        settings (SettingsHandler): An instance of the SettingsHandler class,
            which can be used to access and modify settings.

    Example:
        # Access the LogHandler
        log_handler = HandlerManager.logs()

        # Access the SettingsHandler
        settings_handler = HandlerManager.settings
        max_retries = settings_handler.get_setting("max_retries")

    Note:
        This class assumes that the referenced handler classes
        (LogHandler, CleanUpHandler, etc.) have been properly imported and
        exist in the current module or package.
    """

    cli: Callable = CLIHandler
    etl: Callable = ETLHandler
    logs: Callable = LogHandler
    cleanup: Callable = CleanUpHandler
    scripts: Callable = ScriptsHandler
    selenium: Callable = SeleniumHandler
    directories: Callable = DirectoryHandler
    settings: SettingsHandler = settings_handler
