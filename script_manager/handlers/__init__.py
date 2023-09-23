from typing import Callable

from cleanup import CleanUpHandler
from directories import DirectoryHandler
from logs import LogHandler
from selenium_handler import SeleniumHandler
from settings import SettingsHandler
from settings import settings as settings_handler


class HandlerManager:
    """
    A manager class for handling various functionalities and operations using
    handler classes.

    This class provides access to different handlers such as LogHandler,
    CleanUpHandler, SeleniumHandler, DirectoryHandler, and SettingsHandler.

    Attributes:
        logs (Callable): A callable reference to the LogHandler class, which
            can be used to manage logs.
        cleanup (Callable): A callable reference to the CleanUpHandler class,
            which can be used to manage cleanup tasks.
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

    logs: Callable = LogHandler
    cleanup: Callable = CleanUpHandler
    selenium: Callable = SeleniumHandler
    directories: Callable = DirectoryHandler
    settings: SettingsHandler = settings_handler
