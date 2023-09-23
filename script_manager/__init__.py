import atexit
from typing import Callable

from script_manager.handlers.settings import SettingsHandler


class ScriptManager:
    """
    ScriptManager is responsible for managing the application's scripts,
    handlers, settings, directories, and performing cleanup tasks on exit.

    Attributes:
        handlers (HandlerManager): A manager for various handlers such as logs,
            Selenium, directories, and settings.
        settings (SettingsHandler): An instance of the SettingsHandler class
            for managing application settings.
        scripts (ScriptHandler): An instance of the ScriptHandler class,
            for running and managing scripts.
        directories (DirectoryHandler): An instance of the DirectoryHandler
            class for managing directories.

    Usage:
        The ScriptManager class provides access to various application handlers
        and settings. It also ensures that cleanup tasks are executed when the
        script exits.
    """

    # Initialization
    from script_manager.handlers import HandlerManager

    handlers = HandlerManager()

    # Exposed Properties and Methods
    Scripts: Callable = handlers.scripts
    Directories: Callable = handlers.directories
    settings: SettingsHandler = handlers.settings

    # On Exit
    atexit.register(handlers.cleanup)
