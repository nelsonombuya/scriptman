import atexit
from typing import Callable, List

from script_manager.handlers.cli import CLIHandler
from script_manager.handlers.directories import DirectoryHandler
from script_manager.handlers.interactions import Interaction
from script_manager.handlers.logs import LogLevel as LogLevelEnum
from script_manager.handlers.scripts import ScriptsHandler
from script_manager.handlers.settings import SettingsHandler

# Exposing Enums
LogLevel = LogLevelEnum
SeleniumInteraction = Interaction


class ScriptManager:
    """
    ScriptManager is responsible for managing the application's scripts,
    handlers, settings, directories, and performing cleanup tasks on exit.

    Attributes:
        handlers (HandlerManager): A manager for various handlers such as logs,
            Selenium, directories, and settings.
        cli (CLIHandler): An instance of the CLIHandler class, for running and
            managing scripts from the terminal.
        scripts (ScriptHandler): An instance of the ScriptHandler class,
            for running and managing scripts.
        directories (DirectoryHandler): An instance of the DirectoryHandler
            class for managing directories.
        settings (SettingsHandler): An instance of the SettingsHandler class
            for managing application settings.

    Usage:
        The ScriptManager class provides access to various application handlers
        and settings. It also ensures that cleanup tasks are executed when the
        script exits.
    """

    # Initialization
    from script_manager.handlers import HandlerManager

    handlers = HandlerManager()

    # Exposed Properties and Methods
    settings: SettingsHandler = handlers.settings
    cli: Callable[[List[str]], CLIHandler] = handlers.cli
    scripts: Callable[[], ScriptsHandler] = handlers.scripts
    directories: Callable[[], DirectoryHandler] = handlers.directories

    # On Exit
    atexit.register(handlers.cleanup)
