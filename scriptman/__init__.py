import atexit
from typing import Callable, List

from handlers.cli import CLIHandler
from handlers.directories import DirectoryHandler
from handlers.interactions import Interaction
from handlers.logs import LogLevel as LogLevelEnum
from handlers.scripts import ScriptsHandler
from handlers.settings import SettingsHandler
from handlers.settings import settings as settings_handler

# Exposing Enums
LogLevel = LogLevelEnum
SeleniumInteraction = Interaction


class ScriptMan:
    """
    ScriptManager aka ScriptMan is responsible for managing the application's
    scripts, handlers, settings, directories, and performing cleanup tasks on
    exit.

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
    from handlers import HandlerManager

    handlers = HandlerManager()

    # Exposed Properties and Methods
    settings: SettingsHandler = settings_handler
    cli: Callable[[List[str]], CLIHandler] = handlers.cli
    scripts: Callable[[], ScriptsHandler] = handlers.scripts
    directories: Callable[[], DirectoryHandler] = handlers.directories

    # On Exit
    atexit.register(handlers.cleanup)


ScriptMan.settings.enable_debugging()
