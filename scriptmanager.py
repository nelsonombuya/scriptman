import atexit

from handlers.handler_manager import HandlerManager


class ScriptManager:
    """
    ScriptManager is responsible for managing the application's scripts,
    handlers, settings, directories, and performing cleanup tasks on exit.

    Attributes:
        handlers (HandlerManager): A manager for various handlers such as logs,
            Selenium, directories, and settings.
        cleanup (callable): A function to  perform cleanup tasks on exit.
        settings (SettingsHandler): An instance of the SettingsHandler class
            for managing application settings.
        directories (DirectoryHandler): An instance of the DirectoryHandler
            class for managing directories.

    Usage:
        The ScriptManager class provides access to various application handlers
        and settings. It also ensures that cleanup tasks are executed when the
        script exits.
    """

    # Initialization
    handlers = HandlerManager()

    # Exposed Properties and Methods
    cleanup = handlers.cleanup
    settings = handlers.settings
    directories = handlers.directories()

    # On Exit
    atexit.register(cleanup)
