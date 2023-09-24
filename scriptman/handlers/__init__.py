from typing import Callable, List

from .cleanup import CleanUpHandler
from .cli import CLIHandler
from .database import DatabaseHandler
from .directories import DirectoryHandler
from .etl import ETLHandler
from .logs import LogHandler
from .scripts import ScriptsHandler
from .selenium import SeleniumHandler
from .settings import SettingsHandler
from .settings import settings as settings_handler


class HandlerManager:
    """
    A manager class for handling various functionalities and operations using
    handler classes.

    This class provides access to different handlers such as LogHandler,
    CleanUpHandler, SeleniumHandler, DirectoryHandler, and SettingsHandler.

    Attributes:
        etl (Callable): A callable reference to the ETLHandler class, which can
            be used to run ETL processes for CSV, JSON, and Pandas DataFrame
            data when needed.
        settings (SettingsHandler): An instance of the SettingsHandler class,
            which can be used to access and modify settings.
        logs (Callable[str]): A callable reference to the LogHandler class,
            which can be used to manage logs. You can provide a name for the
            log instance.
        cli (Callable[List[str]]): A callable reference to the CLIHandler
            class, which can be used to run scripts from the command line
            interface.
        db (Callable[str]): A callable reference to the DatabaseHandler class,
            which can be used to run CRUD processes for databases. You can
            provide a database connection string.
        cleanup (Callable): A callable reference to the CleanUpHandler class,
            which can be used to manage cleanup tasks.
        scripts (Callable): A callable reference to the ScriptsHandler class,
            which can be used to manage and run Python scripts.
        selenium (Callable): A callable reference to the SeleniumHandler class,
            which can be used to interact with Selenium.
        directories (Callable): A callable reference to the DirectoryHandler
            class, which can be used to manage directories.

    Example:
        # Access the LogHandler
        logs = HandlerManager.logs("MyLogger")

        # Access the SettingsHandler
        settings = HandlerManager.settings
        max_retries = settings.get_setting("max_retries")

    Note:
        This class assumes that the referenced handler classes
        (LogHandler, CleanUpHandler, etc.) have been properly imported and
        exist in the current module or package.
    """

    etl: Callable[[], ETLHandler] = ETLHandler
    settings: SettingsHandler = settings_handler
    logs: Callable[[str], LogHandler] = LogHandler
    cli: Callable[[List[str]], CLIHandler] = CLIHandler
    db: Callable[[str], DatabaseHandler] = DatabaseHandler
    cleanup: Callable[[], CleanUpHandler] = CleanUpHandler
    scripts: Callable[[], ScriptsHandler] = ScriptsHandler
    selenium: Callable[[], SeleniumHandler] = SeleniumHandler
    directories: Callable[[], DirectoryHandler] = DirectoryHandler
