"""
ScriptMan - A Python Package for Script Management

ScriptMan is a package that provides tools and utilities for managing Python
scripts.

Usage:
import scriptman

# Example: Run a script
scriptman.ScriptsHandler().run_script('my_script.py')

Exposed Classes and Modules:
- CleanUpHandler: Provides cleanup functionalities for scripts.
- CLIHandler: Handles command-line interface interactions.
- CSVHandler: Offers utilities for working with CSV files.
- DatabaseHandler: Provides database interaction capabilities.
- DirectoryHandler: Manages directories and file operations.
- ETLHandler: Offers tools for Extract, Transform, Load (ETL) processes.
- LogHandler: Handles logging with different log levels.
- LogLevel: Enum for different log levels.
- ScriptsHandler: Manages the execution of scripts.
- SeleniumHandler: Provides tools for web automation using Selenium.
- SeleniumInteraction: A class for Selenium-based interactions.
- Settings: Accesses and manages package settings.

For detailed documentation and examples, please refer to the package
documentation.
"""

import atexit

from scriptman.cleanup import CleanUpHandler
from scriptman.cli import CLIHandler
from scriptman.csv_handler import CSVHandler
from scriptman.database import DatabaseHandler
from scriptman.directories import DirectoryHandler
from scriptman.etl import ETLHandler
from scriptman.interactions import Interaction as SeleniumInteraction
from scriptman.logs import LogHandler, LogLevel
from scriptman.scripts import ScriptsHandler
from scriptman.selenium_handler import SeleniumHandler
from scriptman.settings import Settings

atexit.register(CleanUpHandler)

__all__ = [
    "CleanUpHandler",
    "CLIHandler",
    "CSVHandler",
    "DatabaseHandler",
    "DirectoryHandler",
    "ETLHandler",
    "LogHandler",
    "LogLevel",
    "ScriptsHandler",
    "SeleniumHandler",
    "SeleniumInteraction",
    "Settings",
]
