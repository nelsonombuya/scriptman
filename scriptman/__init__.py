"""
ScriptMan - A Python Package for Script Management

ScriptMan is a comprehensive Python package that offers a wide range of tools
and utilities for efficiently managing Python scripts. Whether you're dealing
with data processing, databases, command-line interfaces, web automation, or
simply need better organization for your scripts, ScriptMan provides the
solutions you need.

Usage:
To use ScriptMan, you can import it as follows:

```python

import scriptman

```


# Example: Run a script
scriptman.ScriptsHandler().run_script('my_script.py')


Exposed Classes and Modules:
- `CleanUpHandler`: Provides cleanup functionalities for scripts.
- `CLIHandler`: Handles command-line interface interactions.
- `CSVHandler`: Offers utilities for working with CSV files.
- `DatabaseHandler`: Provides capabilities for database interactions.
- `DirectoryHandler`: Manages directories and file operations.
- `ETLHandler`: Provides tools for Extract, Transform, Load (ETL) processes.
- `LogHandler`: Handles logging with support for different log levels.
- `LogLevel`: An enum for different log levels.
- `ScriptsHandler`: Manages the execution of scripts.
- `SeleniumHandler`: Provides tools for web automation using Selenium.
- `SeleniumInteraction`: Enum for handling Selenium-based interactions.
- `Settings`: Manages and accesses package settings.

Initialization and Setup:
To initialize ScriptMan and set it up for your project, follow these steps:

1. Import the necessary modules from ScriptMan.

```python

from scriptman import Settings, ScriptHandler, CLIHandler

```

2. Initialize the `Settings` class instance to set up the ScriptMan app files
in your project directory. You can do this as follows:

```python

Settings.init(
    app_dir='your_project_directory',
    logging=True,  # Enable logging (default is True)
    debugging=False,  # Enable debugging mode (default is False)
)

```

For detailed documentation, examples, and advanced usage, please refer to the

[ScriptMan package documentation]
(https://github.com/nelsonombuya/scriptman/blob/main/docs/README.md).

"""

import atexit

from scriptman._cleanup import CleanUpHandler
from scriptman._cli import CLIHandler
from scriptman._csv import CSVHandler
from scriptman._database import DatabaseHandler
from scriptman._directories import DirectoryHandler
from scriptman._etl import ETLHandler
from scriptman._logs import LogHandler, LogLevel
from scriptman._scripts import ScriptsHandler
from scriptman._selenium import SeleniumHandler
from scriptman._selenium_interactions import SeleniumInteraction
from scriptman._settings import Settings

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
