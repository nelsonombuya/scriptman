"""
ScriptMan - DirectoryHandler

This module provides the DirectoryHandler class, responsible for creating and
managing directories for a ScriptManager setup.

Usage:
- Import the DirectoryHandler class from this module.
- Initialize a DirectoryHandler instance to manage directories for your
  ScriptManager setup.

Example:
```python
from scriptman._directory import DirectoryHandler

directory_handler = DirectoryHandler()
# Default directories are created upon initialization.
# (Ensure app_dir is set in the package settings)
```

Classes:
- `DirectoryHandler`: Manages directory creation and management for the
  ScriptManager application.

Attributes:
- `ROOT_DIR (str)`: The key for the root directory in the directories
  dictionary.
- `LOGS_DIR (str)`: The key for the logs directory in the directories
  dictionary.
- `SCRIPTS_DIR (str)`: The key for the scripts directory in the directories
  dictionary.
- `HELPERS_DIR (str)`: The key for the helpers directory in the directories
  dictionary.
- `SELENIUM_DIR (str)`: The key for the selenium custom driver directory in
  the directories dictionary.
- `HANDLERS_DIR (str)`: The key for the handlers directory in the directories
  dictionary.
- `DOWNLOADS_DIR (str)`: The key for the downloads directory in the directories
  dictionary.
- `SCRIPT_MAN_DIR (str)`: The key for the ScriptManager directory in the
  directories dictionary.
- `DEFAULT_DIRECTORIES (List[str])`: A list of default directory names to be
  created.

Methods:
- `__init__()`: Initializes a DirectoryHandler instance and creates the default
  directories.
- `create_and_set_directories()`: Creates and sets the default directories.
- `create_directory(directory_name: str) -> str`: Creates a directory if it
  does not exist and returns its path.
- `create_selenium_directory() -> str`: Creates the 'selenium' directory if it
  does not exist within the 'downloads' directory and returns its path.
- `__str__() -> str`: Returns a JSON representation of the directories
  dictionary.
"""

import json
import os
from typing import Dict

from scriptman._settings import Settings


class DirectoryHandler:
    """
    DirectoryHandler handles the creation and management of directories for a
    ScriptManager setup.

    Attributes:
        ROOT_DIR (str): The key for the root directory in the directories
            dictionary.
        LOGS_DIR (str): The key for the logs directory in the directories
            dictionary.
        SCRIPTS_DIR (str): The key for the scripts directory in the directories
            dictionary.
        HELPERS_DIR (str): The key for the helpers directory in the directories
            dictionary.
        SELENIUM_DIR (str): The key for the selenium custom driver directory in
            the directories dictionary.
        HANDLERS_DIR (str): The key for the handlers directory in the
            directories dictionary.
        DOWNLOADS_DIR (str): The key for the downloads directory in the
            directories dictionary.
        SCRIPT_MAN_DIR (str): The key for the ScriptManager directory in the
            directories dictionary.
        DEFAULT_DIRECTORIES (List[str]): A list of default directory names to
            be created.

    Methods:
        __init__(): Initializes a DirectoryHandler instance and creates the
            default directories.
        create_and_set_directories(): Creates and sets the default directories.
        create_directory(directory_name: str) -> str: Creates a directory if it
            does not exist and returns its path.
        create_selenium_directory() -> str: Creates the 'selenium' directory if
            it does not exist within the 'downloads' directory and returns its
            path.
        __str__() -> str: Returns a JSON representation of the directories
            dictionary.
    """

    ROOT_DIR = "root"
    LOGS_DIR = "logs"
    SCRIPTS_DIR = "scripts"
    HELPERS_DIR = "helpers"
    SELENIUM_DIR = "selenium"
    HANDLERS_DIR = "handlers"
    DOWNLOADS_DIR = "downloads"
    SCRIPT_MAN_DIR = "scriptman"
    DEFAULT_DIRECTORIES = [DOWNLOADS_DIR, LOGS_DIR, SCRIPTS_DIR, HELPERS_DIR]

    def __init__(self) -> None:
        """
        Initializes a DirectoryHandler instance and creates the default
        directories.
        """
        self.directories: Dict[str, str] = {}
        self.create_and_set_directories()

    @property
    def root_dir(self) -> str:
        """
        Get the root directory for the setup, based on the location of this
        script.

        Returns:
            str: The path to the root directory.

        Raises:
            RuntimeError: If APP DIR has not been set in the settings.
        """
        if Settings.app_dir:
            return os.path.abspath(os.path.join(Settings.app_dir, "app"))
        elif Settings.debug_mode:
            return os.path.join(os.path.dirname(__file__), "..", "app")
        raise RuntimeError("APP DIR has not been set! Run settings.init()")

    @property
    def logs_dir(self) -> str:
        """
        Get the logs directory for the ScriptManager Package
        Files.

        Returns:
            str: The path to the ScriptManager logs directory.
        """
        return self.directories.get(
            self.LOGS_DIR,
            self.create_directory(self.LOGS_DIR),
        )

    @property
    def scripts_dir(self) -> str:
        """
        Get the scripts directory for the ScriptManager Package Files.

        Returns:
            str: The path to the ScriptManager scripts directory.
        """
        return self.directories.get(
            self.SCRIPTS_DIR,
            self.create_directory(self.SCRIPTS_DIR),
        )

    @property
    def helpers_dir(self) -> str:
        """
        Get the helper files directory for the ScriptManager Package Files.

        Returns:
            str: The path to the ScriptManager helper files directory.
        """
        return self.directories.get(
            self.HELPERS_DIR,
            self.create_directory(self.HELPERS_DIR),
        )

    @property
    def selenium_dir(self) -> str:
        """
        Get the selenium custom driver directory for the ScriptManager Package
        Files.

        Returns:
            str: The path to the ScriptManager selenium custom driver
                directory.
        """
        return self.directories.get(
            self.SELENIUM_DIR,
            self.create_selenium_directory(),
        )

    @property
    def handlers_dir(self) -> str:
        """
        Get the handlers directory for the ScriptManager Package Files.

        Returns:
            str: The path to the handlers directory.
        """
        return os.path.dirname(os.path.abspath(__file__))

    @property
    def downloads_dir(self) -> str:
        """
        Get the downloads directory for the ScriptManager Package Files.

        Returns:
            str: The path to the ScriptManager downloads directory.
        """
        return self.directories.get(
            self.DOWNLOADS_DIR,
            self.create_directory(self.DOWNLOADS_DIR),
        )

    @property
    def script_man_dir(self) -> str:
        """
        Get the root directory for the ScriptManager Package Files.

        Returns:
            str: The path to the ScriptManager directory.
        """
        return os.path.abspath(os.path.join(self.handlers_dir, "..", ".."))

    def create_and_set_directories(self) -> None:
        """
        Create and set the default directories.
        """
        self.directories[self.ROOT_DIR] = self.root_dir
        self.directories[self.SCRIPT_MAN_DIR] = self.script_man_dir
        for directory_name in self.DEFAULT_DIRECTORIES:
            self.create_directory(directory_name)

    def create_directory(self, directory_name: str) -> str:
        """
        Create a directory if it does not exist and return its path.

        Args:
            directory_name (str): The name of the directory to create.

        Returns:
            str: The path of the created or existing directory.
        """
        directory_path = os.path.join(self.root_dir, directory_name)
        self.directories[directory_name] = directory_path
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
            print(f"Created {directory_path}")
        return directory_path

    def create_selenium_directory(self) -> str:
        """
        Create the 'selenium' directory if it does not exist within the
        'downloads' directory and return its path.

        If the 'selenium' directory already exists, its path is returned
        without modification.

        Returns:
            str: The path of the 'selenium' directory.
        """
        selenium_dir = os.path.join(self.downloads_dir, self.SELENIUM_DIR)
        self.directories[self.SELENIUM_DIR] = selenium_dir
        os.makedirs(selenium_dir, exist_ok=True)
        return selenium_dir

    def __str__(self) -> str:
        """
        Returns the list of directories when the DirectoryManager Object is
        printed.

        Returns:
            str: JSON representation of the directories.
        """
        return json.dumps(self.directories, indent=4)
