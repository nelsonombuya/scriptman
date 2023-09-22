import os
from typing import Dict

from .settings import settings


class DirectoryHandler:
    """
    DirectoryHandler handles the creation and management of directories for a
    ScriptManager setup.
    """

    def __init__(self) -> None:
        self.directories: Dict[str, str] = {}
        self.create_and_set_directories()

    def create_and_set_directories(self) -> None:
        """
        Create and set the default directories.
        """
        self.directories["root"] = self.get_root_dir()
        self.directories["scriptman"] = self.get_script_man_dir()
        directories_to_create = ["downloads", "logs", "scripts", "helpers"]
        for directory_name in directories_to_create:
            self.create_directory(directory_name)

    def get_root_dir(self) -> str:
        """
        Get the root directory for the setup, based on the location of this
        script.
        """
        if settings.app_dir:
            root_dir = os.path.abspath(os.path.join(settings.app_dir, "app"))
        elif "root" in self.directories:
            root_dir = self.directories["root"]
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            root_dir = os.path.abspath(os.path.join(current_dir, "..", "app"))
        return root_dir

    def get_script_man_dir(self) -> str:
        """
        Get the root directory for the ScriptManager Package Files.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.abspath(os.path.join(current_dir, ".."))

    def create_directory(self, directory_name: str) -> str:
        """
        Create a directory if it does not exist and return its path.
        Args:
            directory_name (str): The name of the directory to create.

        Returns:
            str: The path of the created or existing directory.
        """
        directory_path = self.directories.get(directory_name)

        if directory_path and os.path.isdir(directory_path):
            return directory_path

        root_dir = self.directories["root"]
        if not os.path.isdir(root_dir):
            os.makedirs(root_dir, exist_ok=True)

        directory_path = os.path.join(root_dir, directory_name)
        self.directories[directory_name] = directory_path

        if not os.path.isdir(directory_path):
            os.makedirs(directory_path, exist_ok=True)

        return directory_path

    def create_selenium_directory(self) -> str:
        """
        Create the 'selenium' directory if it does not exist within the
        'downloads' directory, and return its path.

        If the 'selenium' directory already exists, its path is returned
        without modification.

        Returns:
            str: The path of the 'selenium' directory.
        """
        selenium_dir = self.directories.get("selenium", None)
        if selenium_dir and os.path.isdir(selenium_dir):
            return selenium_dir

        downloads_dir = self.directories.get("downloads")
        if not downloads_dir or not os.path.isdir(downloads_dir):
            downloads_dir = self.create_directory("downloads")

        selenium_dir = os.path.join(downloads_dir, "selenium")
        self.directories["selenium"] = selenium_dir

        if not os.path.isdir(selenium_dir):
            os.makedirs(selenium_dir, exist_ok=True)

        return selenium_dir

    def __str__(self) -> str:
        """
        Returns the list of directories when the Dir Object is printed.
        """
        return str(self.directories)
