"""
ScriptMan - Edge Selenium WebDriver Management

This module provides the `Edge` class for managing Microsoft Edge Selenium
WebDriver instances.

Usage:
- Import the `Edge` class from this module.
- Initialize an `Edge` instance using `Edge()`.
- Use the initialized `Edge` instance to interact with Edge WebDriver.

Example:
```python
from scriptman._selenium_edge import Edge

edge = Edge()
# Your Edge WebDriver instance is ready to use.
```

Classes:
- `Edge`: Manages the creation and configuration of Microsoft Edge Selenium
    WebDriver instances.

Attributes:
- `driver (webdriver.Edge)`: The Edge WebDriver instance.
- `_downloads_directory (str)`: The directory path for downloads.

Methods (Edge):
- `__init__(self) -> None`: Initializes an Edge instance and sets the
    downloads directory.
- `_get_driver(self) -> webdriver.Edge`: Gets an Edge WebDriver instance
    with specified options.
- `_get_edge_options(
        self,
        edge_executable_path: Optional[str] = None
    ) -> webdriver.EdgeOptions`: Gets Edge WebDriver options with
    specified configurations.

For detailed documentation and examples, please refer to the package
documentation.
"""

from typing import Optional

from selenium import webdriver
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager

from scriptman._directories import DirectoryHandler
from scriptman._selenium_interactions import SeleniumInteractionHandler
from scriptman._settings import Settings


class Edge(SeleniumInteractionHandler):
    """
    Edge manages the creation of Microsoft Edge Selenium WebDriver instances.

    Attributes:
        driver (webdriver.Edge): The Edge WebDriver instance.
        _downloads_directory (str): The directory path for downloads.
    """

    def __init__(self) -> None:
        """
        Initialize Edge instance and set the downloads directory.
        """
        self._downloads_directory = DirectoryHandler().downloads_dir
        self.driver = self._get_driver()
        super().__init__(self.driver)

    def _get_driver(self) -> webdriver.Edge:
        """
        Get an Edge WebDriver instance with specified options.

        Returns:
            webdriver.Edge: An Edge WebDriver instance.
        """
        options = self._get_edge_options()
        service = Service(EdgeChromiumDriverManager().install())
        return webdriver.Edge(options=options, service=service)

    def _get_edge_options(
        self,
        edge_executable_path: Optional[str] = None,
    ) -> webdriver.EdgeOptions:
        """
        Get Edge WebDriver options with specified configurations.

        Args:
            edge_executable_path (str, optional): Path to the Edge binary
                executable.

        Returns:
            webdriver.EdgeOptions: Edge WebDriver options.
        """
        options = webdriver.EdgeOptions()

        if edge_executable_path:
            options.binary_location = edge_executable_path

        if Settings.selenium_optimizations and not Settings.debug_mode:
            optimization_args = [
                "--headless",
                "--no-sandbox",
                "--mute-audio",
                "--disable-gpu",
                "--disable-infobars",
                "--disable-extensions",
                "--disable-dev-shm-usage",
                "--disable-notifications",
                "--disable-setuid-sandbox",
                "--remote-debugging-port=9222",
                "--disable-browser-side-navigation",
                "--disable-blink-features=AutomationControlled",
            ]
            [options.add_argument(arg) for arg in optimization_args]

        options.add_experimental_option(
            "prefs",
            {
                "download.directory_upgrade": True,
                "download.safebrowsing.enabled": True,
                "download.prompt_for_download": False,
                "download.default_directory": self._downloads_directory,
            },
        )

        return options
