from pathlib import Path
from typing import Optional

from selenium.webdriver import Edge as EdgeDriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver import EdgeOptions
from webdriver_manager.microsoft import EdgeChromiumDriverManager

from scriptman.core.config import config
from scriptman.utils.selenium._enums import SeleniumBrowser


class Edge(SeleniumBrowser[EdgeDriver]):

    def _get_driver(self) -> EdgeDriver:
        """
        🏎 Get an Edge WebDriver instance with specified options.

        Returns:
            webdriver.Edge: An Edge WebDriver instance.
        """
        options = self._get_edge_options()
        service = Service(EdgeChromiumDriverManager().install())
        return EdgeDriver(options=options, service=service)

    def _get_edge_options(
        self, edge_executable_path: Optional[Path] = None
    ) -> EdgeOptions:
        """
        ⚙ Get Edge WebDriver options with specified configurations.

        Args:
            edge_executable_path (str, optional): Path to the Edge binary
                executable.

        Returns:
            webdriver.EdgeOptions: Edge WebDriver options.
        """
        options = EdgeOptions()

        if edge_executable_path:
            options.binary_location = edge_executable_path.resolve().as_posix()

        if config.env.get("selenium_optimizations"):
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
                "download.default_directory": config.env.download_directory,
            },
        )

        return options
