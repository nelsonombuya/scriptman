from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Union

from loguru import logger


from scriptman.powers.generics import T
from scriptman.powers.selenium._edge import Edge, EdgeDriver
from scriptman.powers.selenium._firefox import Firefox, FirefoxDriver
from scriptman.powers.selenium._chrome import Chrome, ChromeDriver

Driver = Union[ChromeDriver, EdgeDriver, FirefoxDriver]


class SeleniumBrowser(ABC, Generic[T]):
    _driver: T

    def __init__(self) -> None:
        """
        🚀 Initialize the SeleniumBrowser instance and set the WebDriver instance.

        This method calls the abstract method `_get_driver` to initialize the WebDriver
        instance and assigns it to the `_driver` attribute.
        """
        self.log = logger.bind(handler=self.__class__.__qualname__)
        self._driver = self._get_driver()

    @abstractmethod
    def _get_driver(self) -> T:
        """
        🏎 Get the WebDriver instance associated with the current browser.

        Returns:
            T: The WebDriver instance.
        """
        pass

    @property
    def driver(self) -> T:
        """
        🏎 Get the WebDriver instance associated with the current Selenium browser.

        Returns:
            T: The WebDriver instance (Chrome, Edge, or Firefox) used by the browser.
        """
        return self._driver


class Browsers(Enum):
    """
    🌐 Browser Enums

    This enum contains the different browsers that Scriptman supports.

    Attributes:
        EDGE (str): Microsoft Edge
        CHROME (str): Google Chrome
        FIREFOX (str): Mozilla Firefox
        CHROME_LOCAL (str): Google Chrome (Downloaded and Locally Installed Instance)
    """

    EDGE = "Microsoft Edge"
    CHROME = "Google Chrome"
    FIREFOX = "Mozilla Firefox"
    CHROME_LOCAL = "Google Chrome (Local)"


BrowserMap: dict[Browsers, type[SeleniumBrowser]] = {
    Browsers.EDGE: Edge,
    Browsers.CHROME: Chrome,
    Browsers.FIREFOX: Firefox,
    Browsers.CHROME_LOCAL: Chrome,
}
