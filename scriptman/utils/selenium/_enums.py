from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Union

from selenium.webdriver import Chrome as ChromeDriver
from selenium.webdriver import Edge as EdgeDriver
from selenium.webdriver import Firefox as FirefoxDriver

from scriptman.utils.generics import T
from scriptman.utils.selenium.browsers.edge import Edge
from scriptman.utils.selenium.browsers.firefox import Firefox
from scriptman.utils.selenium.browsers.chrome import Chrome

Driver = Union[ChromeDriver, EdgeDriver, FirefoxDriver]


class SeleniumBrowser(ABC, Generic[T]):
    @property
    @abstractmethod
    def driver(self) -> T:
        pass


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


BrowserMap: dict[Browsers, type[SeleniumBrowser[Driver]]] = {
    Browsers.EDGE: Edge,
    Browsers.CHROME: Chrome,
    Browsers.FIREFOX: Firefox,
    Browsers.CHROME_LOCAL: Chrome,
}
