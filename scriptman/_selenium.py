"""
ScriptMan - SeleniumHandler

This module provides the `SeleniumHandler` class for managing the creation of
Selenium WebDriver instances for various web browsers.

Usage:
- Import the `SeleniumHandler` class from this module.
- Create an instance of `SeleniumHandler` to manage the creation of Selenium
    WebDriver instances for different browsers.
- Utilize the provided browser-specific methods to create WebDriver instances
    and automate web interactions using Selenium.

Classes:
- `SeleniumHandler`: Manages the creation of Selenium WebDriver instances for
    various browsers.

Attributes:
- None

Methods:
- `__init__(self) -> None`: Initializes the `SeleniumHandler` instance.
- `chrome: Callable[[], Chrome]`: Provides a callable property to create a
    Chrome WebDriver instance using the `Chrome` class.
- `edge: Callable[[], Edge]`: Provides a callable property to create an Edge
    WebDriver instance using the `Edge` class.
- `firefox: Callable[[], Firefox]`: Provides a callable property to create a
    Firefox WebDriver instance using the `Firefox` class.
- `any: Callable[[], AnyBrowser]`: Provides a callable property to create a
    WebDriver instance using any of the available Browser Classes as defined in
    Selenium Browser Index (SBI).

Initialization:
- Create an instance of `SeleniumHandler` to manage Selenium WebDriver
    instances for various browsers.

Examples:
```python
# Initialize SeleniumHandler
selenium_handler = SeleniumHandler()

# Create a Selenium WebDriver instance using the Chrome browser
driver = selenium_handler.chrome()

# Create a Selenium WebDriver instance using the Edge browser
driver = selenium_handler.edge()

# Create a Selenium WebDriver instance using the Firefox browser
driver = selenium_handler.firefox()

# Create a Selenium WebDriver instance using the any browser
driver = selenium_handler.any()
```

Properties:
- `chrome`: Provides a callable property to create a Chrome WebDriver instance
    using the `Chrome` class.
- `edge`: Provides a callable property to create an Edge WebDriver instance
    using the `Edge` class.
- `firefox`: Provides a callable property to create a Firefox WebDriver
    instance using the `Firefox` class.
- `any`: Provides a callable property to create a WebDriver instance using any
    of the available browser drivers as defined in Selenium Browser Index (SBI)
"""

from enum import Enum
from typing import Union

from scriptman._logs import LogHandler
from scriptman._selenium_chrome import Chrome
from scriptman._selenium_edge import Edge
from scriptman._selenium_firefox import Firefox
from scriptman._settings import SBI


class Browsers(Enum):
    EDGE = "Microsoft Edge"
    CHROME = "Google Chrome"
    FIREFOX = "Mozilla Firefox"


AnyBrowser = Union[Chrome, Edge, Firefox]
BROWSER_QUEUE = [Browsers.EDGE, Browsers.FIREFOX, Browsers.CHROME]


class InvalidBrowserSelectionError(Exception):
    """
    Custom exception raised when an invalid Selenium browser selection is made.
    """

    def __init__(self, selected_browser):
        """
        Initialize the exception with the selected browser name.
        """
        super().__init__("Exceeded Browser Index in selection.")


class SeleniumHandler:
    def __init__(self) -> None:
        """
        Initialize a `SeleniumHandler` instance.

        `SeleniumHandler` manages the creation of Selenium WebDriver instances
        for various web browsers, including Chrome, Edge, and Firefox.

        Example:
            To create a Selenium WebDriver instance with default settings:
            >>> selenium_handler = SeleniumHandler()
            >>> driver = selenium_handler.chrome()
            >>> driver = selenium_handler.edge()
            >>> driver = selenium_handler.firefox()
            >>> driver = selenium_handler.any()

            You can also use the browser instance for extra functionality:
            >>> selenium_handler = SeleniumHandler.chrome()
            >>> selenium_handler = SeleniumHandler.edge()
            >>> selenium_handler = SeleniumHandler.firefox()
            >>> selenium_handler = SeleniumHandler.any()
            >>> selenium_handler.wait_for_downloads_to_finish()
            > See `SeleniumInteractionsHandler` for more details.
        """

    # FIXME: Uses browser queue when user has selected only one browser
    # FIXME: Might have conflicts in case of simultaneous runs
    # edge: Callable[[], Edge] = Edge
    # chrome: Callable[[], Chrome] = Chrome
    # firefox: Callable[[], Firefox] = Firefox

    @staticmethod
    def any() -> AnyBrowser:
        try:
            browser_info = {
                Browsers.EDGE: (Browsers.EDGE, Edge),
                Browsers.CHROME: (Browsers.CHROME, Chrome),
                Browsers.FIREFOX: (Browsers.FIREFOX, Firefox),
            }.get(BROWSER_QUEUE[SBI.get_index()], BROWSER_QUEUE[0])

            log_message = f"Currently using {browser_info[0].value}"
            LogHandler("Selenium Handler").message(log_message)
            return browser_info[1]()
        except IndexError:
            raise InvalidBrowserSelectionError("Exceeded Browser Queue Index")
