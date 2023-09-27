"""
ScriptMan - SeleniumHandler

This module provides the SeleniumHandler class for managing the creation of
Selenium WebDriver instances.

Usage:
- Import the SeleniumHandler class from this module.
- Create an instance of SeleniumHandler to manage the creation of Selenium
WebDriver instances for various browsers.
- Use the provided browser-specific methods to create WebDriver instances and
automate web interactions using Selenium.

Class:
- `SeleniumHandler`: Manages the creation of Selenium WebDriver instances for
various browsers.

Attributes:
- None

Methods:
- `__init__(self) -> None`: Initialize the SeleniumHandler instance.
- `chrome: Callable[[], Chrome] = Chrome`: Provides a callable property to
create a Chrome WebDriver instance using the Chrome class.
- `firefox: Callable[[], Firefox] = Firefox`: Provides a callable property to
create a Firefox WebDriver instance using the Firefox class.

Initialization:
- Create an instance of `SeleniumHandler` to manage Selenium WebDriver
instances for various browsers.

Examples:
```python
# Initialize SeleniumHandler
selenium_handler = SeleniumHandler()

# Create a Selenium WebDriver instance using the Chrome browser
driver = selenium_handler.chrome.get_driver()

# Create a Selenium WebDriver instance using the Firefox browser
driver = selenium_handler.firefox.get_driver()

# You can also use the browser instance for extra functionality
selenium_handler = SeleniumHandler.chrome()
selenium_handler = SeleniumHandler.firefox()
selenium_handler.wait_for_downloads_to_finish()
```

Properties:
- `chrome`: Provides a callable property to create a Chrome WebDriver instance
    using the Chrome class.
- `firefox`: Provides a callable property to create a Firefox WebDriver
    instance using the Chrome class.
"""

from typing import Callable, Union

from scriptman._selenium_chrome import Chrome
from scriptman._selenium_firefox import Firefox

AnyBrowser = Union[Chrome, Firefox]


class SeleniumHandler:
    def __init__(self) -> None:
        """
        SeleniumHandler manages the creation of Selenium WebDriver instances.

        SeleniumHandler is responsible for creating and managing instances of
        Selenium WebDriver for various browsers.

        > Chrome
        It utilizes the `Chrome` class to create WebDriver instances with
        various configurations, allowing users to automate web interactions
        using Selenium.

        > Firefox
        It utilizes the `Firefox` class to create WebDriver instances with
        various configurations, allowing users to automate web interactions
        using Selenium.

        Example:
            To create a Selenium WebDriver instance with default settings:
            >>> selenium_handler = SeleniumHandler()
            >>> driver = selenium_handler.chrome.get_driver()
            >>> driver = selenium_handler.firefox.get_driver()

            You can also use the browser instance for extra functionality:
            >>> selenium_handler = SeleniumHandler.chrome
            >>> selenium_handler = SeleniumHandler.firefox
            >>> selenium_handler.wait_for_downloads_to_finish()
            > See `SeleniumInteractionsHandler` for more details.
        """

    chrome: Callable[[], Chrome] = Chrome
    firefox: Callable[[], Firefox] = Firefox
