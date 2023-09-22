from .chrome import Chrome


class SeleniumHandler:
    """
    SeleniumHandler manages the creation of Selenium WebDriver instances.

    SeleniumHandler is responsible for creating and managing instances of
    Selenium WebDriver for various browsers.

    > Chrome
    It utilizes the `Chrome` class to create WebDriver instances with various
    configurations, allowing users to automate web interactions using Selenium.

    Example:
        To create a Selenium WebDriver instance with default settings:
        >>> selenium_handler = SeleniumHandler()
        >>> driver = selenium_handler.chrome.get_driver()

        You can also use the browser instance for extra functionality:
        >>> selenium_handler = SeleniumHandler.chrome
        >>> selenium_handler.wait_for_downloads_to_finish()
        > See `SeleniumInteractionsHandler` for more details.

    Attributes:
        chrome (Chrome): An instance of the `Chrome` class, which handles
            Chrome WebDriver configurations and creation.

    See Also:
        > `Chrome`: The class responsible for configuring and creating Chrome
            WebDriver instances.
    """

    chrome = Chrome()
