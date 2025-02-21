from abc import abstractmethod
from pathlib import Path
from typing import Any, Union

try:

    from pandas import DataFrame

    from scriptman.powers.etl._extractor import DataExtractor
except ImportError:
    raise ImportError(
        "ETL is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[etl]."
    )

try:
    from scriptman.powers.selenium import SeleniumInstance
except ImportError:
    raise ImportError(
        "Selenium is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[selenium]."
    )

PossibleSeleniumDataTypes = Union[
    str,
    Path,
    DataFrame,
    dict[str, Any],
    list[dict[str, Any]],
]


class SeleniumExtractor(SeleniumInstance, DataExtractor[PossibleSeleniumDataTypes]):
    """Selenium-based data extractor that integrates with ETL"""

    @abstractmethod
    def extract(self) -> PossibleSeleniumDataTypes:
        """
        🗃 Extract data from the source. If downloading a file, returns a Path object, or
        a string containing the glob pattern; which will search for the file in the
        scriptman download folder.

        Returns:
            PossibleSeleniumDataTypes: The extracted data or file path or file glob
                pattern. If file glob pattern is used, it will search for the file in
                the scriptman download folder.
        """
        pass
