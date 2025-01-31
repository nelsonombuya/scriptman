from abc import abstractmethod
from pathlib import Path
from typing import Any, Union
from scriptman.utils.etl.extractor import DataExtractor
from scriptman.utils.selenium import SeleniumInstance

PossibleSeleniumDataTypes = Union[dict[str, Any], list[dict[str, Any]], Path, str]


class SeleniumExtractor(SeleniumInstance, DataExtractor[PossibleSeleniumDataTypes]):
    """Selenium-based data extractor that integrates with ETL"""

    @abstractmethod
    def extract(self) -> PossibleSeleniumDataTypes:
        """
        🗃 Extract data from the source. If downloading a file, returns a Path object, or
        a string containing the glob pattern; which will search for the file in the
        scriptman download folder.

        Returns:
            PossibleSeleniumDataTypes: The extracted data or file path or glob pattern.
        """
        pass
