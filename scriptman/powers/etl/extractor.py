from abc import ABC, abstractmethod
from typing import Generic

from scriptman.powers.generics import T


class DataExtractor(ABC, Generic[T]):
    """Base class for all data extraction methods used by ETL."""

    @abstractmethod
    def extract(self) -> T:
        """Extract data from the source"""
        pass
