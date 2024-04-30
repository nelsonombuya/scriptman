"""
ScriptMan - CSVHandler

This module provides the CSVHandler class, responsible for handling CSV files
and data.

Usage:
- Import the CSVHandler class from this module.
- Use the methods provided by the CSVHandler class to perform CSV file
operations.

Example:
```python
from scriptman._csv import CSVHandler

# Find a CSV file
csv_file_path = CSVHandler.find_csv_file('my_data')

# Extract data from a CSV file
data = CSVHandler.extract_csv(csv_file_path)

# Update a CSV entry
CSVHandler.update_csv_entry(0, 'Status', csv_file_path, data, value='Updated')

# Save data to a CSV file
data_to_save = [{'Name': 'John', 'Age': 30}, {'Name': 'Alice', 'Age': 25}]
CSVHandler.save_to_csv(data_to_save, 'my_data_updated')
```

Classes:
- `CSVHandler`: A class for handling CSV files and data.

For detailed documentation and examples, please refer to the package
documentation.
"""

from glob import glob
from typing import List, Optional, Union

import chardet
import pandas as pd

from scriptman._directories import DirectoryHandler


class CSVHandler:
    """
    A class for handling CSV files and data.
    """

    @staticmethod
    def find_csv_file(
        csv_file_name: str,
        csv_directory: Optional[str] = None,
    ) -> str:
        """
        Find the CSV file in the specified directory.

        Args:
            csv_file_name (str): The name of the CSV file.
            csv_directory (str, optional): Directory to search for CSV files.
                If not provided, it will be retrieved from the environment
                variable DOWNLOADS_DIRECTORY.

        Returns:
            str: The path to the CSV file.
        """
        csv_directory = csv_directory or DirectoryHandler().downloads_dir
        files = glob(f"{csv_directory}/*{csv_file_name}*.csv")
        return files[0] if files else ""

    @staticmethod
    def extract_csv(
        csv_file_path: str,
        encoding: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract data from the CSV file.

        Args:
            csv_file_path (str): The path to the CSV file.
            encoding (str, optional): Encoding of the CSV file.

        Returns:
            pd.DataFrame: The extracted data as a pandas.DataFrame.
        """
        if not encoding:
            with open(csv_file_path, "rb") as csv_file:
                encoding = chardet.detect(csv_file.read())["encoding"]

        return pd.read_csv(csv_file_path, encoding=encoding)

    @staticmethod
    def update_csv_entry(
        index,
        prop: str,
        csv_file_path: str,
        csv_data: pd.DataFrame,
        skipped: bool = False,
        value: Optional[str] = None,
    ) -> None:
        """
        Update a CSV entry.

        Args:
            index (any): Index of the CSV entry.
            prop (str): Property to update (e.g., "Status").
            csv_file_path (str): Path to the CSV file.
            csv_data (pd.DataFrame): DataFrame containing CSV data.
            skipped (bool, optional): Whether the update was skipped.
            value (str, optional): Value to update the property.

        Returns:
            None
        """
        status = "Skipped" if skipped else value or "Updated"
        csv_data.loc[index, prop] = status
        csv_data.to_csv(csv_file_path, index=False)

    @staticmethod
    def save_to_csv(
        data: Union[List[dict], pd.DataFrame],
        csv_file_name: str,
        csv_directory: Optional[str] = None,
    ) -> str:
        """
        Save data to a CSV file.

        Args:
            data (Union[List[dict], pd.DataFrame]): Data to save to the CSV.
            csv_file_name (str): Name of the CSV file.
            csv_directory (str, optional): The location to save the CSV file.
                Defaults to the environment variable DOWNLOADS_DIRECTORY.

        Returns:
            str: The path to the created CSV file.
        """
        csv_directory = csv_directory or DirectoryHandler().downloads_dir
        data = pd.DataFrame(data) if isinstance(data, list) else data
        csv_file_path = rf"{csv_directory}\{csv_file_name}.csv"
        data.to_csv(csv_file_path, index=False)
        return csv_file_path
