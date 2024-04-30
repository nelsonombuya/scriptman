"""
ScriptMan - ETLHandler

This module provides the ETLHandler class, responsible for performing data
extraction, transformation, and loading tasks.

Usage:
- Import the ETLHandler class from this module.
- Initialize an ETLHandler instance to perform ETL operations.

Example:
```python
from scriptman._etl import ETLHandler

etl_handler = ETLHandler()
# Use the ETLHandler instance for data extraction, transformation, and loading.
```

Classes:
- `ETLHandler`: Handles data extraction, transformation, and loading.

Attributes:
- None

Methods:
- `__init__()`: Initializes an ETLHandler instance.
- `from_df(df: pd.DataFrame) -> pd.DataFrame`: Extracts data from a DataFrame
    source.
- `from_json(
        extraction_function: Callable[[], List[Dict[str, Any]]],
        extract_data_from_sublists: bool = True,
        keys: List[str] = []
    ) -> pd.DataFrame`: Extracts data from a JSON source.
- `from_csv(
        filename: str,
        directory: Optional[str] = None
    ) -> pd.DataFrame`: Extracts data from a CSV file.
- `from_db(
        db_connection_string: str,
        query: str,
        params: tuple = ()
    ) -> pd.DataFrame`: Extracts data from a database.
- `to_df() -> pd.DataFrame`: Returns extracted data as a Pandas DataFrame.
- `to_csv(
        filename: str,
        directory: Optional[str] = None
    ) -> str`: Saves the loaded data to a CSV file.
- `to_db(
        table_name: str,
        db_connection_string: str,
        truncate: bool = False,
        recreate: bool = False,
        force_nvarchar: bool = False,
        keys: List[str] = []
    ) -> None`: Loads the data into a database table.
"""

import json
import math
from typing import Any, Callable, Dict, List, MutableMapping, Optional, Union

import pandas as pd
from tqdm import tqdm

from scriptman._csv import CSVHandler
from scriptman._database import DatabaseHandler
from scriptman._logs import LogHandler, LogLevel


class ETLHandler:
    """
    ETLHandler class for performing data extraction, transformation, and
    loading.

    This class provides methods for extracting data from various sources,
    transforming it, and loading it into a database.
    """

    def __init__(self) -> None:
        """
        Initialize an ETLHandler instance.
        """
        self._data: pd.DataFrame = pd.DataFrame()
        self._log: LogHandler = LogHandler("ETL Handler")
        self._nested_data: Dict[str, List[Dict[str, Any]]] = {}

    def from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract data from a DataFrame source.

        Args:
            df (pd.DataFrame): The DataFrame to extract.

        Returns:
            pd.DataFrame: The extracted DataFrame.
        """
        self._log.message("Data extraction started...")
        self._data = df
        self._log.message("Data extraction complete.")
        self._log_number_of_records()
        return self._data

    def from_json(
        self,
        extraction_function: Callable[[], List[Dict[str, Any]]],
        extract_data_from_sublists: bool = True,
        keys: List[str] = [],
    ) -> pd.DataFrame:
        """
        Extract data from a JSON source.

        Args:
            extraction_function (Callable[[], List[Dict[str, Any]]]): A
                function to extract JSON data.
            extract_data_from_sublists (bool, optional): Whether to extract
                data from nested dictionaries. Defaults to True.
            keys (List[str]): A list of key columns in the main dataset to use
                as a reference key when extracting any sublist.

        Returns:
            pd.DataFrame: The extracted and transformed data as a DataFrame.
        """
        self._log.message("Data extraction started...")
        data = extraction_function()
        self._log.message("Data extraction complete.")

        self._log.message("Data transformation started...")
        self._log.message("Flattening dictionaries...")
        data = [self._flatten_dict(item) for item in data]

        if data and extract_data_from_sublists:
            self._log.message("Extracting Tables from Nested Dictionaries...")
            data = self._extract_nested_data(data, keys)

        self._log.message("Data transformation complete.")
        self._data = pd.DataFrame(data)
        self._log_number_of_records()
        return self._data

    def from_csv(
        self,
        filename: str,
        directory: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract data from a CSV file.

        Args:
            filename (str): The name of the CSV file.
            directory (optional, str): The directory where the CSV file is
                located. Defaults to the downloads directory.

        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        self._log.message("Data extraction started...")
        csv_handler = CSVHandler()
        csv_file = csv_handler.find_csv_file(filename, directory)
        self._data = csv_handler.extract_csv(csv_file)
        self._log.message("Data extraction complete.")
        self._log_number_of_records()
        return self._data

    def from_db(
        self,
        db_connection_string: str,
        query: str,
        params: tuple = (),
    ) -> pd.DataFrame:
        """
        Extract data from a database.

        Args:
            db_connection_string (str): The database connection string to use.
            query (str): The prepared query to use to extract the data.
            params (optional, tuple): The parameters to use in the prepared
                query. Defaults to ()

        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        self._log.message("Data extraction started...")
        db = DatabaseHandler(db_connection_string)
        db.connect()
        result = db.execute_read_query(query, params)
        self._data = pd.DataFrame(result)
        self._log.message("Data extraction complete.")
        self._log_number_of_records()
        return self._data

    def to_df(self) -> List[Dict]:
        """
        Return extracted data as a Pandas DataFrame.

        Returns:
            List[Dict]: The list of extracted data frames, including the nested
                data frames.
        """
        extracted_data: List[Dict] = [{"main": self._data}]

        if self._nested_data:
            self._log.message("Creating DataFrames from Nested Data...")
            for name, data in self._nested_data.items():
                nested_etl = ETLHandler()
                nested_etl.from_json(lambda: data, True)
                nested_dfs = nested_etl.to_df()
                extracted_data.append({name: nested_dfs})

        print("The extracted data is: ", json.dumps(extracted_data))
        return extracted_data

    def to_csv(self, filename: str, directory: Optional[str] = None) -> str:
        """
        Save the loaded data to a CSV file.

        Args:
            filename (str): The name of the CSV file to save.
            directory (Optional[str], optional): The directory where the CSV
                file will be saved. Defaults to None.

        Returns:
            str: The path to the saved CSV file.
        """
        if self._data.empty:
            self._log.message("Dataset is empty!", LogLevel.WARN)
            return ""

        if self._nested_data:
            self._log.message(f"Separating Nested Data from [{filename}]...")
            for file, data in self._nested_data.items():
                nested_etl = ETLHandler()
                nested_etl.from_json(lambda: data, True)
                nested_etl.to_csv(f"{filename}_{file}", directory)

        csv_path = CSVHandler.save_to_csv(self._data, filename, directory)
        self._log.message(f"{csv_path} has been created successfully.")
        return csv_path

    def to_db(
        self,
        table_name: str,
        db_connection_string: str,
        truncate: bool = False,
        recreate: bool = False,
        force_nvarchar: bool = False,
        keys: List[str] = [],
        bulk_execute: bool = True,
        nested_keys: Optional[List[str]] = None,
    ) -> None:
        """
        Load the data into a database table.

        Args:
            table_name (str): The name of the database table.
            db_connection_string (str): The connection string for the database.
            truncate (bool, optional): Whether to truncate the table.
                Defaults to False.
            recreate (bool, optional): Whether to recreate the table.
                Defaults to False.
            force_nvarchar (bool, optional): Whether to force NVARCHAR data
                type for all columns. Defaults to False.
            keys (List[str], optional): List of keys for updates.
                Defaults to [].
            bulk_execute (bool, optional): Whether to use bulk execute for
                queries. Defaults to True.
            nested_keys (Optional[list[str]], optional): List of keys for
                updates in nested tables. Defaults to None.
        """
        if self._data.empty:
            self._log.message("Dataset is empty!", LogLevel.WARN)
            return

        if self._nested_data:
            self._log.message(f"Separating Nested Data from [{table_name}]...")
            for tbl, data in self._nested_data.items():
                nested_etl = ETLHandler()
                nested_etl.from_json(lambda: data, True, keys)
                nested_etl.to_db(
                    truncate=truncate,
                    recreate=recreate,
                    keys=nested_keys or keys,
                    bulk_execute=bulk_execute,
                    force_nvarchar=force_nvarchar,
                    table_name=f"{table_name}_{tbl}",
                    db_connection_string=db_connection_string,
                )

        self._table = table_name
        self._force_nvarchar = force_nvarchar
        self._db = DatabaseHandler(db_connection_string)
        self._table_exists = self._db.table_exists(self._table)
        self._records_exist = self._db.table_has_records(self._table)

        if (
            not keys
            or not self._table_exists
            or not self._records_exist
            or truncate
            or recreate
        ):
            return self._insert(truncate, recreate, bulk_execute)
        else:
            return self._update(keys, bulk_execute)

    def _insert(
        self,
        truncate: bool = False,
        recreate: bool = False,
        bulk_execute: bool = True,
    ) -> None:
        """
        Insert data into a database table.

        Args:
            truncate (bool): Whether to truncate the table.
            recreate (bool): Whether to recreate the table.
            bulk_execute (bool): Whether to use bulk execute for queries.
        """
        tbl_query = self._generate_create_table_query(self._table, self._data)
        insert_query = self._generate_insert_query(self._table, self._data)
        prepared_data = self._prepare_data()

        if not self._table_exists:
            self._db.create_table(tbl_query)
        elif self._table_exists and recreate:
            self._db.drop_table(self._table)
            self._db.create_table(tbl_query)
        elif self._table_exists and truncate:
            self._db.truncate_table(self._table)

        try:
            if bulk_execute:
                self._db.execute_many(insert_query, prepared_data)
            else:
                raise MemoryError
        except MemoryError:
            self._log.message(
                "Bulk Query Execution Failed. Executing single queries...",
                LogLevel.WARN,
            )
            for row in tqdm(
                unit="record(s)",
                iterable=prepared_data,
                desc=f"Loading data onto [{self._table}]",
            ):
                self._db.execute_write_query(insert_query, row)

    def _update(self, keys: List[str], bulk_execute: bool = True) -> None:
        """
        Update data in a database table, and if the record doesn't exist,
        insert it.

        Args:
            keys (List[str]): List of keys for updates.
            bulk_execute (bool): Whether to use bulk execute for queries.
        """

        try:
            if bulk_execute:
                # Bulk Execute Update Query
                upd_query = self._generate_update_query(
                    self._table,
                    self._data,
                    keys,
                )
                prepared_data = self._prepare_data(keys)
                self._log.message(f"Updating data on [{self._table}]...")
                self._db.execute_many(upd_query, prepared_data)

                # Bulk Execute Insert Query With WHERE NOT EXISTS Clause
                ins_query = self._convert_update_query_to_insert_query(
                    upd_query,
                    keys,
                )
                prepared_data = self._prepare_data(keys, True)
                self._log.message(f"Inserting new data on [{self._table}]...")
                self._db.execute_many(ins_query, prepared_data)
            else:
                raise MemoryError
        except MemoryError:
            upd_query = self._generate_update_query(
                self._table,
                self._data,
                keys,
            )
            ins_query = self._convert_update_query_to_insert_query(upd_query)
            prepared_data = self._prepare_data(keys)

            for row in tqdm(
                unit="record(s)",
                iterable=prepared_data,
                desc=f"Updating data on [{self._table}]",
            ):
                try:
                    self._db.execute_write_query(upd_query, row, True)
                except ValueError:
                    self._db.execute_write_query(ins_query, row, True)

    def _prepare_data(
        self, keys: Optional[List[str]] = None, duplicate_keys: bool = False
    ) -> List[tuple]:
        """
        Prepare data for insertion into a database table.

        This method shifts the order of values in the tuple to match the order
        of the keys used in the ETL Process (if keys is set).

        It also converts any nested list or dictionary in the tuples to a
        string for easier loading onto the database.

        Lastly, it converts any 'nan' values to None for easier loading onto
        the db.

        Args:
            keys (Optional[List[str]]): List of keys for updates.
            duplicate_keys (bool): Whether to additionally duplicate and append
                the keys to the end of the tuple. Useful for the WHERE NOT
                EXISTS clause.

        Returns:
            List[Tuple]: A list of tuples representing the processed rows ready
                for insertion.
        """
        prepared_rows = []

        for _, row in self._data.iterrows():
            processed_row = []

            if keys is not None:
                shifted_values = [row[c] for c in row.index if c not in keys]
                shifted_values.extend([row[col] for col in keys])

                if duplicate_keys:
                    shifted_values.extend([row[col] for col in keys])

                processed_row.extend(
                    json.dumps(val) if isinstance(val, (list, dict)) else val
                    for val in shifted_values
                )
            else:
                processed_row.extend(
                    json.dumps(val) if isinstance(val, (list, dict)) else val
                    for val in row
                )

            # Replace 'nan' value with None
            processed_row = tuple(
                None if isinstance(val, float) and math.isnan(val) else val
                for val in processed_row
            )
            prepared_rows.append(processed_row)

        return prepared_rows

    def _generate_create_table_query(
        self, table_name: str, pandas_dataset: pd.DataFrame
    ) -> str:
        """
        Generate the SQL query for creating a database table.

        Args:
            table_name (str): The name of the database table.
            pandas_dataset (pandas.DataFrame): The dataset from the CSV file.

        Returns:
            str: The SQL query for creating the table.
        """
        column_definitions = [
            f'"{column}" {self._get_column_data_type(pandas_dataset[column])}'
            for column in pandas_dataset.columns
        ]
        columns_str = ",\n".join(column_definitions)
        create_table_query = f"""
            CREATE TABLE "{table_name}" (
            {columns_str}
            )
        """
        return create_table_query

    def _get_column_data_type(self, column: pd.Series) -> str:
        """
        Get the SQL data type for a column based on its data type.

        Args:
            column (pd.Series): The column of the DataFrame.

        Returns:
            str: The SQL data type for the column.
        """
        dtype_map = {"int64": "INT", "float64": "FLOAT", "bool": "BOOLEAN"}

        return (
            "NVARCHAR(MAX)"
            if self._force_nvarchar
            else dtype_map.get(str(column.dtype), "NVARCHAR(MAX)")
        )

    def _generate_insert_query(
        self, table_name: str, pandas_dataset: pd.DataFrame
    ) -> str:
        """
        Generate the SQL query for inserting data into a database table.

        Args:
            table_name (str): The name of the database table.
            pandas_dataset (pandas.DataFrame): The extracted pandas dataset.

        Returns:
            str: The SQL query for inserting data into the table.
        """
        column_names = [
            f'"{column_name}"'  # Comment for formatting
            for column_name in pandas_dataset.columns
        ]
        placeholders = ",".join(["?" for _ in column_names])
        columns = ",".join(column_names)
        return f"""
            INSERT INTO "{table_name}" ({columns})
            VALUES ({placeholders})
        """

    def _generate_update_query(
        self,
        table_name: str,
        pandas_dataset: pd.DataFrame,
        selected_columns: List[str],
    ) -> str:
        """
        Generate the SQL query for updating data in a database table.

        Args:
            table_name (str): The name of the database table.
            pandas_dataset (pandas.DataFrame): The extracted pandas dataset.
            selected_columns (List[str]): List of keys for updates.

        Returns:
            str: The SQL query for updating data in the table.
        """
        column_defs = [
            f'"{column_name}" = ?'
            for column_name in pandas_dataset.columns
            if column_name not in selected_columns
        ]
        columns = ", ".join(column_defs)
        where_conditions = " AND ".join(
            [f'"{column_name}" = ?' for column_name in selected_columns]
        )
        return f"""
            UPDATE "{table_name}"
            SET {columns}
            WHERE {where_conditions}
        """

    def _convert_update_query_to_insert_query(
        self, update_query: str, selected_columns: Optional[List[str]] = None
    ) -> str:
        """
        Convert an update query into an insert query with an optional WHERE NOT
        EXISTS clause.

        Args:
            update_query (str): The update query to convert.
            selected_columns (List[str], optional): List of keys for the WHERE
                NOT EXISTS clause.

        Returns:
            str: The converted insert query.
        """
        # Extract table name and SET clause from the update query
        table_name_start = update_query.index('"') + 1
        table_name_end = update_query.index('"', table_name_start)
        table_name = update_query[table_name_start:table_name_end]

        set_start = update_query.index("SET") + len("SET") + 1
        set_clause = update_query[set_start:]

        where_start = update_query.index("WHERE") + len("WHERE") + 1
        where_clause = update_query[where_start:]

        # Extract column names and placeholders from the SET clause
        set_parts = [part.strip() for part in set_clause.split(",")]
        column_names = []
        placeholders = []

        for set_part in set_parts:
            column_name_end = set_part.index("=")
            column_name = set_part[:column_name_end].strip()
            column_names.append(column_name)
            set_part_index = set_part.index("?")
            placeholder = set_part[set_part_index:]
            placeholders.append(placeholder)

        # Extract column names and values from the WHERE clause
        where_parts = [part.strip() for part in where_clause.split("AND")]
        for where_part in where_parts:
            column_name_end = where_part.index("=")
            column_name = where_part[:column_name_end].strip()
            column_names.append(column_name)
            where_part_index = where_part.index("?")
            placeholder = where_part[where_part_index:]
            placeholders.append(placeholder)

        # Replace the remaining WHERE clause in the placeholders
        placeholders = [
            placeholder if "WHERE" not in placeholder else "?"
            for placeholder in placeholders
        ]

        # Create the insert query with column names and placeholders
        insert_query = f"""
            INSERT INTO "{table_name}"
            ({', '.join(column_names)})
        """

        # Append the WHERE NOT EXISTS subquery or VALUES as needed
        # flake8: noqa: E126 # NOTE: Indentation problems for this section
        if selected_columns:
            insert_query += f"""
            SELECT {', '.join(placeholders)}
            WHERE NOT EXISTS (
                SELECT * FROM "{table_name}"
                WHERE {" AND ".join([
                    f'"{column}" = ?'
                    for column in selected_columns
                ])}
            )
            """
        else:
            insert_query += f"""
            VALUES ({', '.join(placeholders)})
            """

        return insert_query

    def _flatten_dict(
        self,
        d: MutableMapping,
        sep: str = "_",
    ) -> MutableMapping:
        """
        Flatten a dictionary by joining nested keys with a separator.

        Args:
            d (MutableMapping): The dictionary to be flattened.
            sep (str, optional): The separator to join nested keys with.
                Defaults to "_".

        Returns:
            MutableMapping: The flattened dictionary.
        """
        flat_dict = dict(d)
        [flat_dict] = pd.json_normalize(flat_dict, sep=sep).to_dict("records")
        return flat_dict

    def _extract_nested_data(
        self,
        data: Union[List[MutableMapping], List[dict]],
        keys: List[str],
    ):
        """
        Extract nested data from a list of dictionaries.

        Args:
            data (List[MutableMapping] | List[dict]): The list of dictionaries
                containing nested data.
            keys (List[str]): The list of columns to use as keys to reference
                the parent dataset.
        Returns:
            List[MutableMapping]: The list of dictionaries after extracting
                nested data.
        """
        # Find columns with list values
        columns_with_list_values = set(
            [
                key
                for dictionary in data
                for key, value in dictionary.items()
                if isinstance(value, list)
                for item in value
                if isinstance(item, dict)
            ]
        )

        # Iterate through columns with list values
        for column in columns_with_list_values:
            extracted_data = []
            for dictionary in data:
                if column in dictionary:
                    for key in keys:
                        if key in dictionary and dictionary[column]:
                            # Add a key column for reference to the main data
                            for d in dictionary[column]:
                                d[key] = dictionary[key]
                    extracted_data.append(dictionary[column])
                    del dictionary[column]  # Remove column from original data

            # Merge extracted lists
            extracted_data = [
                item
                for sublist in extracted_data
                if sublist
                for item in sublist
                if isinstance(item, dict)
            ]

            # Add extracted data to the list of nested tables to load
            self._nested_data.update({column: extracted_data})

        return data  # Return remaining data without nested tables

    def _log_number_of_records(self):
        """
        Logs the number of records after data extraction.

        Also prints the DataFrame if it's less than 5 rows for easier
        inspection.
        """
        num_records = len(self._data)
        if num_records > 0:
            self._log.message(f"Number of records extracted: {num_records}")
            if num_records <= 5:
                self._log.message("Extracted data:", LogLevel.INFO)
                self._log.message(str(self._data), LogLevel.INFO)
        else:
            self._log.message("No records were extracted.", LogLevel.WARN)
