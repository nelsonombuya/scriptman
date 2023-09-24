import json
import math
from typing import Any, Callable, Dict, List, MutableMapping, Optional, Union

import pandas as pd
from tqdm import tqdm

from scriptman.csv_handler import CSVHandler
from scriptman.database import DatabaseHandler
from scriptman.logs import LogHandler, LogLevel
from scriptman.settings import Settings


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
        self._log = LogHandler("ETL Handler")
        self._data: pd.DataFrame = pd.DataFrame()
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

        if extract_data_from_sublists:
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

    def to_df(self) -> pd.DataFrame:
        """
        Return extracted data as a Pandas DataFrame.

        Returns:
            pd.DataFrame: The extracted DataFrame.
        """
        return self._data

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
                    keys=keys,
                    truncate=truncate,
                    recreate=recreate,
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
            return self._insert(truncate, recreate)
        else:
            return self._update(keys)

    def _insert(self, truncate: bool = False, recreate: bool = False) -> None:
        """
        Insert data into a database table.

        Args:
            truncate (bool): Whether to truncate the table.
            recreate (bool): Whether to recreate the table.
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
            self._db.execute_many(insert_query, prepared_data)
        except MemoryError:
            for row in tqdm(
                unit="record(s)",
                iterable=prepared_data,
                desc=f"Loading data onto [{self._table}]",
            ):
                self._db.execute_write_query(insert_query, row)

    def _update(self, keys: List[str]) -> None:
        """
        Update data in a database table.

        Args:
            keys (List[str]): List of keys for updates.
        """
        upd_query = self._generate_update_query(self._table, self._data, keys)
        ins_query = self._convert_update_query_to_insert_query(upd_query)
        prepared_data = self._prepare_data(keys)

        try:
            self._db.execute_many(upd_query, prepared_data)
        except MemoryError:
            for row in tqdm(
                unit="record(s)",
                iterable=prepared_data,
                desc=f"Updating data on [{self._table}]",
            ):
                try:
                    self._db.execute_write_query(upd_query, row, True)
                except ValueError:
                    self._db.execute_write_query(ins_query, row, True)

    def _prepare_data(self, keys: Optional[List[str]] = None) -> List[tuple]:
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

    def _convert_update_query_to_insert_query(self, update_query: str) -> str:
        """
        Convert an update query into an insert query.

        Args:
            update_query (str): The update query to convert.

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

        for i, set_part in enumerate(set_parts):
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
                for item in sublist
                if isinstance(item, dict)
            ]

            # Add extracted data to the list of nested tables to load
            self._nested_data.update({column: extracted_data})

        return data  # Return remaining data without nested tables

    def _log_number_of_records(self):
        """
        Logs the number of records after data extraction.

        Also prints the DataFrame if debugging mode is on.
        """
        self._log.message(f"Extracted {len(self._data)} records.")

        if Settings.debug_mode:
            print(self._data)
