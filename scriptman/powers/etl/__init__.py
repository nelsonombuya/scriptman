try:
    from contextlib import contextmanager
    from functools import partial
    from pathlib import Path
    from typing import Any, Callable, Generator, Iterator, Literal, Optional, cast

    from loguru import logger
    from pandas import DataFrame, MultiIndex, concat

    from scriptman.powers.concurrency import TaskExecutor
    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.database._exceptions import DatabaseError
    from scriptman.powers.etl._database import ETLDatabase
    from scriptman.powers.generics import P
    from scriptman.powers.time_calculator import TimeCalculator
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[etl]."
    )

ETL_TYPES = DataFrame | list[dict[str, Any]] | list[tuple[Any, ...]]


class ETL:
    """🔍 Data processing utility for Extract, Transform, Load operations."""

    log = logger
    _data: DataFrame = DataFrame()

    def __init__(self, data: Optional[ETL_TYPES] = None) -> None:
        """
        🚀 Initialize ETL with optional data.

        Args:
            data (Optional[DataFrame | list[dict[str, Any]]]): The data to initialize
                the ETL object with.
        """
        # Delegate DataFrame properties and methods
        self._data: DataFrame = DataFrame(data) if data is not None else DataFrame()
        self.columns = self._data.columns
        self.empty = self._data.empty
        self.index = self._data.index

    """
    🔍 DataFrame property and method delegates
    """

    @property
    def data(self) -> DataFrame:
        """📊 Access the underlying DataFrame."""
        return self._data

    def set_index(self, *args: Any, **kwargs: Any) -> "ETL":
        """🔍 Set the index of the DataFrame."""
        if "inplace" in kwargs:
            self._data.set_index(*args, **kwargs)
            return self
        return ETL(self._data.set_index(*args, **kwargs))

    def __getitem__(self, key: Any) -> Any:
        """🔍 Get an item from the DataFrame."""
        return self._data[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        """🔍 Set an item in the DataFrame."""
        self._data[key] = value

    def __delitem__(self, key: Any) -> None:
        """🔍 Delete an item from the DataFrame."""
        del self._data[key]

    def __contains__(self, key: Any) -> bool:
        """🔍 Check if an item is in the DataFrame."""
        return key in self._data

    def __len__(self) -> int:
        """🔍 Get the length of the DataFrame."""
        return len(self._data)

    def __repr__(self) -> str:
        """🔍 Get the representation of the DataFrame."""
        return repr(self._data)

    """
    🔍 Context managers
    """

    @classmethod
    @contextmanager
    def timed_context(
        cls,
        context: str = "Code Block",
        operation: Optional[Literal["extraction", "transformation", "loading"]] = None,
    ) -> Generator[None, None, None]:
        """
        ⏱️ A context manager for ETL operations, logging the start, completion,
        and details of the data processing with timing information.

        Args:
            context (str): The name of the context for logging purposes.
            operation (Optional[Literal["extraction", "transformation", "loading"]]):
                The specific ETL operation being performed. If provided, additional
                operation-specific logging will be included.

        Yields:
            None: This is a generator function used as a context manager.

        Logs:
            - Info: When the operation starts.
            - Success: When the operation completes.
            - Debug: The number of records and data details if records are found.
            - Warning: If no records were found (for extraction operations).
        """
        preposition = "from" if operation in {"extraction", "transformation"} else "to"
        operation_str = f"{operation} {preposition} {context}" if operation else context
        exception: Optional[Exception] = None

        try:
            with TimeCalculator.context(context):
                cls.log.info(f"Data {operation_str} started...")
                yield
        except Exception as error:
            exception = error
        finally:
            if exception:
                cls.log.error(f"Error during {operation_str}: {exception}")
                raise exception
            cls.log.success(f"Data {operation_str} complete.")

    """
    🔍 Extraction Methods
    """

    @classmethod
    def from_dataframe(cls, data: DataFrame) -> "ETL":
        """
        🔍 Create an ETL object from a DataFrame.
        """
        return cls(data)

    @classmethod
    def from_dataframe_list(cls, data: list[DataFrame], **kwargs: Any) -> "ETL":
        """
        🔍 Create an ETL object from a list of DataFrames.
        """
        return cls(concat(data, **kwargs))

    @classmethod
    def from_etl_list(cls, data: list["ETL"], **kwargs: Any) -> "ETL":
        """
        🔍 Create an ETL object from a list of ETL objects.
        """
        return cls(concat([_.data for _ in data], **kwargs))

    @classmethod
    def from_list(cls, data: list[dict[str, Any]]) -> "ETL":
        """
        🔍 Create an ETL object from a list of dictionaries.
        """
        return cls(data)

    @classmethod
    def from_csv_file(cls, file_path: str | Path) -> "ETL":
        """
        📃 Extract data from a CSV file.

        Args:
            file_path (str | Path): The path to the CSV file to extract from.

        Raises:
            FileNotFoundError: If no files matched the pattern.

        Returns:
            ETL: The extracted data as an ETL object.

        Logs:
            - Info: When a CSV file is found.
            - Debug: The number of records and extracted data if records are found.
            - Warning: If no records were extracted.
        """
        from pandas import read_csv

        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        with cls.timed_context("CSV", "extraction"):
            if file_path.exists():
                cls.log.info(f"Found CSV File at {file_path}...")
                return cls(read_csv(file_path))
            raise FileNotFoundError(f"No file found at: {file_path}")

    @classmethod
    def from_json_file(cls, file_path: str | Path) -> "ETL":
        """
        📃 Extract data from a JSON file.

        Args:
            file_path (str | Path): The path to the JSON file to extract from.

        Raises:
            FileNotFoundError: If no files matched the pattern.

        Returns:
            ETL: The extracted data as an ETL object.

        Logs:
            - Info: When a JSON file is found.
            - Debug: The number of records and extracted data if records are found.
            - Warning: If no records were extracted.
        """
        from json import load

        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        with cls.timed_context("JSON", "extraction"):
            if file_path.exists():
                cls.log.info(f"Found JSON File at {file_path}...")
                with open(file_path, "r", encoding="utf-8") as file:
                    data = load(file)
                return cls(data)
            raise FileNotFoundError(f"No file found at: {file_path}")

    @classmethod
    def from_db(
        cls,
        db: DatabaseHandler,
        query: str,
        params: dict[str, Any] = {},
    ) -> "ETL":
        """
        📂 Extract data from a database using a provided query.

        Args:
            db (DatabaseHandler): The handler to manage the database connection.
            query (str): The SQL query to execute for data extraction.
            params (dict[str, Any], optional): A dictionary of parameters to use in the
                query. Defaults to an empty dictionary.

        Returns:
            ETL: The extracted data as an ETL object.

        Logs:
            - Context: "Database" extraction context.
        """
        with cls.timed_context("Database", "extraction"):
            return cls(db.execute_read_query(query, params))

    @classmethod
    def from_extractor(
        cls, extractor: Callable[P, ETL_TYPES], *args: Any, **kwargs: Any
    ) -> "ETL":
        """
        ⚙ Extract data using a custom extractor function.

        Args:
            extractor: A function that implements the extraction logic
            *args: Additional arguments to pass to the extractor function
            **kwargs: Additional keyword arguments to pass to the extractor function

        Returns:
            ETL: The extracted data as an ETL object
        """
        with cls.timed_context(extractor.__name__, "extraction"):
            return cls(extractor(*args, **kwargs))

    """
    🔍 Transformation methods
    """

    def transform(
        self,
        transformer: Callable[[DataFrame], ETL_TYPES],
        context: str = "Transformation Code Block",
    ) -> "ETL":
        """
        🔍 Apply custom transformation function.

        Args:
            transformer (Callable[[DataFrame], DataFrame]): The transformation function.
            context (str): The context of the transformation.
        """
        with self.timed_context(context, "transformation"):
            return ETL(transformer(self._data))

    def merge(
        self,
        right: "ETL | DataFrame",
        how: Literal["inner", "left", "right", "outer", "cross"] = "inner",
        on: Optional[str | list[str]] = None,
        left_on: Optional[str | list[str]] = None,
        right_on: Optional[str | list[str]] = None,
        left_index: bool = False,
        right_index: bool = False,
        suffixes: tuple[str, str] = ("_x", "_y"),
        **kwargs: Any,
    ) -> "ETL":
        """
        🔀 Merge the ETL object with another ETL object or DataFrame.

        This method wraps pandas' DataFrame.merge functionality to combine two datasets.

        Args:
            right (ETL | DataFrame): The right ETL object or DataFrame to merge with.
            how (Literal["inner", "left", "right", "outer", "cross"]): Type of merge to
                perform. Defaults to "inner".
            on (Optional[str | list[str]]): Column(s) to join on if column names are the
                same in both datasets. Defaults to None.
            left_on (Optional[str | list[str]]): Column(s) from the left dataset to join
                on. Defaults to None.
            right_on (Optional[str | list[str]]): Column(s) from the right dataset to join
                on. Defaults to None.
            left_index (bool): Use the index from the left dataset as join key. Defaults
                to False.
            right_index (bool): Use the index from the right dataset as join key. Defaults
                to False.
            suffixes (tuple[str, str]): Suffixes to use for overlapping column names.
                Defaults to ("_x", "_y").
            **kwargs: Additional arguments to pass to pandas' merge function.

        Returns:
            ETL: A new ETL object with the merged data.
        """
        with self.timed_context("Merge", "transformation"):
            return ETL(
                self._data.merge(
                    right.data if isinstance(right, ETL) else right,
                    on=on,
                    how=how,
                    left_on=left_on,
                    right_on=right_on,
                    suffixes=suffixes,
                    left_index=left_index,
                    right_index=right_index,
                    **kwargs,
                )
            )

    def concat(
        self, other: "ETL | DataFrame | list[ETL | DataFrame]", **kwargs: Any
    ) -> "ETL":
        """
        🔗 Concatenate this ETL object with other ETL objects or DataFrames.

        This method wraps pandas' concat functionality to combine datasets by stacking
        them.

        Args:
            other: The ETL object(s) or DataFrame(s) to concatenate with this one.
            axis: The axis to concatenate along (0 for rows/vertically, 1 for
                columns/horizontally). Defaults to 0.
            ignore_index: If True, do not use the index values on the concatenation axis.
                Defaults to False.
            **kwargs: Additional arguments to pass to pandas' concat function.

        Returns:
            ETL: A new ETL object with the concatenated data.
        """
        with self.timed_context("Concatenation", "transformation"):
            if "axis" not in kwargs:
                kwargs["axis"] = 0

            if "ignore_index" not in kwargs:
                kwargs["ignore_index"] = False

            if isinstance(other, (ETL, DataFrame)):
                others = [other]
            else:
                others = other

            df_list = [self._data] + [_.data if isinstance(_, ETL) else _ for _ in others]
            result = concat(df_list, **kwargs)
            return ETL(result)

    def filter(self, condition: Any, context: str = "Filtering Code Block") -> "ETL":
        """
        🔍 Filter rows based on condition.

        The conditions are applied to the DataFrame and the result is returned as a new
        ETL object.

        Args:
            condition: The condition to filter the rows by.
            context (str): The context of the filtering. Defaults to
                "Filtering Code Block".
        """
        with self.timed_context(context, "transformation"):
            return ETL(self._data[condition])

    def flatten_all_nested_columns(
        self, case: Literal["snake", "camel"] = "snake"
    ) -> "ETL":
        """
        🔍 Flatten all the columns in the dataframe that contain nested dictionaries.

        This method identifies all columns containing nested dictionaries and flattens
        them into separate columns. The new column names are created by combining the
        original column name with the nested keys, separated by the specified separator.

        Args:
            case (Literal["snake", "camel"], optional): The case to use for the new
                column names. Defaults to "snake".

        Returns:
            ETL: A new ETL object with all nested dictionary columns flattened.

        Example:
            If your DataFrame has columns 'metadata' and 'settings' with nested
            dictionaries, calling `etl.flatten_all_nested_columns()` will flatten both
            columns in one operation.
        """
        with self.timed_context("Flatten Nested Dictionaries", "transformation"):
            # Identify columns with dictionary values
            dict_columns = []
            for col in self._data.columns:
                if self._data[col].apply(lambda x: isinstance(x, dict)).any():
                    dict_columns.append(col)

            # No dictionary columns found
            if not dict_columns:
                return ETL(self._data)

            # Flatten each dictionary column
            result = self._data.copy()
            for col in dict_columns:
                result = ETL(result).flatten(col, case).data

            return ETL(result)

    def flatten(self, column: str, case: Literal["snake", "camel"] = "snake") -> "ETL":
        """
        🔍 Flatten a nested dictionary column into separate columns.

        This method takes a column containing nested dictionaries and flattens it,
        creating new columns for each nested key. The new column names are created
        by combining the original column name with the nested keys, separated by
        the specified separator.

        Args:
            column (str): The name of the column containing nested dictionaries.
            case (Literal["snake", "camel"], optional): The case to use for the new
                column names. Defaults to "snake".


        Returns:
            ETL: A new ETL object with the flattened DataFrame.

        Example:
            If your DataFrame has a column 'metadata' with values like:
            {'user': {'id': 123, 'name': 'John'}, 'status': 'active'}

            After calling `etl.flatten('metadata')`, you'll get new columns:
            'metadata_user_id', 'metadata_user_name', 'metadata_status'

            The final DataFrame will look like this:
            | metadata_user_id | metadata_user_name | metadata_status | status |
            | 123              | John               | active          | active |
        """
        from pandas import json_normalize

        with self.timed_context("Flatten", "transformation"):
            if column not in self._data.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame")

            # Extract and flatten the nested dictionaries
            df = self._data.copy()
            nested_data = df[column].tolist()
            flattened = json_normalize(nested_data, sep="_")
            case_func = (
                self.__convert_to_snake_case
                if case == "snake"
                else self.__convert_to_camel_case
            )
            flattened.columns = [  # type:ignore # Modifying types to avoid type errors
                case_func(f"{column.lower()}_{str(col).lower()}")
                for col in flattened.columns
            ]

            # Combine with original DataFrame (excluding the original nested column)
            result = df.drop(columns=[column])
            for col in flattened.columns:
                result[col] = flattened[col].values

            return ETL(result)

    def pop_nested_column(self, column: str, drop: bool = True) -> "ETL":
        """
        📊 Extract a nested list column into a new ETL object.

        This method takes a column containing nested lists (of strings, dicts, tuples,
            etc.) and creates a new ETL object with the nested data expanded into rows,
            preserving the original index values as columns for each row.

        Args:
            column (str): The name of the column containing nested data to extract.
            drop (bool, optional): Whether to drop the original column from the
                DataFrame. Defaults to True.

        Returns:
            ETL: A new ETL object with the extracted and normalized nested data.

        Raises:
            ValueError: If the column doesn't exist or contains invalid data.

        Example:
            If your DataFrame has indices ['country_id', 'id'] and a column
            'skilledTrades' with lists of strings, calling
            `etl.pop_nested_column('skilledTrades')` will create a new ETL object with
            columns ['country_id', 'id', 'skilledTrades_value'] where each nested item
            becomes a separate row.

            For lists of tuples or lists of lists, each element in the tuple or list
            becomes a separate column with positional naming (e.g., 'column_0',
            'column_1', etc.).
        """
        with self.timed_context(f"Pop nested column: {column}", "transformation"):
            if column not in self._data.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame")

            # Get index columns - if no index is set, use the DataFrame's default index
            if self._data.index.name is None and not isinstance(
                self._data.index, MultiIndex
            ):
                index_cols: list[str] = []
                has_named_index: bool = False
            else:
                index_cols = (
                    [str(_) for _ in self._data.index.names]
                    if isinstance(self._data.index, MultiIndex)
                    else [str(self._data.index.name)]
                )
                has_named_index = True

            # Create a list to store the expanded rows
            expanded_rows: list[dict[str, Any]] = []

            # Iterate through each row in the DataFrame
            for idx, row in self._data.iterrows():
                if not isinstance(row[column], (list, tuple)) or not row[column]:
                    continue  # Skip rows with empty or non-list/tuple values

                # Get the index values for this row
                if has_named_index:
                    if isinstance(idx, (tuple, list)):
                        idx_values = {name: val for name, val in zip(index_cols, idx)}
                    else:
                        idx_values = {index_cols[0]: idx}
                else:
                    idx_values = {}

                # Process each item in the nested list
                for item in row[column]:
                    if isinstance(item, dict):
                        # For dictionaries, flatten with prefix to avoid key collisions
                        nested_row = idx_values.copy()
                        for k, v in item.items():
                            # If the key exists in the index, prefix it to avoid collision
                            if k in idx_values:
                                nested_row[f"{column}_{k}"] = v
                            else:
                                nested_row[f"{column}_{k}"] = v
                        expanded_rows.append(nested_row)
                    elif isinstance(item, (tuple, list)):
                        # For tuples, create positionally named columns
                        nested_row = idx_values.copy()
                        for i, element in enumerate(item):
                            nested_row[f"{column}_{i}"] = element
                        expanded_rows.append(nested_row)
                    else:
                        # For primitives (strings, numbers, etc.)
                        expanded_row = idx_values.copy()
                        expanded_row[f"{column}_value"] = item
                        expanded_rows.append(expanded_row)

            # Remove the original column from the main DataFrame
            if drop:
                self._data = self._data.drop(columns=[column])

            # Return a new ETL object with the expanded data
            return ETL(expanded_rows)

    def get_nested_list_columns(self, pop: bool = False) -> dict[str, "ETL"]:
        """
        🔍 Get all columns that contain lists of dictionaries and return them as ETL
        instances.

        Args:
            pop (bool, optional): If True, removes the nested list columns from the
                original DataFrame. Defaults to False.

        Returns:
            dict[str, ETL]: A dictionary where keys are column names and values are ETL
                instances containing the nested list of dictionaries.

        Example:
            If your DataFrame has columns 'users' and 'settings' where 'users' contains:
            [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}]

            # Without popping (maintains original DataFrame):
            nested_cols = etl.get_nested_list_columns()
            # Returns: {'users': ETL([{'id': 1, ...}]), 'settings': ETL([...])}
            # etl still contains both 'users' and 'settings' columns

            # With popping (removes columns from original DataFrame):
            nested_cols = etl.get_nested_list_columns(pop=True)
            # Returns: {'users': ETL([{'id': 1, ...}]), 'settings': ETL([...])}
            # etl no longer contains 'users' and 'settings' columns
        """
        nested_columns: dict[str, "ETL"] = {}
        columns_to_drop: list[str] = []

        for col in self._data.columns:
            # Skip empty columns or columns with all NA values
            if self._data[col].empty or self._data[col].isna().all():
                continue

            # Get first non-null value to check type
            first_value = (
                self._data[col].dropna().iloc[0]
                if not self._data[col].dropna().empty
                else None
            )
            if not isinstance(first_value, (list, tuple)):
                continue

            # Check if the first value contains dictionaries
            if first_value and all(isinstance(item, dict) for item in first_value):
                all_dicts = []
                for item_list in self._data[col].dropna():
                    all_dicts.extend(item_list)

                nested_columns[col] = ETL(all_dicts)
                columns_to_drop.append(col)

        if pop and columns_to_drop:
            self._data = self._data.drop(columns=columns_to_drop)

        return nested_columns

    def to_snake_case(self) -> "ETL":
        """
        🐍 Converts all column names in the DataFrame to snake_case.

        This method transforms column names like 'FirstName', 'first-name', 'First Name'
        to 'first_name'.

        Returns:
            ETL: A new ETL instance with snake_case column names.

        Example:
            # Convert columns like 'FirstName', 'LastName' to 'first_name', 'last_name'
            etl_snake = etl.to_snake_case()
        """
        # Create a copy of the DataFrame with renamed columns
        renamed_columns = {_: self.__convert_to_snake_case(_) for _ in self._data.columns}
        new_data = self._data.rename(columns=renamed_columns)
        self.log.info(f"Converted {len(renamed_columns)} column names to snake_case")
        return ETL(new_data)

    def to_camel_case(self) -> "ETL":
        """
        🐐 Converts all column names in the DataFrame to camelCase.

        This method transforms column names like 'first_name', 'last_name' to
        'FirstName', 'LastName'.

        Returns:
            ETL: A new ETL instance with camelCase column names.

        Example:
            # Convert columns like 'first_name', 'last_name' to 'FirstName', 'LastName'
            etl_camel = etl.to_camel_case()
        """
        # Create a copy of the DataFrame with renamed columns
        renamed_columns = {_: self.__convert_to_camel_case(_) for _ in self._data.columns}
        new_data = self._data.rename(columns=renamed_columns)
        self.log.info(f"Converted {len(renamed_columns)} column names to camelCase")
        return ETL(new_data)

    def __convert_to_snake_case(self, name: str) -> str:
        """🐍 Convert a string to snake_case."""
        from re import sub

        # Replace spaces, hyphens, and other separators with underscores
        s1 = sub(r"[\s\-\.]", "_", name)
        # Insert underscore between camelCase transitions
        s2 = sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
        # Convert to lowercase
        return s2.lower()

    def __convert_to_camel_case(self, name: str) -> str:
        """🐐 Convert a string to camelCase."""
        from re import sub

        # Replace underscores with spaces
        s1 = sub(r"_", " ", name)

        # Handle capitalization - first word should be lowercase, others uppercase
        words = s1.split()
        if len(words) == 1:
            # If it's a single word, just lowercase it
            result = words[0].lower()
        else:
            # First word lowercase, rest capitalized
            first_word = words[0].lower()
            rest_words = [word.capitalize() for word in words[1:]]
            result = first_word + "".join(rest_words)

        return result

    """
    🔍 Loading methods
    """

    def to_dataframe(self) -> DataFrame:
        """
        🔍 Convert the ETL object to a DataFrame.
        """
        return self._data

    def to_list(self) -> list[dict[str, Any]]:
        """
        🔍 Convert the ETL object to a list of dictionaries.
        """
        return cast(
            list[dict[str, Any]],
            self._data.reset_index().to_dict(orient="records"),
        )

    def to_csv_file(self, file_path: str | Path) -> Path:
        """
        📃 Saves the data to a CSV file using the given file path.

        Args:
            file_path (str | Path): The path to the file to save the data to.

        Returns:
            ETL: The ETL object with the data saved to the file.

        Logs:
            - Warning: If the dataset is empty.
            - Success: The path to the saved file.
        """
        with self.timed_context("CSV", "loading"):
            if self.empty:
                self.log.warning("Dataset is empty!")
                raise ValueError("Dataset is empty!")

            file_path = Path(file_path) if isinstance(file_path, str) else file_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            self._data.reset_index().to_csv(file_path, index=False)
            self.log.success(f"Data saved to {file_path}")
            return file_path

    def to_json_file(self, file_path: str | Path, indent: int = 2) -> Path:
        """
        📃 Saves the data to a JSON file using the given file path.

        Args:
            file_path (str | Path): The path to the file to save the data to.
            indent (int, optional): The number of spaces to indent the JSON file.
                Defaults to 2.

        Returns:
            ETL: The ETL object with the data saved to the file.

        Logs:
            - Warning: If the dataset is empty.
            - Success: The path to the saved file.
        """
        with self.timed_context("JSON", "loading"):
            if self.empty:
                self.log.warning("Dataset is empty!")
                raise ValueError("Dataset is empty!")

            file_path = Path(file_path) if isinstance(file_path, str) else file_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            self._data.reset_index().to_json(file_path, orient="records", indent=indent)
            self.log.success(f"Data saved to {file_path}")
            return file_path

    def to_db(
        self,
        db_handler: DatabaseHandler,
        table_name: str,
        batch_size: int = 1000,
        batch_execute: bool = True,
        force_nvarchar: bool = False,
        synchronize_schema: bool = True,
        method: Literal["truncate", "replace", "insert", "update", "upsert"] = "upsert",
    ) -> bool:
        """
        📂 Loads the ETL data into a database table, with options for batch execution and
        different loading methods (truncate, replace, upsert, insert, update).

        NOTE: To have the database be created/inserted/updated with the correct keys,
        ensure that the indices are defined in the dataset using the `set_index` method.

        Args:
            db_handler (DatabaseHandler): The database handler to use for executing
                queries.
            table_name (str): The name of the table to load the data into.
            batch_execute (bool, optional): Whether to execute queries in batches.
                Defaults to True.
            force_nvarchar (bool, optional): Whether to force the use of NVARCHAR data
                types. Defaults to False.
            batch_size (Optional[int], optional): The number of rows to include in each
                batch. Defaults to 1000.
            method (Literal["truncate", "replace", "insert", "update", "upsert"]):
                The loading method to use. Defaults to "upsert".
            synchronize_schema (bool, optional): Whether to synchronize the schema of
                the table before loading the data. Defaults to True.

        Raises:
            ValueError: If the dataset is empty or if bulk execute is disabled.
            DatabaseError: If a database error occurs during execution.

        Returns:
            bool: True if the data was loaded successfully.
        """
        # Wrap the handler with ETLDatabase for extended functionality
        executor = TaskExecutor()
        db = ETLDatabase(db_handler)
        table_exists: bool = db.table_exists(table_name)

        if self.empty:
            self.log.warning("Dataset is empty!")
            raise ValueError("Dataset is empty!")

        if method == "truncate" and table_exists:
            db.truncate_table(table_name)

        if method == "replace" and table_exists:
            db.drop_table(table_name)
            table_exists = False

        if (method in {"upsert", "update"}) and self._data.index.empty:
            message = (
                "Dataset has no index! "
                "Please set the index using the `set_index` method."
            )
            self.log.error(message)
            raise ValueError(message)

        if table_exists and synchronize_schema:
            db.synchronize_table_schema(
                force_nvarchar=force_nvarchar,
                table_name=table_name,
                df=self._data,
            )

        if not table_exists:
            self.log.warning(f'Table "{table_name}" does not exist. Creating table...')
            db.create_table(
                table_name=table_name,
                keys=[str(_) for _ in self._data.index.names],
                columns=db.get_table_data_types(self._data.reset_index(), force_nvarchar),
            )
            method = "insert"
            self.log.info(f"Since table was created, method set to: {method}")

        query, values = {
            "insert": db.generate_prepared_insert_query,
            "update": db.generate_prepared_update_query,
            "upsert": db.generate_prepared_upsert_query,
        }.get(method, db.generate_prepared_upsert_query)(
            force_nvarchar=force_nvarchar,
            table_name=table_name,
            df=self._data,
        )
        self.log.info(
            f"{method.capitalize()}ing data "
            f'into "{db.database_name}"."{table_name}" '
            f'with "{db.database_type}" database'
        )
        self.log.debug(f"Query: {query}")

        try:
            if f"merge [{table_name}] as target" in query.lower():
                return self._merge(
                    query=query,
                    values=values,
                    database_handler=db,
                    batch_size=batch_size,
                    table_name=table_name,
                    force_nvarchar=force_nvarchar,
                )

            if not batch_execute:
                raise ValueError("Bulk Execute is disabled.")

            return db.execute_write_batch_query(query, values, batch_size)

        except (MemoryError, ValueError) as error:
            if not batch_execute:
                self.log.info("Bulk Execute is disabled. Executing single queries...")
            else:
                self.log.warning(f"Bulk Execution Failed: {error}")
                self.log.warning("Executing single queries...")

            tasks = executor.multithread(
                [(db.execute_write_query, (query, row), {}) for row in values]
            )
            tasks.await_results()  # Will raise an exception if any query fails
            return tasks.are_successful

        except DatabaseError as error:
            self.log.error(f"Database Error: {error}. Retrying using insert/update...")
            partial_func = partial(self._insert_or_update, db, table_name)
            tasks = executor.multithread(
                [(lambda row: partial_func(row), (row,), {}) for row in values]
            )
            tasks.await_results()  # Will raise an exception if any query fails
            return tasks.are_successful

    def _merge(
        self,
        database_handler: ETLDatabase,
        table_name: str,
        query: str,
        values: Iterator[dict[str, Any]] | list[dict[str, Any]],
        force_nvarchar: bool = False,
        batch_size: int = 1000,
    ) -> bool:
        """
        ✍🏾 Private method to merge data into the mssql database using a temporary table.

        Args:
            database_handler (ETLDatabase): The database handler to use for executing
                queries.
            query (str): The query to execute.
            values (Iterator[dict[str, Any]] | list[dict[str, Any]]): The values to merge.

        Returns:
            bool: True if the data was merged successfully.
        """
        from uuid import uuid4

        temp_table = f"temp_{table_name}_{str(uuid4())[:8]}".replace("-", "_")
        merge_query = query.format(source_table=temp_table)

        try:
            # Create the temporary table
            database_handler.create_table(
                table_name=temp_table,
                keys=[str(_) for _ in self._data.index.names],
                columns=database_handler.get_table_data_types(
                    self._data.reset_index(), force_nvarchar
                ),
            )

            # Insert the data into the temporary table
            temp_query, temp_values = database_handler.generate_prepared_insert_query(
                temp_table, self._data, force_nvarchar
            )
            database_handler.execute_write_batch_query(
                temp_query, temp_values, batch_size
            )

            """
            ✍🏾 Merge the data into the target table

            NOTE: Since the data is already in the temporary table, we can just
            execute the merge query without values.
            """
            return database_handler.execute_write_query(
                merge_query, check_affected_rows=True
            )

        except DatabaseError as error:
            # If the merge fails, retry using insert/update
            self.log.error(f"Database Error: {error}. Retrying using insert/update...")
            for value in values:
                self._insert_or_update(database_handler, table_name, value)
            return True

        finally:
            # Drop the temporary table
            database_handler.drop_table(temp_table)

    def _insert_or_update(
        self, database_handler: ETLDatabase, table_name: str, record: dict[str, Any]
    ) -> bool:
        """
        ✍🏾 Private method to insert/update a single record into the database.

        This method tries to insert the record into the database first, and if
        that fails, it retries using an update query.

        Args:
            database_handler (DatabaseHandler): The handler for the database.
            table_name (str): The name of the table.
            record (dict[str, Any]): The record to insert or update.
        """
        insert_query, values = database_handler.generate_prepared_insert_query(
            table_name, DataFrame([record])
        )
        update_query, _ = database_handler.generate_prepared_update_query(
            table_name, DataFrame([record])
        )

        results: list[bool] = []
        for value in values:
            try:
                results.append(database_handler.execute_write_query(insert_query, value))
            except DatabaseError as error:
                self.log.error(f"Database Error: {error}. Retrying using update...")
                results.append(database_handler.execute_write_query(update_query, value))
        return all(results)


__all__: list[str] = ["ETL"]
