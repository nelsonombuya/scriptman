try:
    from contextlib import contextmanager
    from functools import partial
    from pathlib import Path
    from typing import Any, Callable, Generator, Literal, Optional

    from loguru import Logger, logger
    from pandas import DataFrame

    from scriptman.powers.database._exceptions import DatabaseError
    from scriptman.powers.etl._database import ETLDatabase, ETLDatabaseInterface
    from scriptman.powers.executor import TaskExecutor
    from scriptman.powers.time_calculator import TimeCalculator
except ImportError:
    raise ImportError(
        "Pandas is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[etl]."
    )

ETL_TYPES = DataFrame | list[dict[str, Any]] | list[tuple[Any, ...]]


class ETL:
    """🔍 Data processing utility for Extract, Transform, Load operations."""

    log: Logger = logger

    def __init__(self, data: Optional[ETL_TYPES] = None) -> None:
        """
        🚀 Initialize ETL with optional data.

        Args:
            data (Optional[DataFrame | list[dict[str, Any]]]): The data to initialize
                the ETL object with.
        """
        # Delegate DataFrame properties and methods
        self._data = DataFrame(data) if data is not None else DataFrame()
        self.__getitem__ = self._data.__getitem__
        self.set_index = self._data.set_index
        self.__len__ = self._data.__len__
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

    """
    🔍 Context managers
    """

    @contextmanager
    @classmethod
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
        operation_str = f"{operation} from {context}" if operation else context
        exception: Optional[Exception] = None

        try:
            with TimeCalculator.context(context):
                cls.log.info(f"Data {operation_str} started...")
                yield
        except Exception as error:
            exception = error
        finally:
            if exception:
                logger.error(f"Error during {operation_str}: {exception}")
                raise exception

            if operation == "extraction":
                num_records = len(cls()) if isinstance(cls, type) else len(cls)
                if num_records > 0:
                    cls.log.debug(f"Number of records extracted: {num_records}")
                    cls.log.debug(f"Extracted data: {cls}")
                else:
                    cls.log.warning("No records were extracted.")

            elif operation == "transformation":
                cls.log.debug(f"Transformed data: {cls}")

            elif operation == "loading":
                cls.log.success(f"Data loaded to {operation_str}.")

            else:
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
        db: ETLDatabaseInterface,
        query: str,
        params: dict[str, Any] = {},
    ) -> "ETL":
        """
        📂 Extract data from a database using a provided query.

        Args:
            db (ETLDatabaseInterface): The handler to manage the database connection.
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
    def from_extractor(cls, extractor: Callable[[], ETL_TYPES]) -> "ETL":
        """
        ⚙ Extract data using a custom extractor function.

        Args:
            extractor: A function that implements the extraction logic

        Returns:
            ETL: The extracted data as an ETL object
        """
        with cls.timed_context(extractor.__name__, "extraction"):
            return cls(extractor())

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

    def flatten(self, column: str, separator: str = "_") -> "ETL":
        """
        🔍 Flatten a nested dictionary column into separate columns.

        This method takes a column containing nested dictionaries and flattens it,
        creating new columns for each nested key. The new column names are created
        by combining the original column name with the nested keys, separated by
        the specified separator.

        Args:
            column (str): The name of the column containing nested dictionaries.
            separator (str, optional): The separator to use between the original column
                name and nested keys. Defaults to "_".

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
            flattened = json_normalize(nested_data, sep=separator)
            flattened.columns = [
                f"{column}{separator}{col}" for col in flattened.columns  # type: ignore
            ]

            # Combine with original DataFrame (excluding the original nested column)
            result = df.drop(columns=[column])
            for col in flattened.columns:
                result[col] = flattened[col].values

            return ETL(result)

    """
    🔍 Loading methods
    """

    def to_dataframe(self) -> DataFrame:
        """
        🔍 Convert the ETL object to a DataFrame.
        """
        return self._data

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
            self._data.to_csv(file_path, index=False)
            self.log.success(f"Data saved to {file_path}")
            return file_path

    def to_json_file(self, file_path: str | Path) -> Path:
        """
        📃 Saves the data to a JSON file using the given file path.

        Args:
            file_path (str | Path): The path to the file to save the data to.

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
            self._data.to_json(file_path, orient="records")
            self.log.success(f"Data saved to {file_path}")
            return file_path

    def to_db(
        self,
        db: ETLDatabaseInterface,
        table_name: str,
        batch_execute: bool = True,
        force_nvarchar: bool = False,
        batch_size: Optional[int] = None,
        method: Literal["truncate", "replace", "insert", "update", "upsert"] = "upsert",
    ) -> bool:
        """
        📂 Loads the ETL data into a database table, with options for batch execution and
        different loading methods (truncate, replace, upsert, insert, update).

        NOTE: To have the database be created/inserted/updated with the correct keys,
        ensure that the indices are defined in the dataset using the `set_index` method.

        Args:
            db (ETLDatabaseInterface): The database handler to use for executing queries.
            table_name (str): The name of the table to load the data into.
            batch_execute (bool, optional): Whether to execute queries in batches.
                Defaults to True.
            force_nvarchar (bool, optional): Whether to force the use of NVARCHAR data
                types. Defaults to False.
            batch_size (Optional[int], optional): The number of rows to include in each
                batch. Defaults to None.
            method (Literal["truncate", "replace", "insert", "update", "upsert"]):
                The loading method to use. Defaults to "upsert".

        Raises:
            ValueError: If the dataset is empty or if bulk execute is disabled.
            DatabaseError: If a database error occurs during execution.

        Returns:
            bool: True if the data was loaded successfully.
        """
        # Wrap the handler with ETLDatabase for extended functionality
        db = ETLDatabase(db)
        table_exists: bool = db.table_exists(table_name)

        if self.empty:
            self.log.warning("Dataset is empty!")
            raise ValueError("Dataset is empty!")

        if method == "truncate" and table_exists:
            db.truncate_table(table_name)

        if method == "replace" and table_exists:
            db.drop_table(table_name)

        if (method in {"upsert", "update"}) and self.index.empty:
            message = (
                "Dataset has no index! "
                "Please set the index using the `set_index` method."
            )
            self.log.error(message)
            raise ValueError(message)

        if not table_exists:
            self.log.warning(f'Table "{table_name}" does not exist. Creating table...')
            db.create_table(
                table_name=table_name,
                keys=self._data.index.names,
                columns=db.get_table_data_types(self._data, force_nvarchar),
            )

        query, values = {
            "insert": db.generate_prepared_insert_query,
            "update": db.generate_prepared_update_query,
            "upsert": db.generate_prepared_upsert_query,
        }.get(method, db.generate_prepared_upsert_query)(table_name, self._data)

        try:
            if not batch_execute:
                raise ValueError("Bulk Execute is disabled.")
            return db.execute_write_batch_query(query, values, batch_size)

        except (MemoryError, ValueError) as error:
            if not batch_execute:
                self.log.info("Bulk Execute is disabled. Executing single queries...")
            else:
                self.log.warning(f"Bulk Execution Failed: {error}")
                self.log.warning("Executing single queries...")

            return all(
                TaskExecutor[bool]()
                .parallel_io_bound_task(
                    func=db.execute_write_query,
                    args=[(query, row) for row in values],
                )
                .results
            )

        except DatabaseError as error:
            self.log.error(f"Database Error: {error}. Retrying using insert/update...")
            partial_func = partial(self._insert_or_update, db, table_name)
            return all(
                TaskExecutor[bool]()
                .parallel_io_bound_task(
                    func=lambda record: partial_func(record),
                    args=[(record,) for record in values],
                )
                .results
            )

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


__all__: list[str] = ["ETL", "ETLDatabaseInterface"]
