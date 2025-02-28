try:
    from contextlib import contextmanager
    from pathlib import Path
    from typing import Any, Generator, Literal, Optional

    from loguru import Logger, logger
    from pandas import DataFrame, read_csv

    from scriptman.core.config import config
    from scriptman.powers.concurrency import TaskExecutor
    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.database._exceptions import DatabaseError
    from scriptman.powers.etl._extractor import DataExtractor
    from scriptman.powers.generics import T
    from scriptman.powers.time_calculator import TimeCalculator
except ImportError:
    raise ImportError(
        "Pandas is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[etl]."
    )


class ETL(DataFrame):
    log: Logger = logger
    default_downloads_dir: Path = Path(config.settings.downloads_dir)

    @contextmanager
    def extraction_context(
        self, context: str = "Code Block"
    ) -> Generator[None, None, None]:
        """
        📂 A context manager for data extraction processes, logging the start, completion,
        and details of the extracted data.

        Args:
            context (str): The name of the context for logging purposes.

        Yields:
            None: This is a generator function used as a context manager.

        Logs:
            - Info: When data extraction starts.
            - Success: When data extraction completes.
            - Debug: The number of records and extracted data if records are found.
            - Warning: If no records were extracted.
        """

        try:
            with TimeCalculator.context(context):
                self.log.info(f"Data extraction from {context} started...")
                yield
        finally:
            self.log.success(f"Data extraction from {context} complete.")
            num_records = len(self.data)
            if num_records > 0:
                self.log.debug(f"Number of records extracted: {num_records}")
                self.log.debug(f"Extracted data: {self.data}")
            else:
                self.log.warning("No records were extracted.")

    """
    Extraction Methods
    """

    def from_csv_file(self, file_name: str, directory: Optional[Path] = None) -> "ETL":
        """
        📃 Extract data from a CSV file.

        Args:
            file_name (str): The name of the CSV file to extract from.
            directory (Optional[Path], optional): The directory to search for the CSV
                file. Defaults to the environment variable DOWNLOADS_DIRECTORY.

        Raises:
            FileNotFoundError: If no files matched the pattern.

        Returns:
            ETL: The extracted data as an ETL object.

        Logs:
            - Info: When a CSV file is found.
            - Debug: The number of records and extracted data if records are found.
            - Warning: If no records were extracted.
        """

        files = self.default_downloads_dir.glob(f"{file_name}.csv")
        if not files:
            raise FileNotFoundError(f"No files matched the pattern: {file_name}.csv")

        with self.extraction_context("CSV"):
            csv_file = next(files)
            self.log.info(f"Found CSV File at {csv_file}...")
            self = ETL(read_csv(csv_file))
        return self

    def from_json_file(self, file_name: str, directory: Optional[Path] = None) -> "ETL":
        """
        📃 Extract data from a JSON file.

        Args:
            file_name (str): The name of the JSON file to extract from.
            directory (Optional[Path], optional): The directory to search for the JSON
                file. Defaults to the environment variable DOWNLOADS_DIRECTORY.

        Raises:
            FileNotFoundError: If no files matched the pattern.

        Returns:
            ETL: The extracted data as an ETL object.

        Logs:
            - Info: When a JSON file is found.
            - Debug: The number of records and extracted data if records are found.
            - Warning: If no records were extracted.
        """

        files = self.default_downloads_dir.glob(f"{file_name}.json")
        if not files:
            raise FileNotFoundError(f"No files matched the pattern: {file_name}.json")

        with self.extraction_context("JSON"):
            json_file = next(files)
            self.log.info(f"Found JSON File at {json_file}...")
            with open(json_file, "r", encoding="utf-8") as file:
                from json import load

                data = load(file)
            self = ETL(DataFrame(data))
        return self

    def from_db(
        self,
        database_handler: DatabaseHandler,
        query: str,
        params: dict[str, Any],
    ) -> "ETL":
        """
        📂 Extract data from a database using a provided query.

        Args:
            database_handler (DatabaseHandler): The handler to manage the database
                connection.
            query (str): The SQL query to execute for data extraction.
            params (dict[str, Any]): A dictionary of parameters to use in the query.

        Returns:
            ETL: The extracted data as an ETL object.

        Logs:
            - Context: "Database" extraction context.
        """
        with self.extraction_context("Database"):
            self = ETL(database_handler.execute_read_query(query, params))
        return self

    def from_extractor(self, extractor: DataExtractor[T]) -> "ETL":
        """
        ⚙ Extract data using a custom extractor implementation

        Args:
            extractor: An instance of DataExtractor that implements the extraction logic

        Returns:
            ETL: The extracted data as an ETL object
        """
        with self.extraction_context(extractor.__class__.__name__):
            data = extractor.extract()
            if isinstance(data, (list, DataFrame, dict, tuple)):
                self = ETL(data)
            else:
                raise ValueError(f"Unsupported data type from extractor: {type(data)}")
        return self

    """
    Loading Methods
    """

    def to_json_file(self, file_name: str, directory: Optional[Path] = None) -> Path:
        """
        📃 Saves the data to a JSON file using the given file name and directory.

        Args:
            file_name (str): The name of the file to save.
            directory (Path, optional): The directory to save the file in. Defaults to the
                DOWNLOADS_DIRECTORY.

        Returns:
            Path: The path to the saved file.

        Logs:
            - Warning: If the dataset is empty.
            - Success: The path to the saved file.
        """
        if self.empty:
            self.log.warning("Dataset is empty!")
            raise ValueError("Dataset is empty!")

        self.default_downloads_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.default_downloads_dir / f"{file_name}.json"
        self.to_json(file_path, orient="records")
        self.log.success(f"Data saved to {file_path}")
        return file_path

    def to_csv_file(self, file_name: str, directory: Optional[Path] = None) -> Path:
        """
        📃 Saves the data to a CSV file using the given file name and directory.

        Args:
            file_name (str): The name of the file to save.
            directory (Path, optional): The directory to save the file in. Defaults to the
                DOWNLOADS_DIRECTORY.

        Returns:
            Path: The path to the saved file.

        Logs:
            - Warning: If the dataset is empty.
            - Success: The path to the saved file.
        """
        if self.empty:
            self.log.warning("Dataset is empty!")
            raise ValueError("Dataset is empty!")

        self.default_downloads_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.default_downloads_dir / f"{file_name}.csv"
        self.to_csv(file_path, index=False)
        self.log.success(f"Data saved to {file_path}")
        return file_path

    def to_db(
        self,
        database_handler: DatabaseHandler,
        table_name: str,
        batch_execute: bool = True,
        force_nvarchar: bool = False,
        batch_size: Optional[int] = None,
        method: Literal["truncate", "replace", "insert", "update", "upsert"] = "upsert",
    ) -> bool:
        """
        📂 Loads the ETL data into a database table, with options for batch execution and
        different loading methods (truncate, replace, upsert).

        NOTE: To have the database be created/inserted/updated with the correct keys,
        ensure that the indices are defined in the dataset using the `set_index` method.

        Args:
            database_handler (DatabaseHandler): The database handler to use for executing
                queries.
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

        if self.empty:
            self.log.warning("Dataset is empty!")
            raise ValueError("Dataset is empty!")

        table_exists: bool = database_handler.table_exists(table_name)

        if method == "truncate" and table_exists:
            database_handler.truncate_table(table_name)

        if method == "replace" and table_exists:
            database_handler.drop_table(table_name)

        if (method == "upsert" or method == "update") and self.index.empty:
            message = (
                "Dataset has no index! "
                "Please set the index using the `set_index` method."
            )
            self.log.error(message)
            raise ValueError(message)

        if not table_exists:
            self.log.warning(f'Table "{table_name}" does not exist. Creating table...')
            database_handler.create_table(
                table_name=table_name,
                keys=self.data.index.names,
                columns=database_handler.get_table_data_types(self, force_nvarchar),
            )

        query, values = {
            "insert": database_handler.generate_prepared_insert_query,
            "update": database_handler.generate_prepared_update_query,
            "upsert": database_handler.generate_prepared_upsert_query,
        }.get(method, database_handler.generate_prepared_upsert_query)(table_name, self)

        try:
            if not batch_execute:
                raise ValueError("Bulk Execute is disabled.")
            database_handler.execute_write_batch_query(query, values, batch_size)
        except (MemoryError, ValueError) as error:
            self.log.warning(
                f"Bulk Query Execution Failed: {error}. Executing single queries..."
            )
            return all(
                TaskExecutor[bool]()
                .parallel_io_bound_task(
                    func=database_handler.execute_write_query,
                    args=[(query, row) for row in values],
                )
                .results
            )
        except DatabaseError as error:
            self.log.error(f"Database Error: {error}. Retrying using insert/update...")
            return all(
                TaskExecutor[bool]()
                .parallel_io_bound_task(
                    func=self._insert_or_update,
                    args=[(database_handler, table_name, value) for value in values],
                )
                .results
            )

        return True

    def _insert_or_update(
        self, database_handler: DatabaseHandler, table_name: str, record: dict[str, Any]
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


__all__: list[str] = ["ETL", "DataExtractor"]
