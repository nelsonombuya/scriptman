from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Optional

from loguru import Logger, logger
from pandas import DataFrame, read_csv

from scriptman.core.config import config
from scriptman.utils.concurrency import TaskExecutor
from scriptman.utils.database._database import DatabaseError, DatabaseHandler
from scriptman.utils.time_calculator import TimeCalculator


class ETL(DataFrame):
    log: Logger = logger

    @contextmanager
    def extraction_context(self, context_name: str = "Code Block"):
        """
        A context manager for data extraction processes, logging the start, completion,
        and details of the extracted data.

        Args:
            context_name (str): The name of the context for logging purposes.

        Yields:
            None: This is a generator function used as a context manager.

        Logs:
            - Info: When data extraction starts.
            - Success: When data extraction completes.
            - Debug: The number of records and extracted data if records are found.
            - Warning: If no records were extracted.
        """

        try:
            with TimeCalculator.time_context_manager(context_name):
                self.log.info(f"Data extraction from {context_name} started...")
                yield
        finally:
            self.log.success(f"Data extraction from {context_name} complete.")
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
        Extract data from a CSV file.

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

        directory = directory or Path(config.env.downloads_dir)
        files = directory.glob(f"{file_name}.csv")
        if not files:
            raise FileNotFoundError(f"No files matched the pattern: {file_name}.csv")

        with self.extraction_context("CSV"):
            csv_file = next(files)
            self.log.info(f"Found CSV File at {csv_file}...")
            self = ETL(read_csv(csv_file))
        return self

    def from_json_file(self, file_name: str, directory: Optional[Path] = None) -> "ETL":
        """
        Extract data from a JSON file.

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

        directory = directory or Path(config.env.downloads_dir)
        files = directory.glob(f"{file_name}.json")
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
        with self.extraction_context("Database"):
            self = ETL(database_handler.execute_read_query(query, params))
        return self

    """
    Loading Methods
    """

    def to_json_file(self, file_name: str, directory: Optional[Path] = None) -> Path:
        """
        Saves the data to a JSON file using the given file name and directory.

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

        directory = directory or Path(config.env.downloads_dir)
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / f"{file_name}.json"
        self.to_json(file_path, orient="records")
        self.log.success(f"Data saved to {file_path}")
        return file_path

    def to_csv_file(self, file_name: str, directory: Optional[Path] = None) -> Path:
        """
        Saves the data to a CSV file using the given file name and directory.

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

        directory = directory or Path(config.env.downloads_dir)
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / f"{file_name}.csv"
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
        method: Literal["truncate", "replace", "upsert"] = "upsert",
    ) -> bool:
        """
        Loads the ETL data into a database table, with options for batch execution and
        different loading methods (truncate, replace, upsert).

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
            method (Literal["truncate", "replace", "upsert"], optional): The method to use
                for loading data (truncate, replace, upsert). Defaults to "upsert".

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

        elif method == "replace" and table_exists:
            database_handler.drop_table(table_name)

        if not table_exists:
            self.log.warning(f'Table "{table_name}" does not exist.')
            database_handler.create_table(
                table_name=table_name,
                keys=self.data.index.names,
                columns=database_handler.get_table_data_types(self, force_nvarchar),
            )

        query, values = database_handler.generate_prepared_upsert_query(table_name, self)

        try:
            if not batch_execute:
                raise ValueError("Bulk Execute is disabled.")
            database_handler.execute_write_batch_query(query, values, batch_size)
        except (MemoryError, ValueError) as error:
            self.log.warning(
                f"Bulk Query Execution Failed: {error}. Executing single queries..."
            )
            TaskExecutor.run_async(
                TaskExecutor().parallel_io_bound_task(
                    func=database_handler.execute_write_query,
                    args=[(query, row) for row in values],
                )
            )
        except DatabaseError as error:
            self.log.error(f"Database Error: {error}. Retrying using insert/update...")
            TaskExecutor.run_async(
                TaskExecutor().parallel_io_bound_task(
                    func=self._upsert,
                    args=[(database_handler, table_name, value) for value in values],
                )
            )

        return True

    def _upsert(
        self, database_handler: DatabaseHandler, table_name: str, record: dict[str, Any]
    ) -> None:
        """
        Private method to upsert a single record into the database.

        This method tries to insert the record into the database first, and if
        that fails, it retries using an update query.

        Args:
            database_handler (DatabaseHandler): The handler for the database.
            table_name (str): The name of the table.
            record (dict[str, Any]): The record to upsert.
        """
        insert_query, values = database_handler.generate_prepared_insert_query(
            table_name, DataFrame([record])
        )
        update_query, _ = database_handler.generate_prepared_update_query(
            table_name, DataFrame([record])
        )

        for value in values:
            try:
                database_handler.execute_write_query(insert_query, value)
            except DatabaseError as error:
                self.log.error(f"Database Error: {error}. Retrying using update...")
                database_handler.execute_write_query(update_query, value)
