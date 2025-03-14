from abc import ABC, abstractmethod
from re import IGNORECASE, match, search, sub
from typing import Any, Optional

from loguru import logger
from tqdm import tqdm

from scriptman.core.config import config
from scriptman.powers.database._config import DatabaseConfig
from scriptman.powers.time_calculator import TimeCalculator


class DatabaseHandler(ABC):
    def __init__(self, config: DatabaseConfig) -> None:
        """
        🚀 Initializes the DatabaseHandler class.

        Args:
            connection_string (str): The connection string for the database.
        """
        super().__init__()
        self.config = config
        self.log = logger.bind(
            database=self.database_name,
            handler=self.__class__.__name__,
        )

    @property
    def database_name(self) -> str:
        """
        🆔 Get the name of the database.

        Returns:
            str: The name of the database.
        """
        return self.config.database

    @property
    @abstractmethod
    def database_type(self) -> str:
        """
        🆔 Get the type of the database.

        Returns:
            str: The type of the database.
        """
        pass

    @property
    @abstractmethod
    def connection_string(self) -> str:
        """
        ✍🏾 Get the connection string for the database.

        NOTE: This should convert the database config into an appropriate connection
        string for the handler.

        Returns:
            str: The connection string for the database.
        """
        pass

    @abstractmethod
    def connect(self) -> bool:
        """
        🔗 Connects to the database using the connection string provided.

        Raises:
            DatabaseError: If a connection to the database cannot be established.
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """
        🛑 Closes the database connection if there was a connection.

        Raises:
            DatabaseError: If there was an error disconnecting from the database.
        """
        pass

    @abstractmethod
    def execute_read_query(
        self, query: str, params: dict[str, Any] = {}
    ) -> list[dict[str, Any]]:
        """
        📖 Executes the given SQL query with optional parameters and returns the
        results as a list of dictionaries.

        NOTE: This method should be used for SELECT queries only; and is best used with
        prepared queries.

        Args:
            query (str): The SQL query to execute.
            params (dict[str, Any], optional): The parameters for the query.

        Returns:
            list[dict[str, Any]]: The results of the query as a list of dictionaries.
        """
        pass

    @abstractmethod
    def execute_write_query(
        self,
        query: str,
        params: dict[str, Any] = {},
        check_affected_rows: bool = False,
    ) -> bool:
        """
        ✍🏾 Executes the given SQL query with optional parameters and commits the
        transaction.

        NOTE: This method should be used for INSERT, UPDATE, DELETE, and other write
        queries only; and is best used with prepared queries.

        Args:
            query (str): The SQL query to execute.
            params (dict[str, Any], optional): The parameters for the query.
            check_affected_rows (bool, optional): When true, raises a DatabaseError if no
                row was affected. Defaults to False.

        Returns:
            bool: True if the query was executed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def execute_write_batch_query(
        self,
        query: str,
        rows: list[dict[str, Any]] = [],
        batch_size: Optional[int] = config.settings.get("BATCH_SIZE"),
    ) -> bool:
        """
        📃 Executes multiple SQL insert queries with the given SQL query and rows.

        NOTE: This method should be used for INSERT/UPDATE queries only; and is best used
        with prepared queries.

        Args:
            query (str): The SQL query to execute for each row.
            rows (list[dict[str, Any]], optional): The list of rows to insert.
            batch_size (Optional[int], optional): The number of rows to insert in each
                batch. Defaults to the BATCH_SIZE from the environment configuration.

        Returns:
            bool: True if the queries were executed successfully, False otherwise.
        """
        pass

    def table_exists(self, table_name: str) -> bool:
        """
        ❓ Checks if the given table exists in the database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        query = (
            "SELECT * "
            "FROM information_schema.tables "
            f'WHERE table_name = "{table_name}"'
        )
        try:
            return bool(self.execute_read_query(query))
        except Exception as e:
            self.log.error(f'Table "{table_name}" not found: {e}')
            return False

    def table_has_records(self, table_name: str) -> bool:
        """
        ❓ Checks if the given table has records in the database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table has records, False otherwise.
        """
        query = f'SELECT * FROM "{table_name}" LIMIT 1'
        return bool(self.execute_read_query(query))

    def create_table(
        self, table_name: str, columns: dict[str, str], keys: Optional[list[str]] = None
    ) -> bool:
        """
        🔨 Creates a table with the given name and columns.

        Args:
            table_name (str): The name of the table.
            columns (dict[str, str]): A dictionary of column names and their data types.
            keys (Optional[list[str]]): A list of column names to set as the primary key.

        Returns:
            bool: True if the table was created, False otherwise.
        """
        try:
            column_definitions = ", ".join(
                [
                    f"{column_name} {column_type}"
                    for column_name, column_type in columns.items()
                ]
            )

            if keys:
                column_definitions += f", PRIMARY KEY ({', '.join(keys)})"

            query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_definitions})'
            result = self.execute_write_query(query)
            if result:
                self.log.info(f'Table "{table_name}" created')
                return result
            else:
                self.log.error(f'Table "{table_name}" not created')
                return result
        except Exception as e:
            self.log.error(f'Table "{table_name}" not created: {e}')
            return False

    def truncate_table(self, table_name: str) -> bool:
        """
        🧹 Truncates the given table if it exists.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table was truncated, False otherwise.
        """
        try:
            result = self.execute_write_query(f'TRUNCATE TABLE "{table_name}"')
            if result:
                self.log.info(f'Table "{table_name}" truncated')
                return result
            else:
                self.log.error(f'Table "{table_name}" not truncated')
                return result
        except Exception as e:
            self.log.error(f'Table "{table_name}" not truncated: {e}')
            return False

    def drop_table(self, table_name: str) -> bool:
        """
        🧹 Drops the given table if it exists.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table was dropped, False otherwise.
        """
        try:
            result = self.execute_write_query(f'DROP TABLE IF EXISTS "{table_name}"')
            if result:
                self.log.info(f'Table "{table_name}" dropped')
                return result
            else:
                self.log.error(f'Table "{table_name}" not dropped')
                return result
        except Exception as e:
            self.log.error(f'Table "{table_name}" not dropped: {e}')
            return False

    def convert_query_to_named_placeholders(
        self, query: str, values: list[tuple[Any, ...]]
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        Reverse engineers a prepared SQL query (INSERT, UPDATE, DELETE) and its values
        into a list of dictionaries and its corresponding query with named placeholders.

        This is particularly useful when using a query with "?" for placeholders (like in
        pyodbc prepared queries).

        Args:
            query (str): The SQL query to reverse engineer.
            values (list[tuple]): The list of value tuples corresponding to the query.

        Returns:
            tuple[str, list[dict]]: A tuple containing the query with placeholders and
                the list of dictionaries where the keys are the column names and
                index names and the values are the corresponding values for each row.
        """
        with TimeCalculator.context("Reverse Engineering Query"):
            column_names: list[str] = []
            result: list[dict[str, Any]] = []

            if query.strip().upper().startswith("UPDATE"):  # UPDATE QUERY
                search_result = search(r"SET (.*?) WHERE", query, IGNORECASE)
                assert search_result is not None, "Invalid UPDATE query"
                column_names = [
                    column.split("=")[0].strip().strip('"')
                    for column in search_result.group(1).split(",")
                ]

                search_result = search(r"WHERE (.*)", query, IGNORECASE)
                assert search_result is not None, "Invalid UPDATE query"
                where_clause = search_result.group(1)
                index_names = [
                    column.split("=")[0].strip().strip('"')
                    for column in where_clause.split("AND")
                ]

                for value_tuple in tqdm(values, desc="Processing rows"):
                    row_dict = {}
                    for i, column_name in enumerate(column_names):
                        row_dict[column_name] = value_tuple[i]
                    for i, index_name in enumerate(index_names):
                        row_dict[index_name] = value_tuple[len(column_names) + i]
                    result.append(row_dict)

                column_names += index_names

            elif query.strip().upper().startswith("INSERT INTO"):  # INSERT QUERY
                match_result = match(
                    r'INSERT INTO "([^"]+)" \(([^)]+)\) VALUES \(([^)]+)\)',
                    query.strip(),
                    IGNORECASE,
                )
                assert match_result is not None, "Invalid INSERT INTO query"
                column_names = [
                    column.strip().strip('"')
                    for column in match_result.group(2).split(",")
                ]

                for value_tuple in tqdm(values, desc="Processing rows"):
                    row_dict = {}
                    for i, column_name in enumerate(column_names):
                        row_dict[column_name] = value_tuple[i]
                    result.append(row_dict)

            elif query.strip().upper().startswith("DELETE FROM"):  # DELETE QUERY
                match_result = match(
                    r'DELETE FROM "([^"]+)" WHERE (.*)', query.strip(), IGNORECASE
                )
                assert match_result is not None, "Invalid DELETE FROM query"
                column_names = [
                    column.split("=")[0].strip().strip('"')
                    for column in match_result.group(2).split("AND")
                ]

                for value_tuple in tqdm(values, desc="Processing rows"):
                    row_dict = {}
                    for i, column_name in enumerate(column_names):
                        row_dict[column_name] = value_tuple[i]
                    result.append(row_dict)

            for column_name in tqdm(column_names, desc="Converting placeholders"):
                query = sub(r"\?", f":{column_name}", query, count=1)
            return query, result

    def __del__(self) -> None:
        """
        🧹 Destructor to disconnect from the database when the instance is
        destroyed.
        """
        self.disconnect()
