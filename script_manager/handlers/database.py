import re
from typing import List

import pyodbc

from script_manager.handlers.logs import LogHandler, LogLevel


class DatabaseHandler:
    def __init__(self, db_connection_string: str) -> None:
        """
        Initializes the DatabaseHandler class.

        Args:
            db_connection_string (str): The connection string for the database.
        """
        self._log = LogHandler(self._extract_db_name(db_connection_string))
        self._db_connection_string = db_connection_string
        self._connection = None
        self.connect()

    def connect(self):
        """
        Connects to the database using the connection string provided.
        """
        try:
            self._connection = pyodbc.connect(self._db_connection_string)
            self._log.message("Successfully connected to the database")
        except pyodbc.Error as error:
            self._log.message(
                level=LogLevel.ERROR,
                message="Unable to connect to the database",
                details={
                    "Error": str(error),
                    "Connection String": self._db_connection_string,
                },
            )

    def disconnect(self):
        """
        Closes the database connection if there was a connection.
        """
        if self._connection is not None:
            self._connection.close()
            self._log.message("Disconnected from the database")

    def execute_read_query(
        self,
        query: str,
        params: tuple = (),
    ) -> List[pyodbc.Row]:
        """
        Executes the given SQL query with optional parameters and returns the
        results as a list of tuples.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters for the query.
                Defaults to ().

        Returns:
            List[pyodbc.Row]: The results of the query as a list of tuples.
        """
        if self._connection is None:
            return []

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except pyodbc.Error as error:
            self._log.message(
                level=LogLevel.ERROR,
                message="Unable to execute read query",
                details={
                    "Error Message": str(error),
                    "Query Used": query,
                    "Params Used": params,
                },
            )
            return []
        finally:
            cursor.close()

    def execute_write_query(
        self,
        query: str,
        params: tuple = (),
        check_affected_rows: bool = False,
    ) -> bool:
        """
        Executes the given SQL query with optional parameters and commits the
        transaction.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters for the query.
                Defaults to ().
            check_affected_rows (bool, optional): When true, raises an error if
                no row was affected.
                Defaults to False.

        Returns:
            bool: True if the query was executed successfully, False otherwise.
        """
        if self._connection is None:
            return False

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            self._connection.commit()
            if check_affected_rows and cursor.rowcount == 0:
                raise ValueError("No rows were affected by the query.")
            return True
        except pyodbc.Error as error:
            self._log.message(
                level=LogLevel.ERROR,
                message="Unable to execute write query",
                details={
                    "Error Message": str(error),
                    "Query Used": query,
                    "Params Used": params,
                },
            )
            return False
        finally:
            cursor.close()

    def execute_many(self, query: str, rows: List[tuple]) -> bool:
        """
        Executes multiple insert queries with the given SQL query and rows.

        Args:
            query (str): The SQL query to execute for each row.
            rows (List[tuple]): The rows to insert, where each row is a tuple.

        Returns:
            bool: True if the queries were executed successfully,
                False otherwise.
        """
        if self._connection is None:
            return False

        cursor = self._connection.cursor()
        try:
            cursor.fast_executemany = True
            cursor.executemany(query, rows)
            self._connection.commit()
            self._log.message("Executed Bulk Query Successfully.")
            return True
        except pyodbc.Error as error:
            self._log.message(
                level=LogLevel.ERROR,
                message="Unable to bulk execute query",
                details={
                    "Error Message": str(error),
                    "Query Used": query,
                    "Rows Used": rows,
                },
            )
            return False
        finally:
            cursor.close()

    def create_table(self, query: str) -> bool:
        """
        Creates a new table with the given SQL query if it doesn't exist.

        Args:
            query (str): The SQL query to create the table.

        Returns:
            bool: True if the table was created, False otherwise.
        """
        table = self._extract_table_name(query)
        if not self.table_exists(table):
            self.execute_write_query(query)
            self._log.message(f"Table [{table}] created")
            return True
        else:
            self._log.message(f"Table [{table}] Already Exists", LogLevel.WARN)
            return False

    def truncate_table(self, table_name: str) -> bool:
        """
        Truncates the given table if it exists.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table was truncated, False otherwise.
        """
        query = f"TRUNCATE TABLE {table_name}"
        try:
            return self.execute_write_query(query)
        finally:
            self._log.message(f"Table [{table_name}] truncated")

    def drop_table(self, table_name: str) -> bool:
        """
        Drops the given table if it exists.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table was dropped, False otherwise.
        """
        query = f"DROP TABLE {table_name}"
        try:
            return self.execute_write_query(query)
        finally:
            self._log.message(f"Table [{table_name}] dropped")

    def table_exists(self, table: str) -> bool:
        """
        Checks if the table exists in the database.

        Args:
            table (str): The table's name.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        if self._connection is None:
            return False
        cursor = self._connection.cursor()
        try:
            query = """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = ?;
            """
            cursor.execute(query, (table,))
            fetched = cursor.fetchone()
            count = fetched[0] if fetched is not None else 0
            return count > 0
        except Exception:
            return False
        finally:
            cursor.close()

    def table_has_records(self, table_name: str) -> bool:
        """
        Checks if a table has records by selecting the first record.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table has records, False otherwise.
        """
        if self._connection is None:
            return False
        cursor = self._connection.cursor()
        try:
            query = f"SELECT COUNT(*) FROM [{table_name}];"
            cursor.execute(query)
            fetched = cursor.fetchone()
            count = fetched[0] if fetched is not None else 0
            return count > 0
        except Exception:
            return False
        finally:
            cursor.close()

    def _extract_table_name(self, query: str) -> str:
        """
        Extracts the name of the table from the given SQL query.

        Args:
            query (str): The SQL query.

        Returns:
            str: The name of the table.
        """
        pattern = r"(?:TABLE|INTO|FROM|UPDATE)\s+([^\s,;()]+)"
        match = re.search(pattern, query, re.IGNORECASE)
        table_name = (match.group(1) if match else "").replace('"', "")
        return table_name

    def _extract_db_name(self, connection_string: str) -> str:
        """
        Extracts the name of the database from the given database connection
        string.

        Args:
            connection_string (str): The Database Connection String.

        Returns:
            str: The name of the database, or 'Database Handler' otherwise.
        """
        match = re.search(r"Database=([^;]+)", connection_string)
        return match.group(1) if match else "Database Handler"

    def __del__(self) -> None:
        self.disconnect()
