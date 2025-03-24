from abc import ABC, abstractmethod
from re import IGNORECASE, match, search, sub
from typing import Any, Iterator, Literal, Optional

from loguru import logger
from tqdm import tqdm

from scriptman.core.config import config
from scriptman.powers.time_calculator import TimeCalculator


class DatabaseHandler(ABC):
    def __init__(
        self,
        driver: str,
        server: str,
        database: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        🚀 Initializes the DatabaseHandler class.

        Args:
            driver (str): The driver for the database.
            server (str): The server for the database.
            database (str): The database for the database.
            port (Optional[int], optional): The port for the database. Defaults to None.
            username (Optional[str], optional): The username for the database. Defaults
                to None.
            password (Optional[str], optional): The password for the database. Defaults
                to None.
        """
        super().__init__()
        self.port = port
        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.log = logger.bind(database=self.database, handler=self.__class__.__name__)

    @property
    def database_name(self) -> str:
        """
        🆔 Get the name of the database.

        Returns:
            str: The name of the database.
        """
        return self.database

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
    def execute_write_bulk_query(
        self, query: str, rows: list[dict[str, Any]] = []
    ) -> bool:
        """
        📃 Executes multiple SQL insert queries with the given SQL query and rows.

        NOTE: This method should be used for INSERT/UPDATE queries only; and is best used
        with prepared queries.

        Args:
            query (str): The SQL query to execute for each row.
            rows (list[dict[str, Any]], optional): The list of rows to insert.

        Returns:
            bool: True if the queries were executed successfully, False otherwise.
        """
        pass

    @abstractmethod
    def execute_write_batch_query(
        self,
        query: str,
        rows: Iterator[dict[str, Any]] | list[dict[str, Any]],
        batch_size: int = config.settings.get("BATCH_SIZE", 1000),
    ) -> bool:
        """
        📃 Executes multiple SQL insert queries with the given SQL query and rows.

        NOTE: This method should be used for INSERT/UPDATE queries only; and is best used
        with prepared queries.

        Args:
            query (str): The SQL query to execute for each row.
            rows (Iterator[dict[str, Any]], list[dict[str, Any]]): The iterator of rows
                to insert.
            batch_size (int, optional): The number of rows to insert in each batch.
                Defaults to the BATCH_SIZE from the environment configuration.

        Returns:
            bool: True if the queries were executed successfully, False otherwise.
        """
        pass

    def table_exists(self, table_name: str) -> bool:
        """❓ Checks if the given table exists in the database."""
        query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :table_name"
        try:
            return bool(self.execute_read_query(query, {"table_name": table_name}))
        except Exception as e:
            self.log.error(f'Table "{table_name}" not found: {e}')
            return False

    def table_has_records(self, table_name: str) -> bool:
        """❓ Checks if the given table has records in the database."""
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
            if self.table_exists(table_name):
                self.log.warning(f'Table "{table_name}" already exists')
                return True

            if keys:
                for key in keys:
                    if key in columns and columns[key].upper() == "NVARCHAR(MAX)":
                        columns[key] = "NVARCHAR(255)"
                        self.log.debug(
                            f'Converting primary key "{key}" '
                            "from NVARCHAR(MAX) to NVARCHAR(255)"
                        )

            column_definitions = ", ".join(
                [
                    f'"{column_name}" {column_type}'
                    for column_name, column_type in columns.items()
                ]
            )

            if keys:
                column_definitions += f", PRIMARY KEY ({', '.join(keys)})"

            query = f'CREATE TABLE "{table_name}" ({column_definitions})'
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
        """🧹 Truncates the given table if it exists."""
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
        """🧹 Drops the given table if it exists."""
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
                assert search_result is not None, f"Invalid UPDATE query: {query}"
                column_names = [
                    column.split("=")[0].strip().strip('"')
                    for column in search_result.group(1).split(",")
                ]

                search_result = search(r"WHERE (.*)", query, IGNORECASE)
                assert search_result is not None, f"Invalid UPDATE query: {query}"
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
                assert match_result is not None, f"Invalid INSERT INTO query: {query}"
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
                assert match_result is not None, f"Invalid DELETE FROM query: {query}"
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

    def convert_named_placeholders_to_query(
        self, query: str, params: dict[str, Any]
    ) -> tuple[str, tuple[Any, ...]]:
        """
        Converts a SQL query with named placeholders (:name) into a query with question
        mark placeholders (?) and corresponding tuple of values.

        This is particularly useful when converting named parameter queries into a format
        suitable for drivers that use question mark placeholders (like pyodbc).

        Args:
            query (str): The SQL query with named placeholders (e.g., :name, :age)
            params (dict[str, Any]): Dictionary of parameter names and their values

        Returns:
            tuple[str, tuple[Any, ...]]: A tuple containing:
                - The query with ? placeholders
                - A tuple of values in the order of appearance

        Example:
            query = "SELECT * FROM users "
                    "WHERE name = :name "
                    "OR nickname = :name "
                    "AND age = :age"
            params = {"name": "John", "age": 25}

            Returns: ("SELECT * FROM users WHERE name = ? OR nickname = ? AND age = ?",
                    ("John", "John", 25))
        """
        # Find all named parameters in the query
        param_names = []
        current_pos = 0
        converted_query = query

        while True:
            # Find the next named parameter
            pos = converted_query.find(":", current_pos)
            if pos == -1:
                break

            # Extract parameter name
            end_pos = pos + 1
            while end_pos < len(converted_query) and (
                converted_query[end_pos].isalnum() or converted_query[end_pos] == "_"
            ):
                end_pos += 1

            param_name = converted_query[pos + 1 : end_pos]
            param_names.append(param_name)

            current_pos = end_pos

        # Replace named parameters with question marks
        for param_name in param_names:
            converted_query = converted_query.replace(f":{param_name}", "?")

        # Create tuple of values in order of appearance
        values = tuple(params[param_name] for param_name in param_names)
        return converted_query, values

    def convert_named_placeholders_to_bulk_query(
        self, query: str, params_list: list[dict[str, Any]]
    ) -> tuple[str, list[tuple[Any, ...]]]:
        """
        Converts a SQL query with named placeholders into a query with question mark
        placeholders and a list of value tuples for bulk execution.

        This is particularly useful when converting named parameter queries into a format
        suitable for batch operations with drivers that use question mark placeholders.

        Args:
            query (str): The SQL query with named placeholders (e.g., :name, :age)
            params_list (list[dict[str, Any]]): List of parameter dictionaries, each
                containing parameter names and their values

        Returns:
            tuple[str, list[tuple[Any, ...]]]: A tuple containing:
                - The query with ? placeholders
                - A list of value tuples in the order of appearance for each row

        Example:
            query = "INSERT INTO users (name, age) VALUES (:name, :age)"
            params_list = [
                {"name": "John", "age": 25},
                {"name": "Jane", "age": 30}
            ]

            Returns: (
                "INSERT INTO users (name, age) VALUES (?, ?)",
                [
                    ("John", 25),
                    ("Jane", 30)
                ]
            )

        Raises:
            ValueError: If any dictionary in params_list is missing required parameters
        """
        if not params_list:
            return query, []

        # Get the converted query and parameter order from the first set of params
        converted_query, _ = self.convert_named_placeholders_to_query(
            query, params_list[0]
        )

        # Extract parameter names in order of appearance
        param_names = []
        current_pos = 0
        original_query = query

        while True:
            pos = original_query.find(":", current_pos)
            if pos == -1:
                break

            end_pos = pos + 1
            while end_pos < len(original_query) and (
                original_query[end_pos].isalnum() or original_query[end_pos] == "_"
            ):
                end_pos += 1

            param_name = original_query[pos + 1 : end_pos]
            param_names.append(param_name)
            current_pos = end_pos

        # Create list of value tuples
        values_list = []
        for params in tqdm(params_list, desc="Converting parameters"):
            # Verify all required parameters are present
            missing_params = set(param_names) - set(params.keys())
            if missing_params:
                raise ValueError(
                    f"Missing required parameters in one or more rows: {missing_params}"
                )

            # Create tuple of values in the correct order
            values = tuple(params[param_name] for param_name in param_names)
            values_list.append(values)

        return converted_query, values_list

    def validate_query_parameterization(
        self, query: str
    ) -> tuple[bool, Literal["named", "question_mark", "none"]]:
        """
        Validates the parameterization style of a SQL query and performs additional
        safety checks.

        Args:
            query (str): The SQL query to validate

        Returns:
            tuple[bool, Literal["named", "question_mark", "none"]]: A tuple containing:
                - Boolean indicating if the query is parameterized
                - Literal indicating the parameter style ('named', 'question_mark', or
                    'none')

        Raises:
            ValueError: If the query contains mixed parameter styles or other validation
                errors
        """
        # Initialize counters and collections
        string_literal = False
        string_delimiter = ""
        named_params = set()
        question_marks = 0
        current_pos = 0

        while current_pos < len(query):
            char = query[current_pos]

            # Handle string literals to avoid false positives
            if char in ["'", '"']:
                if not string_literal:
                    string_literal = True
                    string_delimiter = char
                elif char == string_delimiter:
                    string_literal = False
                current_pos += 1
                continue

            # Skip contents within string literals
            if string_literal:
                current_pos += 1
                continue

            # Check for named parameters
            if char == ":":
                # Validate it's actually a parameter and not just a colon
                if current_pos + 1 < len(query) and (
                    query[current_pos + 1].isalnum() or query[current_pos + 1] == "_"
                ):
                    end_pos = current_pos + 1
                    while end_pos < len(query) and (
                        query[end_pos].isalnum() or query[end_pos] == "_"
                    ):
                        end_pos += 1
                    param_name = query[current_pos + 1 : end_pos]
                    named_params.add(param_name)
                    current_pos = end_pos
                    continue

            # Check for question marks
            if char == "?":
                question_marks += 1

            current_pos += 1

        # Perform validation checks
        has_named = bool(named_params)
        has_question_marks = bool(question_marks)

        # Check for mixed parameter styles
        if has_named and has_question_marks:
            raise ValueError(
                "Mixed parameter styles detected. Query contains both named parameters "
                f"({named_params}) and question marks ({question_marks} found)"
            )

        # Check for potential SQL injection vulnerabilities
        dangerous_patterns = [
            "EXEC ",
            "EXECUTE ",
            "sp_",
            "xp_",  # Stored procedures
            "INTO OUTFILE",
            "INTO DUMPFILE",  # File operations
            # ";",  # Multiple statement execution
            # "--", # Comments that might be used maliciously
            # "/*",  # Comments that might be used maliciously
            # "*/",  # Comments that might be used maliciously
        ]

        for pattern in dangerous_patterns:
            if pattern.lower() in query.lower():
                raise ValueError(
                    f"Potentially unsafe SQL pattern detected: '{pattern}'. "
                    "Please use parameterized queries for any user input"
                )

        # Validate unclosed string literals
        if string_literal:
            raise ValueError("Query contains unclosed string literal")

        # Determine parameterization style
        if has_named:
            return True, "named"
        elif has_question_marks:
            return True, "question_mark"
        else:
            # Additional check for queries that should be parameterized
            potentially_unsafe = any(
                operator in query.upper()
                for operator in ["LIKE", "IN", "=", "<", ">", "<=", ">=", "<>"]
            )
            if potentially_unsafe:
                self.log.warning(
                    "Query contains comparison operators but no parameters. "
                    "Consider using parameterized queries for better security"
                )
            return False, "none"

    def __del__(self) -> None:
        """
        🧹 Destructor to disconnect from the database when the instance is
        destroyed.
        """
        self.disconnect()
