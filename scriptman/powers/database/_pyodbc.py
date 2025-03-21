try:
    from contextlib import contextmanager
    from functools import partial
    from queue import Queue
    from threading import Lock
    from typing import Any, Generator, Optional

    from pyodbc import Connection, Cursor, Error, Row, connect
    from tqdm import tqdm

    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.database._exceptions import DatabaseError
except ImportError:
    raise ImportError(
        "pyodbc is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[pyodbc]."
    )


class PyODBCHandler(DatabaseHandler):
    _connection_pool: Queue[Connection] = Queue()
    _pool_lock: Lock = Lock()

    def __init__(
        self,
        driver: str,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int] = None,
        max_pool_size: int = 8,
    ) -> None:
        """
        🚀 Initializes the PyODBCHandler class.

        Args:
            driver (str): The driver for the database.
            server (str): The server for the database.
            database (str): The database for the database.
            username (str): The username for the database.
            password (str): The password for the database.
            port (Optional[int], optional): The port for the database. Defaults to None.
            max_pool_size (Optional[int], optional): The maximum size of the connection
                pool. Defaults to None.
        """
        super().__init__(
            port=port,
            driver=driver,
            server=server,
            database=database,
            username=username,
            password=password,
        )
        self._max_pool_size = max_pool_size
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """🔄 Initializes the connection pool if it's empty."""
        with self._pool_lock:
            if self._connection_pool.empty():
                for _ in range(self._max_pool_size):
                    conn = connect(self.connection_string)
                    self._connection_pool.put(conn)

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """🔒 Thread-safe connection context manager."""
        connection: Optional[Connection] = None
        try:
            connection = self._connection_pool.get(timeout=30)
            yield connection
        finally:
            if connection:
                if not connection.closed:
                    self._connection_pool.put(connection)
                else:
                    # Replace dead connection
                    new_conn = connect(self.connection_string)
                    self._connection_pool.put(new_conn)

    @contextmanager
    def get_cursor(self) -> Generator[Cursor, None, None]:
        """🔒 Thread-safe cursor context manager."""
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    @property
    def database_type(self) -> str:
        """
        📁 Returns the type of database being used.

        Returns:
            str: The type of database being used.
        """
        driver_lower = self.driver.lower()

        if "postgresql" in driver_lower:
            return "postgresql"
        elif "mysql" in driver_lower:
            return "mysql"
        elif "mariadb" in driver_lower:
            return "mariadb"
        elif "sqlite" in driver_lower:
            return "sqlite"
        elif any(term in driver_lower for term in ["sql server", "mssql"]):
            return "mssql"
        elif "oracle" in driver_lower:
            return "oracle"
        else:
            return "unknown"

    @property
    def connection_string(self) -> str:
        """
        ✍🏾 Generates a connection string for the database using the configuration settings

        Returns:
            str: The connection string for the database.
        """
        return (
            f"Driver={{{self.driver}}};"
            + f"Server={self.server};"
            + (f"Port={self.port};" if self.port else "")
            + f"Database={self.database};"
            + f"UID={self.username};"
            + f"PWD={self.password}"
        )

    def connect(self) -> bool:
        """
        🔗 Connects to the database using the connection string provided.

        Returns:
            bool: True if a connection to the database was established, False otherwise.

        Raises:
            DatabaseError: If a connection to the database cannot be established.
        """
        try:
            with self.get_connection() as _:
                self.log.success("Successfully connected to the database")
                return True
        except Error as error:
            self.log.error("Unable to connect to the database", error)
            raise DatabaseError("Unable to connect to the database", error)

    def disconnect(self) -> bool:
        """
        🛑 Closes the database connection if there was a connection.

        Returns:
            bool: True if the connection was closed successfully, False otherwise.

        Raises:
            DatabaseError: If there was an error disconnecting from the database.
        """
        try:
            while not self._connection_pool.empty():
                conn = self._connection_pool.get()
                if conn and not conn.closed:
                    conn.close()
            self.log.info("Disconnected and closed all connections in the pool")
            return True
        except Error as error:
            self.log.error("Unable to disconnect from the database", error)
            raise DatabaseError("Unable to disconnect from the database", error)

    def _map_row_to_dict(self, cursor: Cursor, row: Row) -> dict[str, Any]:
        """
        🗺 Maps a row to a dictionary.

        Args:
            cursor (Cursor): The cursor to use.
            row (Row): The row to map.

        Returns:
            dict[str, Any]: The mapped row.
        """
        return {column[0]: row[i] for i, column in enumerate(cursor.description)}

    def execute_read_query(
        self, query: str, params: dict[str, Any] = {}
    ) -> list[dict[str, Any]]:
        """
        📖 Executes the given SQL query with optional parameters and returns the
        results as a list of dictionaries.

        Args:
            query (str): The SQL query to execute.
            params (dict[str, Any]): A dictionary of parameters to use in the query.
                Defaults to an empty dictionary.

        Returns:
            list[dict[str, Any]]: The results of the query as a list of dictionaries.

        Raises:
            DatabaseError: If a connection to the database cannot be established.
        """
        _, parameter_style = self.validate_query_parameterization(query)
        assert parameter_style == "named", "Named parameters are required"
        _query, _params = self.convert_named_placeholders_to_query(query, params)

        try:
            with self.get_cursor() as cursor:
                cursor.execute(_query, _params)
                mapper = partial(self._map_row_to_dict, cursor)
                return [mapper(row) for row in cursor.fetchall()]
        except Error as error:
            self.log.error(
                f"Unable to execute read query: \n"
                f"Error: {error} \n"
                f"Query: {query} \n"
                f"Params: {params} \n"
            )
            raise DatabaseError("Unable to execute read query", error)

    def execute_write_query(
        self, query: str, params: dict[str, Any] = {}, check_affected_rows: bool = False
    ) -> bool:
        """
        Executes the given SQL query with optional parameters and commits the
        transaction.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): The parameters for the query. Defaults to ().
            check_affected_rows (bool, optional): When true, raises an error if
                no row was affected. Defaults to False.

        Returns:
            bool: True if the query was executed successfully, False otherwise.
        """
        _, parameter_style = self.validate_query_parameterization(query)
        assert parameter_style == "named", "Named parameters are required"
        _query, _params = self.convert_named_placeholders_to_query(query, params)

        try:
            with self.get_cursor() as cursor:
                cursor.execute(_query, _params)
                if check_affected_rows and cursor.rowcount == 0:
                    raise ValueError("No rows were affected by the query.")
                return True
        except (Error, ValueError) as error:
            self.log.error(
                f"Unable to execute write query: \n"
                f"Error: {error} \n"
                f"Query: {query} \n"
                f"Params: {params} \n"
            )
            raise DatabaseError("Unable to execute write query", error)

    def execute_write_batch_query(
        self,
        query: str,
        rows: list[dict[str, Any]] = [],
        batch_size: Optional[int] = None,
    ) -> bool:
        """
        ⛓ Executes a bulk write operation with the given query and rows of parameters.
        Optionally processes the data in batches for memory efficiency.

        Args:
            query (str): The SQL query to execute (using ? parameters)
            rows (list[dict[str, Any]]): The list of rows to process. Each row is a
                dictionary where the keys are the column names and the values are the
                corresponding values for each row.
            batch_size (Optional[int]): Size of batches to process. If None, processes
                all at once

        Returns:
            bool: True if the operation was successful

        Raises:
            DatabaseError: If the bulk operation fails
        """
        _, parameter_style = self.validate_query_parameterization(query)
        assert parameter_style == "named", "Named parameters are required"
        _query, _params_list = self.convert_named_placeholders_to_bulk_query(query, rows)

        try:
            if not rows:
                self.log.info("No rows to process in bulk operation")
                return True

            with self.get_cursor() as cursor:
                cursor.fast_executemany = True
                if batch_size is None:
                    self.log.info(f"Executing bulk operation for {len(rows)} rows...")
                    cursor.executemany(_query, _params_list)
                else:
                    total_batches = (len(rows) + batch_size - 1) // batch_size
                    self.log.info(
                        f"Executing bulk operation for {len(rows)} rows "
                        f"in {total_batches} batches of {batch_size}..."
                    )

                    for i in tqdm(
                        range(0, len(rows), batch_size),
                        desc="Processing batches",
                        total=total_batches,
                        unit="batch",
                    ):
                        cursor.executemany(query, rows[i : i + batch_size])
            return True

        except Error as error:
            sample_rows = "\n\t".join(str(row) for row in rows[:5])
            if len(rows) > 5:
                sample_rows += "\n\t... and more rows"
            self.log.error(
                "Unable to bulk execute query: \n"
                f"Error: {error} \n"
                f"Query: {query} \n"
                f"Rows: {sample_rows} \n"
            )
            raise DatabaseError("Unable to bulk execute query", error)

    def __del__(self) -> None:
        """
        Destructor to disconnect from the database when the instance is
        destroyed.
        """
        self.disconnect()
