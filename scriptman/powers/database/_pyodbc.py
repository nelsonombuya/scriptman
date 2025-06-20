try:
    from contextlib import contextmanager
    from functools import partial
    from queue import Queue
    from threading import Lock
    from typing import Any, Generator, Optional

    from pyodbc import Connection, Cursor, Error, Row, connect

    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.database._exceptions import DatabaseError
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
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
        pool_size: int = 20,
        pool_timeout: int = 60,
        pool_recycle_time: int = 3600,
        pool_validate_connections: bool = True,
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
            pool_size (int, optional): The size of the connection pool. Defaults to 20.
            pool_timeout (int, optional): The timeout in seconds for getting connection
                from pool. Defaults to 60.
            pool_recycle_time (int, optional): The time in seconds after which
                connections are recreated. Defaults to 3600 (1 hour).
            pool_validate_connections (bool, optional): Whether to validate connections
                before use. Defaults to True.
        """
        super().__init__(
            port=port,
            driver=driver,
            server=server,
            database=database,
            username=username,
            password=password,
        )
        self._pool_size = pool_size
        self._pool_timeout = pool_timeout
        self._pool_recycle_time = pool_recycle_time
        self._connection_created_times: dict[int, float] = {}
        self._pool_validate_connections = pool_validate_connections
        self._initialize_pool()

    @classmethod
    def for_etl(
        cls,
        driver: str,
        server: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int] = None,
    ) -> "PyODBCHandler":
        """
        🚀 Create PyODBCHandler optimized for heavy ETL workloads with maximum
        supported connection pool settings.

        This configuration provides:
        - pool_size=100: Very large connection pool for maximum concurrency
        - pool_timeout=300: Extended timeout (5 minutes)
        - pool_recycle_time=900: Faster connection recycling (15 minutes)
        - pool_validate_connections=True: Connection validation enabled

        Note: PyODBC can handle high connection counts but may be less efficient
        than SQLAlchemy for very large pools. Monitor performance and adjust
        if needed.

        Args:
            driver (str): The driver for the database.
            server (str): The server for the database.
            database (str): The database for the database.
            username (str): The username for the database.
            password (str): The password for the database.
            port (Optional[int], optional): The port for the database.

        Returns:
            PyODBCHandler: Configured handler for heavy ETL workloads
        """
        return cls(
            driver=driver,
            server=server,
            database=database,
            username=username,
            password=password,
            port=port,
            pool_size=100,
            pool_timeout=300,  # 5 minutes
            pool_recycle_time=900,  # 15 minutes
            pool_validate_connections=True,
        )

    def upgrade_to_etl(self) -> "PyODBCHandler":
        """
        🚀 Upgrade this existing handler to heavy ETL connection pool settings.

        This method reinitializes the connection pool with maximum ETL settings
        while preserving all existing connection parameters.

        Configuration applied:
        - pool_size=100: Very large connection pool for maximum concurrency
        - pool_timeout=300: Extended timeout (5 minutes)
        - pool_recycle_time=900: Faster connection recycling (15 minutes)
        - pool_validate_connections=True: Connection validation enabled

        Returns:
            PyODBCHandler: The same instance with upgraded pool settings
        """
        self.log.info("Upgrading connection pool to heavy ETL settings...")
        self.disconnect()

        self._pool_size = 100
        self._pool_timeout = 300
        self._pool_recycle_time = 900
        self._pool_validate_connections = True
        self._connection_created_times.clear()

        self._initialize_pool()
        self.log.success("Successfully upgraded to heavy ETL mode")
        return self

    def _initialize_pool(self) -> None:
        """🔄 Initializes the connection pool if it's empty."""
        from time import time

        with self._pool_lock:
            if self._connection_pool.empty():
                for _ in range(self._pool_size):
                    conn = connect(self.connection_string)
                    self._connection_created_times[id(conn)] = time()
                    self._connection_pool.put(conn)

                self.log.info(
                    f"Initialized connection pool with {self._pool_size} connections, "
                    f"timeout={self._pool_timeout}s, "
                    f"recycle_time={self._pool_recycle_time}s"
                )

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """🔒 Thread-safe connection context manager with validation and recycling."""
        from time import time

        connection: Optional[Connection] = None
        try:
            connection = self._connection_pool.get(timeout=self._pool_timeout)

            # Check if connection needs recycling
            conn_id = id(connection)
            if conn_id in self._connection_created_times:
                age = time() - self._connection_created_times[conn_id]
                if age > self._pool_recycle_time:
                    # Connection is too old, replace it
                    if not connection.closed:
                        connection.close()
                    connection = connect(self.connection_string)
                    self._connection_created_times[id(connection)] = time()

            # Validate connection if enabled
            if self._pool_validate_connections and not connection.closed:
                try:
                    cursor = connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                except Exception:
                    # Connection is invalid, replace it
                    if not connection.closed:
                        connection.close()
                    connection = connect(self.connection_string)
                    self._connection_created_times[id(connection)] = time()

            yield connection
        finally:
            if connection:
                if not connection.closed:
                    self._connection_pool.put(connection)
                else:
                    # Replace dead connection
                    new_conn = connect(self.connection_string)
                    self._connection_created_times[id(new_conn)] = time()
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
                self.log.success(
                    f"Successfully connected to {self.database} with pool configuration: "
                    f"pool_size={self._pool_size}, timeout={self._pool_timeout}s, "
                    f"recycle_time={self._pool_recycle_time}s, "
                    f"validation={self._pool_validate_connections}"
                )
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
        assert parameter_style in ["named", "none"], "Named parameters are required"
        _query, _params = self.convert_named_placeholders_to_query(query, params)

        try:
            with self.get_cursor() as cursor:
                cursor.execute(_query, _params)
                mapper = partial(self._map_row_to_dict, cursor)
                return [mapper(row) for row in cursor.fetchall()]
        except Error as error:
            if "deadlock" in str(error).lower():
                self.log.warning("Deadlock detected, retrying query...")
                return self.execute_read_query(query, params)
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
        assert parameter_style in ["named", "none"], "Named parameters are required"
        _query, _params = self.convert_named_placeholders_to_query(query, params)

        try:
            with self.get_cursor() as cursor:
                cursor.execute(_query, _params)
                if check_affected_rows and cursor.rowcount == 0:
                    raise ValueError("No rows were affected by the query.")
                return True
        except (Error, ValueError) as error:
            if "deadlock" in str(error).lower():
                self.log.warning("Deadlock detected, retrying query...")
                return self.execute_write_query(query, params, check_affected_rows)
            self.log.error(
                f"Unable to execute write query: \n"
                f"Error: {error} \n"
                f"Query: {query} \n"
                f"Params: {params} \n"
            )
            raise DatabaseError("Unable to execute write query", error)

    def execute_write_bulk_query(
        self, query: str, rows: list[dict[str, Any]] = []
    ) -> bool:
        """
        ⛓ Executes a bulk write operation with the given query and rows of parameters.

        Args:
            query (str): The SQL query to execute (using ? parameters)
            rows (list[dict[str, Any]]): The list of rows to process. Each row is a
                dictionary where the keys are the column names and the values are the
                corresponding values for each row.

        Returns:
            bool: True if the operation was successful

        Raises:
            DatabaseError: If the bulk operation fails
        """
        _, parameter_style = self.validate_query_parameterization(query)
        assert parameter_style in ["named", "none"], "Named parameters are required"
        _query, _params_list = self.convert_named_placeholders_to_bulk_query(query, rows)

        try:
            if not rows:
                self.log.info("No rows to process in bulk operation")
                return True
            with self.get_cursor() as cursor:
                cursor.fast_executemany = True
                self.log.info(f"Executing bulk operation for {len(rows)} rows...")
                cursor.executemany(_query, _params_list)
            return True
        except Error as error:
            if "deadlock" in str(error).lower():
                self.log.warning("Deadlock detected, retrying query...")
                return self.execute_write_bulk_query(query, rows)
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
