try:
    from typing import Any, Iterator, Optional

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError

    from scriptman.core.config import config
    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.database._exceptions import DatabaseError
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using "
        "scriptman[sqlalchemy]."
    )


class SQLAlchemyHandler(DatabaseHandler):
    def __init__(
        self,
        protocol: str,
        driver: str,
        server: str,
        database: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        windows_auth: bool = False,
    ) -> None:
        """
        🚀 Initializes the SQLAlchemyHandler class.

        Args:
            driver (str): The driver for the database.
            server (str): The server for the database.
            database (str): The database for the database.
            port (Optional[int], optional): The port for the database. Defaults to None.
            username (Optional[str], optional): The username for the database. Defaults
                to None.
            password (Optional[str], optional): The password for the database. Defaults
                to None.
            windows_auth (bool, optional): Whether to use Windows authentication. Defaults
                to False.
        """
        super().__init__(
            port=port,
            driver=driver,
            server=server,
            database=database,
            username=username,
            password=password,
        )
        self._windows_auth = windows_auth
        self._protocol = protocol
        self._engine: Engine
        self.connect()

    @property
    def windows_auth(self) -> bool:
        """
        🔑 Returns whether Windows authentication is enabled.
        """
        return self._windows_auth

    @property
    def database_name(self) -> str:
        """
        📁 Returns the name of the database being used.
        """
        return self.database

    @property
    def database_type(self) -> str:
        """
        📁 Returns the type of database being used.

        Returns:
            str: The type of database being used.
        """
        return self._engine.dialect.name

    @property
    def connection_string(self) -> str:
        """
        ✍🏾 Generates a connection string for the database using the configuration settings

        Returns:
            str: The connection string for the database.
        """
        from urllib.parse import quote_plus

        server = f"{self.server}{f':{self.port}' if self.port is not None else ''}"
        trusted_connection = "&Trusted_Connection=yes" if self.windows_auth else ""
        credentials = (
            f"{self.username}:{quote_plus(self.password)}@"
            if self.username and self.password and not self.windows_auth
            else ""
        )
        return (
            f"{self._protocol}://{credentials}{server}/{self.database}?"
            f"driver={self.driver}{trusted_connection}"
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
            self._engine = create_engine(self.connection_string)
            with self._engine.connect() as session:  # Test the connection
                session.execute(text("SELECT 1"))
            self.log.success(f"Successfully connected to {self.database}")
            return True
        except SQLAlchemyError as error:
            self.log.error(f"Unable to connect to {self.database}: {error}")
            raise DatabaseError(f"Unable to connect to {self.database}", error)

    def disconnect(self) -> bool:
        """
        🛑 Closes the database connection if there was a connection.

        Returns:
            bool: True if the connection was closed successfully, False otherwise.

        Raises:
            DatabaseError: If there was an error disconnecting from the database.
        """
        try:
            self._engine.dispose()
            self.log.info("Disconnected from the database")
            return True
        except SQLAlchemyError as error:
            self.log.error(f"Unable to disconnect from the database: {error}")
            raise DatabaseError("Unable to disconnect from the database", error)

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

        try:
            with self._engine.connect() as session:
                return [dict(_._mapping) for _ in session.execute(text(query), params)]
        except SQLAlchemyError as error:
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
        ✍🏾 Executes a given SQL write query with optional parameters and commits the
        transaction.

        Args:
            query (str): The SQL query to execute.
            params (dict[str, Any], optional): A dictionary of parameters to be used in
                the query. Defaults to an empty dictionary.
            check_affected_rows (bool, optional): If True, raises a ValueError if no rows
                were affected by the query. Defaults to False.

        Returns:
            bool: True if the query was executed successfully, otherwise raises a
                DatabaseError.

        Raises:
            DatabaseError: If unable to execute the write query.
            ValueError: If check_affected_rows is True and no rows were affected by the
                query.
        """
        _, parameter_style = self.validate_query_parameterization(query)
        assert parameter_style in ["named", "none"], "Named parameters are required"

        try:
            with self._engine.begin() as session:
                result = session.execute(text(query), params)
                if check_affected_rows and result.rowcount == 0:
                    raise ValueError("No rows were affected by the query.")
                return True
        except (SQLAlchemyError, ValueError) as error:
            self.log.error(
                "Unable to execute write query: \n"
                f"Error: {error} + \n"
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

        try:
            if not rows:
                self.log.info("No rows to process in bulk operation")
                return True

            with self._engine.begin() as session:
                self.log.info(f"Executing bulk operation for {len(rows)} rows...")
                session.execute(text(query), rows)
            self.log.success("Bulk operation completed successfully")
            return True

        except SQLAlchemyError as error:
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

    def execute_write_batch_query(
        self,
        query: str,
        rows: Iterator[dict[str, Any]] | list[dict[str, Any]] = [],
        batch_size: int = config.settings.get("BATCH_SIZE", 1000),
    ) -> bool:
        """
        ⛓ Executes a bulk write operation with the given query and rows of parameters.
        Optionally processes the data in batches for memory efficiency.

        Args:
            query (str): The SQL query to execute (using ? parameters)
            rows (Union[Iterator[list[dict[str, Any]]], list[dict[str, Any]]]): Either a
                list of rows to process or an iterator that yields batches of rows.
                Each row is a dictionary where the keys are the column names and the
                values are the corresponding values for each row.
            batch_size (int): Size of batches to process.

        Returns:
            bool: True if the operation was successful

        Raises:
            DatabaseError: If the bulk operation fails
        """
        total_rows: int = 0
        batch_number: int = 0
        batched_rows: list[dict[str, Any]] = []
        _, parameter_style = self.validate_query_parameterization(query)
        assert parameter_style in ["named", "none"], "Named parameters are required"

        try:
            self.log.info("Executing bulk operation with iterative batches...")
            for record in rows:
                batched_rows.append(record)
                total_rows += 1
                if len(batched_rows) >= batch_size:
                    batch_number += 1
                    self.execute_write_bulk_query(query, batched_rows)
                    batched_rows = []

            if batched_rows:  # Process any remaining rows
                batch_number += 1
                self.execute_write_bulk_query(query, batched_rows)

            self.log.success(
                f"Successfully executed the bulk operation with "
                f"{batch_number} batches of {total_rows} rows"
            )
            return True

        except SQLAlchemyError as error:
            sample_rows = "\n\t".join(str(row) for row in batched_rows[:5])
            if len(batched_rows) > 5:
                sample_rows += "\n\t... and more rows"

            self.log.error(
                "Unable to bulk execute query: \n"
                f"Error: {error}\n"
                f"Query: {query}\n"
                f"Batch Size: {batch_size}\n"
                f"Total Rows: {len(batched_rows)}\n"
                f"Sample Rows:\n\t{sample_rows}"
            )
            raise DatabaseError("Unable to bulk execute query", error)

    def __del__(self) -> None:
        """
        Destructor to disconnect from the database when the instance is
        destroyed.
        """
        try:
            self.disconnect()
        except Exception as error:
            self.log.error(f"Unable to disconnect from the database: {error}")
