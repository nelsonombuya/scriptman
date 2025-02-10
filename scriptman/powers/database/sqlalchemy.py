try:
    from typing import Any, Optional

    from loguru import logger
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from tqdm import tqdm

    from scriptman.powers.database._config import DatabaseConfig
    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.database._exceptions import DatabaseError
except ImportError:
    raise ImportError(
        "SQLAlchemy is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[etl]."
    )


class SQLAlchemyHandler(DatabaseHandler):
    def __init__(self, config: DatabaseConfig) -> None:
        """
        🚀 Initializes the SQLAlchemyHandler class.

        Args:
            config (DatabaseConfig): The configuration settings for the database.
        """
        super().__init__(config)
        self._engine: Engine
        self.connect()

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
        port_part = f":{self.config.port}" if self.config.port is not None else ""
        return (
            f"{self.config.driver}://"
            f"{self.config.username}:{self.config.password}@"
            f"{self.config.server}{port_part}/"
            f"{self.config.database}"
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
            self.log.success(f"Successfully connected to {self.database_name}")
            return True
        except SQLAlchemyError as error:
            self.log.error(f"Unable to connect to {self.database_name}: {error}")
            raise DatabaseError(f"Unable to connect to {self.database_name}", error)

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
        try:
            with self._engine.connect() as session:
                return [dict(_._mapping) for _ in session.execute(text(query), params)]
        except SQLAlchemyError as error:
            logger.error(
                f"Unable to execute read query: \n"
                f"Error: {error} \n"
                f"Query: {query} \n"
                f"Params: {params} \n"
            )
            raise DatabaseError("Unable to execute read query", error)

    def execute_write_query(
        self,
        query: str,
        params: dict[str, Any] = {},
        check_affected_rows: bool = False,
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
        try:
            if not rows:
                logger.info("No rows to process in bulk operation")
                return True

            with self._engine.begin() as session:
                if batch_size is None:
                    logger.info(f"Executing bulk operation for {len(rows)} rows...")
                    session.execute(text(query), rows)
                else:
                    total_batches = (len(rows) + batch_size - 1) // batch_size
                    logger.info(
                        f"Executing bulk operation for {len(rows)} rows "
                        f"in {total_batches} batches of {batch_size}..."
                    )

                    for i in tqdm(
                        range(0, len(rows), batch_size),
                        desc="Processing batches",
                        total=total_batches,
                        unit="batch",
                    ):
                        session.execute(text(query), rows[i : i + batch_size])

            logger.success("Bulk operation completed successfully")
            return True

        except SQLAlchemyError as error:
            sample_rows = "\n\t".join(str(row) for row in rows[:5])
            if len(rows) > 5:
                sample_rows += "\n\t... and more rows"

            logger.error(
                "Unable to execute bulk operation:\n"
                f"Error: {error}\n"
                f"Query: {query}\n"
                f"Batch Size: {batch_size}\n"
                f"Total Rows: {len(rows)}\n"
                f"Sample Rows:\n\t{sample_rows}"
            )
            raise DatabaseError("Unable to execute bulk operation", error)
