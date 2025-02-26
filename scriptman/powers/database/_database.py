try:
    from abc import ABC, abstractmethod
    from re import IGNORECASE, match, search, sub
    from typing import Any, Optional

    from loguru import logger
    from pandas import DataFrame
    from tqdm import tqdm

    from scriptman.core.config import config
    from scriptman.powers.database._config import DatabaseConfig
    from scriptman.powers.time_calculator import TimeCalculator
except ImportError:
    raise ImportError(
        "Pandas is not installed. "
        "Kindly install the dependencies on your package manager using "
        "scriptman[etl] or scriptman[db]."
    )


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
        return self.config.driver

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
        return bool(self.execute_read_query(query))

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

    def get_table_data_types(
        self, df: DataFrame, force_nvarchar: bool = False
    ) -> dict[str, str]:
        """
        ❔ Returns a dictionary of column names to their corresponding SQL data types.

        This function takes a Pandas DataFrame and an optional boolean parameter
        `force_nvarchar` which if set to True will force all columns to be
        represented as nvarchar(max) in the database.

        Args:
            df (DataFrame): The DataFrame to extract column data types from.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            dict[str, str]: A dictionary of column names to their corresponding SQL data
                types.
        """
        dtype_map = {
            "int64": "INT",
            "int32": "INT",
            "bool": "BOOLEAN",
            "float64": "FLOAT",
            "float32": "FLOAT",
            "timedelta[ns]": "TIME",
            "object": "NVARCHAR(MAX)",  # Typically for string data
            "category": "NVARCHAR(MAX)",
            "datetime64[ns]": "DATETIME",
        }

        return {
            column: (
                "NVARCHAR(MAX)"
                if force_nvarchar
                else dtype_map.get(str(df[column].dtype), "NVARCHAR(MAX)")
            )
            for column in df.columns
        }

    def generate_prepared_insert_query(
        self, table_name: str, df: DataFrame, prepare_data: bool = True
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL insert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to insert into.
            df (DataFrame): The DataFrame containing the data to insert.
            prepare_data (bool): Whether to prepare the data for insertion. Defaults to
                True.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and the
                values are the corresponding values for each row
        """
        columns = ", ".join([f'"{column_name}"' for column_name in df.columns])
        placeholders = ", ".join([f":{column_name}" for column_name in df.columns])
        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
        values = self.transform_df_to_dict(df, prepare_data)
        return query, values

    def generate_prepared_update_query(
        self, table_name: str, df: DataFrame, prepare_data: bool = True
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL update query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to update.
            df (DataFrame): The DataFrame containing the data to update.
            prepare_data (bool): Whether to prepare the data for updating. Defaults to
                True.

        Returns:
            tuple(str, list(tuple)): The prepared SQL query and the list of values to
                update.
        """
        set_clause = ", ".join(
            [f'"{column_name}" = :{column_name}' for column_name in df.columns]
        )
        where_clause = " AND ".join(
            [f'"{index_name}" = :{index_name}' for index_name in df.index.names]
        )
        query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'
        values = self.transform_df_to_dict(df, prepare_data)
        return query, values

    def generate_prepared_delete_query(
        self, table_name: str, df: DataFrame, prepare_data: bool = True
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL delete query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to delete from.
            df (DataFrame): The DataFrame containing the data to delete.
            prepare_data (bool): Whether to prepare the data for deletion. Defaults to
                True.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and the
                values are the corresponding values for each row
        """
        where_clause = " AND ".join(
            [f'"{index_name}" = :{index_name}' for index_name in df.index.names]
        )
        query = f'DELETE FROM "{table_name}" WHERE {where_clause}'
        values = self.transform_df_to_dict(df, prepare_data)
        return query, values

    def generate_prepared_upsert_query(
        self, table_name: str, df: DataFrame, prepare_data: bool = True
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL upsert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to upsert into.
            df (DataFrame): The DataFrame containing the data to upsert.
            prepare_data (bool): Whether to prepare the data for upserting. Defaults to
                True.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and the
                values are the corresponding values for each row
        """
        prep: bool = prepare_data
        query: Optional[str] = None
        values: Optional[list[dict[str, Any]]] = None

        if self.database_type in ["postgresql"]:
            # Use INSERT ... ON CONFLICT DO UPDATE for PostgreSQL
            query, values = self.generate_prepared_insert_query(table_name, df, prep)
            update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns])
            constraints = ", ".join([f'"{c}"' for c in df.index.names])
            query = f"{query} ON CONFLICT ({constraints}) DO UPDATE SET {update_clause}"

        elif self.database_type in ["mysql", "mariadb"]:
            # Use ON DUPLICATE KEY UPDATE for MySQL and MariaDB
            query, values = self.generate_prepared_insert_query(table_name, df, prep)
            update_clause = ", ".join([f'"{col}" = :{col}' for col in df.columns])
            query = f"{query} ON DUPLICATE KEY UPDATE {update_clause}"

        elif self.database_type in ["sqlite"]:
            # Use INSERT OR REPLACE for SQLite
            query, values = self.generate_prepared_insert_query(table_name, df, prep)
            query = str(query).replace("INSERT INTO", "INSERT OR REPLACE INTO")

        elif self.database_type in ["mssql", "oracle"]:
            # Use MERGE for MSSQL Server and Oracle
            query, values = self.generate_merge_query(table_name, df, prep)

        assert query is not None, "Unsupported database type"
        assert values is not None, "No values to upsert"
        return query, values

    def generate_merge_query(
        self, table_name: str, df: DataFrame, prepare_data: bool = True
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a SQL MERGE INTO query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to merge into.
            df (DataFrame): The DataFrame containing the data to merge.
            prepare_data (bool): Whether to prepare the data for merging. Defaults to
                True.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and
                the values are the corresponding values for each row.
        """
        # Columns to be used in the MERGE statement (all columns except key columns)
        columns_to_update = [col for col in df.columns if col not in df.index.names]

        # Build the column names and placeholder parts for the MERGE statement
        column_names = ", ".join([f'"{col}"' for col in df.columns])
        update_columns = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in columns_to_update])

        # Build the conditions to match on the key columns
        match_conditions = " AND ".join(
            [f"source.{k} = target.{k}" for k in df.index.names]
        )

        # Construct the MERGE INTO query for both MSSQL and Oracle
        query = f"""
        MERGE INTO "{table_name}" AS target
        USING (SELECT {', '.join([f':{col}' for col in df.columns])} FROM {table_name})
        AS source
        ON {match_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update_columns}
        WHEN NOT MATCHED THEN
            INSERT ({column_names})
            VALUES ({', '.join([f':{col}' for col in df.columns])})
        """

        # Prepare the list of values as dictionaries to bind to the query
        values = self.transform_df_to_dict(df, prepare_data)
        return query, values

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

    @staticmethod
    def transform_df_to_dict(df: DataFrame, prepare: bool = True) -> list[dict[str, Any]]:
        """
        ⛓ Transforms a pandas DataFrame into a list of dictionaries.

        Args:
            df (DataFrame): The DataFrame to transform.
            prepare_data (bool): Whether to prepare the data for insertion.
                Defaults to True.

        Returns:
            list[dict[str, Any]]: The list of dictionaries where the keys are the
                column names and the index names and the values are the
                corresponding values for each row.
        """
        values: list[dict[str, Any]]

        if prepare:
            with TimeCalculator.context("DataFrame to Dictionary"):
                values = [
                    {str(key): value}
                    for key, value in df.reset_index().to_dict(orient="records")
                ]
        else:
            # HACK: Ignoring the string index conversion for now
            values = df.reset_index().to_dict(orient="records")  # type: ignore

        return values

    def __del__(self) -> None:
        """
        🧹 Destructor to disconnect from the database when the instance is
        destroyed.
        """
        self.disconnect()
