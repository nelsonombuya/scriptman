try:
    from typing import Any, Optional, Protocol, cast

    from pandas import DataFrame
except ImportError:
    raise ImportError(
        "Pandas is not installed. "
        "Kindly install the dependencies on your package manager using scriptman[etl]."
    )


class ETLDatabaseInterface(Protocol):
    """🔒 Protocol defining the required methods for ETL database operations"""

    @property
    def database_name(self) -> str:
        """🔍 Get the database name from the underlying handler"""
        ...

    @property
    def database_type(self) -> str:
        """🔍 Get the database type from the underlying handler"""
        ...

    def execute_read_query(
        self, query: str, params: dict[str, Any] = {}
    ) -> list[dict[str, Any]]:
        """🔍 Execute a read query on the database"""
        ...

    def execute_write_query(
        self, query: str, params: dict[str, Any] = {}, check_affected_rows: bool = False
    ) -> bool:
        """🔍 Execute a write query on the database"""
        ...

    def execute_write_batch_query(
        self,
        query: str,
        rows: list[dict[str, Any]] = [],
        batch_size: Optional[int] = None,
    ) -> bool:
        """🔍 Execute a write batch query on the database"""
        ...

    def table_exists(self, table_name: str) -> bool:
        """🔍 Check if a table exists on the database"""
        ...

    def create_table(
        self, table_name: str, columns: dict[str, str], keys: Optional[list[str]] = None
    ) -> bool:
        """🔍 Create a table on the database"""
        ...

    def truncate_table(self, table_name: str) -> bool:
        """🔍 Truncate a table on the database"""
        ...

    def drop_table(self, table_name: str) -> bool:
        """🔍 Drop a table on the database"""
        ...


class ETLDatabase:
    """📦 ETL database operations using composition instead of inheritance"""

    def __init__(self, database_handler: ETLDatabaseInterface):
        """
        🚀 Initialize ETL database with a database handler.

        Args:
            database_handler: DatabaseHandler object
        """
        self.db = database_handler

    @property
    def database_name(self) -> str:
        """🔍 Get the database name from the underlying handler"""
        return self.db.database_name

    @property
    def database_type(self) -> str:
        """🔍 Get the database type from the underlying handler"""
        return self.db.database_type

    def get_table_data_types(
        self, df: DataFrame, force_nvarchar: bool = False
    ) -> dict[str, str]:
        """
        ❔ Returns a dictionary of column names to their corresponding SQL data types.

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
        self, table_name: str, df: DataFrame
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL insert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to insert into.
            df (DataFrame): The DataFrame containing the data to insert.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and the
                values are the corresponding values for each row
        """
        columns = ", ".join([f'"{column_name}"' for column_name in df.columns])
        placeholders = ", ".join([f":{column_name}" for column_name in df.columns])
        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
        values = cast(list[dict[str, Any]], df.reset_index().to_dict(orient="records"))
        return query, values

    def generate_prepared_update_query(
        self, table_name: str, df: DataFrame
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL update query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to update.
            df (DataFrame): The DataFrame containing the data to update.

        Returns:
            tuple(str, list(tuple)): The prepared SQL query and the list of values to
                update.
        """
        assert df.index.names is not None, "Index names are required"
        set_clause = ", ".join(
            [f'"{column_name}" = :{column_name}' for column_name in df.columns]
        )
        where_clause = " AND ".join(
            [f'"{index_name}" = :{index_name}' for index_name in df.index.names]
        )
        query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'
        values = cast(list[dict[str, Any]], df.reset_index().to_dict(orient="records"))
        return query, values

    def generate_prepared_delete_query(
        self, table_name: str, df: DataFrame
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL delete query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to delete from.
            df (DataFrame): The DataFrame containing the data to delete.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and the
                values are the corresponding values for each row
        """
        assert df.index.names is not None, "Index names are required"
        where_clause = " AND ".join(
            [f'"{index_name}" = :{index_name}' for index_name in df.index.names]
        )
        query = f'DELETE FROM "{table_name}" WHERE {where_clause}'
        values = cast(list[dict[str, Any]], df.reset_index().to_dict(orient="records"))
        return query, values

    def generate_prepared_upsert_query(
        self, table_name: str, df: DataFrame
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL upsert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to upsert into.
            df (DataFrame): The DataFrame containing the data to upsert.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and the
                values are the corresponding values for each row
        """
        query: Optional[str] = None
        values: Optional[list[dict[str, Any]]] = None

        if self.database_type in ["postgresql"]:
            # Use INSERT ... ON CONFLICT DO UPDATE for PostgreSQL
            query, values = self.generate_prepared_insert_query(table_name, df)
            update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns])
            constraints = ", ".join([f'"{c}"' for c in df.index.names])
            query = f"{query} ON CONFLICT ({constraints}) DO UPDATE SET {update_clause}"

        elif self.database_type in ["mysql", "mariadb"]:
            # Use ON DUPLICATE KEY UPDATE for MySQL and MariaDB
            query, values = self.generate_prepared_insert_query(table_name, df)
            update_clause = ", ".join([f'"{col}" = :{col}' for col in df.columns])
            query = f"{query} ON DUPLICATE KEY UPDATE {update_clause}"

        elif self.database_type in ["sqlite"]:
            # Use INSERT OR REPLACE for SQLite
            query, values = self.generate_prepared_insert_query(table_name, df)
            query = str(query).replace("INSERT INTO", "INSERT OR REPLACE INTO")

        elif self.database_type in ["mssql", "oracle"]:
            # Use MERGE for MSSQL Server and Oracle
            query, values = self.generate_merge_query(table_name, df)

        assert query is not None, "Unsupported database type"
        assert values is not None, "No values to upsert"
        return query, values

    def generate_merge_query(
        self, table_name: str, df: DataFrame
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        ✍🏾 Generates a SQL MERGE INTO query using a temporary table approach.

        Args:
            table_name (str): The name of the table to merge into.
            df (DataFrame): The DataFrame containing the data to merge.

        Returns:
            tuple(str, list[dict[str, Any]]): The prepared SQL query and the list of
                dictionaries where the keys are the column names and index names and
                the values are the corresponding values for each row.
        """
        indices = df.index.names
        reset_df = df.reset_index()
        data_types = self.get_table_data_types(reset_df)
        assert indices is not None, "Index names are required"

        # Columns to be used in the MERGE statement
        columns_to_insert = [c for c in reset_df.columns]
        columns_to_update = [c for c in reset_df.columns if c not in indices]

        # Build the query parts
        temp_table = f"#temp_{table_name}"
        temp_schema = ", ".join([f'"{c}" {data_types[c]}' for c in columns_to_insert])
        update = ", ".join([f'target."{c}" = source."{c}"' for c in columns_to_update])
        match_conditions = " AND ".join([f'source."{k}" = target."{k}"' for k in indices])

        # Construct the complete query with temporary table
        query = f"""
        -- Create temporary table
        CREATE TABLE {temp_table} ({temp_schema});

        -- Insert data into temporary table
        INSERT INTO {temp_table} ({', '.join([f'"{c}"' for c in columns_to_insert])})
        VALUES ({', '.join([f':{c}' for c in columns_to_insert])});

        -- Perform the merge operation
        MERGE INTO "{table_name}" AS target
        USING "{temp_table}" AS source
        ON {match_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join([f'"{c}"' for c in columns_to_insert])})
            VALUES ({', '.join([f'source."{c}"' for c in columns_to_insert])});

        -- Clean up temporary table
        DROP TABLE "{temp_table}";
        """

        # Prepare the values
        values = cast(list[dict[str, Any]], df.reset_index().to_dict(orient="records"))
        return query, values

    def execute_read_query(
        self, query: str, params: dict[str, Any] = {}
    ) -> list[dict[str, Any]]:
        """Delegate to the database handler"""
        return self.db.execute_read_query(query, params)

    def execute_write_query(
        self, query: str, params: dict[str, Any] = {}, check_affected_rows: bool = False
    ) -> bool:
        """Delegate to the database handler"""
        return self.db.execute_write_query(query, params, check_affected_rows)

    def execute_write_batch_query(
        self,
        query: str,
        rows: list[dict[str, Any]] = [],
        batch_size: Optional[int] = None,
    ) -> bool:
        """Delegate to the database handler"""
        return self.db.execute_write_batch_query(query, rows, batch_size)

    def table_exists(self, table_name: str) -> bool:
        """Delegate to the database handler"""
        return self.db.table_exists(table_name)

    def create_table(
        self, table_name: str, columns: dict[str, str], keys: Optional[list[str]] = None
    ) -> bool:
        """Delegate to the database handler"""
        return self.db.create_table(table_name, columns, keys)

    def truncate_table(self, table_name: str) -> bool:
        """Delegate to the database handler"""
        return self.db.truncate_table(table_name)

    def drop_table(self, table_name: str) -> bool:
        """Delegate to the database handler"""
        return self.db.drop_table(table_name)
