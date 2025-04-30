try:
    from typing import Any, Iterator, Optional

    from loguru import logger
    from pandas import DataFrame

    from scriptman.powers.database._database import DatabaseHandler

except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using scriptman[etl]."
    )


class ETLDatabase:
    """📦 ETL database operations using composition instead of inheritance"""

    def __init__(self, database_handler: DatabaseHandler):
        """
        🚀 Initialize ETL database with a database handler.

        Args:
            database_handler: DatabaseHandler object
        """
        self.db = database_handler
        self.log = logger.bind(
            database=self.db.database_name,
            handler=self.__class__.__name__,
        )

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
            "float64": "FLOAT",
            "float32": "FLOAT",
            "timedelta[ns]": "TIME",
            "object": "NVARCHAR(MAX)",  # Typically for string data
            "category": "NVARCHAR(MAX)",
            "datetime64[ns]": "DATETIME",
            "bool": "BOOLEAN" if self.database_type in ["postgresql"] else "BIT",
        }

        return {
            column: (
                "NVARCHAR(MAX)"
                if force_nvarchar
                else dtype_map.get(str(df[column].dtype), "NVARCHAR(MAX)")
            )
            for column in df.columns
        }

    def prepare_values(
        self, df: DataFrame, force_nvarchar: bool = False
    ) -> Iterator[dict[str, Any]]:
        """
        ✍🏾 Prepares the values for the given DataFrame in batches.

        Args:
            df (DataFrame): The DataFrame to prepare the values for.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            Iterator[dict[str, Any]]: An iterator of prepared records.
        """
        from json import dumps
        from math import isnan

        def transform_value(value: Any) -> Any:
            if isinstance(value, (float, int)) and isnan(value):
                return None
            if force_nvarchar:
                if isinstance(value, (dict, list)):
                    return dumps(value)
                return str(value) if value else None
            return value

        for record in df.reset_index().to_dict(orient="records"):
            yield {str(k): transform_value(v) for k, v in record.items()}

    def generate_prepared_insert_query(
        self, table_name: str, df: DataFrame, force_nvarchar: bool = False
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL insert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to insert into.
            df (DataFrame): The DataFrame containing the data to insert.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            tuple(str, Iterator[dict[str, Any]]): The prepared SQL query and the
                iterator of dictionaries where the keys are the column names and the
                values are the corresponding values for each row.
        """
        df = df.reset_index()
        columns = ", ".join([f'"{column_name}"' for column_name in df.columns])
        placeholders = ", ".join([f":{column_name}" for column_name in df.columns])
        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
        return query, self.prepare_values(df, force_nvarchar)

    def generate_prepared_update_query(
        self, table_name: str, df: DataFrame, force_nvarchar: bool = False
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL update query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to update.
            df (DataFrame): The DataFrame containing the data to update.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            tuple(str, Iterator[dict[str, Any]]): The prepared SQL query and the
                iterator of dictionaries where the keys are the column names and the
                index names and the values are the corresponding values for each row.
        """
        assert df.index.names is not None, "Index names are required"
        set_clause = ", ".join(
            [f'"{column_name}" = :{column_name}' for column_name in df.columns]
        )
        where_clause = " AND ".join(
            [f'"{index_name}" = :{index_name}' for index_name in df.index.names]
        )
        query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'
        return query, self.prepare_values(df, force_nvarchar)

    def generate_prepared_delete_query(
        self, table_name: str, df: DataFrame, force_nvarchar: bool = False
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL delete query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to delete from.
            df (DataFrame): The DataFrame containing the data to delete.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            tuple(str, Iterator[dict[str, Any]]): The prepared SQL query and the
                iterator of dictionaries where the keys are the column names and the
                values are the corresponding values for each row.
        """
        assert df.index.names is not None, "Index names are required"
        where_clause = " AND ".join(
            [f'"{index_name}" = :{index_name}' for index_name in df.index.names]
        )
        query = f'DELETE FROM "{table_name}" WHERE {where_clause}'
        return query, self.prepare_values(df, force_nvarchar)

    def generate_prepared_upsert_query(
        self, table_name: str, df: DataFrame, force_nvarchar: bool = False
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL upsert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to upsert into.
            df (DataFrame): The DataFrame containing the data to upsert.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            tuple(str, Iterator[dict[str, Any]]): The prepared SQL query and the
                iterator of dictionaries where the keys are the column names and the
                values are the corresponding values for each row.
        """
        var: bool = force_nvarchar
        query: Optional[str] = None
        values: Optional[Iterator[dict[str, Any]]] = None

        if self.database_type in ["postgresql"]:
            # Use INSERT ... ON CONFLICT DO UPDATE for PostgreSQL
            query, values = self.generate_prepared_insert_query(table_name, df, var)
            update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns])
            constraints = ", ".join([f'"{c}"' for c in df.index.names])
            query = f"{query} ON CONFLICT ({constraints}) DO UPDATE SET {update_clause}"

        elif self.database_type in ["mysql", "mariadb"]:
            # Use ON DUPLICATE KEY UPDATE for MySQL and MariaDB
            query, values = self.generate_prepared_insert_query(table_name, df, var)
            update_clause = ", ".join([f'"{col}" = :{col}' for col in df.columns])
            query = f"{query} ON DUPLICATE KEY UPDATE {update_clause}"

        elif self.database_type in ["sqlite"]:
            # Use INSERT OR REPLACE for SQLite
            query, values = self.generate_prepared_insert_query(table_name, df, var)
            query = str(query).replace("INSERT INTO", "INSERT OR REPLACE INTO")

        elif self.database_type in ["mssql", "oracle"]:
            # Use MERGE for MSSQL Server and Oracle
            query, values = self.generate_merge_query(table_name, df, var)

        assert query is not None, "Unsupported database type"
        assert values is not None, "No values to upsert"
        return query, values

    def synchronize_table_schema(
        self, table_name: str, df: DataFrame, force_nvarchar: bool = False
    ) -> bool:
        """
        🔄 Synchronizes the table schema with the DataFrame structure.

        This method will:
        1. Check if the table exists
        2. If it doesn't exist, create it with the DataFrame's schema
        3. If it exists, check for missing columns and add them
        4. Update column data types if needed

        Args:
            table_name (str): The name of the table to synchronize
            df (DataFrame): The DataFrame containing the target schema
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX)

        Returns:
            bool: True if schema was synchronized successfully, False otherwise
        """
        try:
            # Get the target schema from the DataFrame
            target_schema = self.get_table_data_types(df, force_nvarchar)

            if not self.db.table_exists(table_name):
                # Create new table with the DataFrame's schema
                return self.db.create_table(table_name, target_schema)

            # Get current table schema
            schema_query = f"""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
            """
            current_schema = self.db.execute_read_query(schema_query)

            # Convert current schema to a dictionary
            current_columns = {
                row["COLUMN_NAME"]: {
                    "type": row["DATA_TYPE"],
                    "max_length": row["CHARACTER_MAXIMUM_LENGTH"],
                    "nullable": row["IS_NULLABLE"] == "YES",
                }
                for row in current_schema
            }

            # Find missing columns and columns that need type updates
            missing_columns = {}
            type_updates = {}

            for column, target_type in target_schema.items():
                if column not in current_columns:
                    missing_columns[column] = target_type
                else:
                    current_type = current_columns[column]["type"]
                    # Check if type needs to be updated
                    if current_type != target_type:
                        type_updates[column] = target_type

            # Add missing columns
            if missing_columns:
                alter_queries = []
                for column, data_type in missing_columns.items():
                    alter_queries.append(
                        f"ALTER TABLE [{table_name}] ADD [{column}] {data_type}"
                    )
                self.db.execute_multiple_write_queries(";".join(alter_queries))

            # Update column types if needed
            if type_updates:
                alter_queries = []
                for column, new_type in type_updates.items():
                    alter_queries.append(
                        f"ALTER TABLE [{table_name}] ALTER COLUMN [{column}] {new_type}"
                    )
                self.db.execute_multiple_write_queries(";".join(alter_queries))

            return True

        except Exception as e:
            self.log.error(f"Error synchronizing table schema: {str(e)}")
            return False

    def generate_merge_query(
        self, table_name: str, df: DataFrame, force_nvarchar: bool = False
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a SQL MERGE INTO query using a temporary table approach.

        NOTE: The source table needs to be added to the query as {{source_table}} using
        string.format.

        Args:
            table_name (str): The name of the table to merge into.
            df (DataFrame): The DataFrame containing the data to merge.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).

        Returns:
            tuple(str, Iterator[dict[str, Any]]): The prepared SQL query and the
                iterator of dictionaries where the keys are the column names and the
                index names and the values are the corresponding values for each row.
        """
        indices = df.index.names
        reset_df = df.reset_index()
        assert indices is not None, "Index names are required"
        data_types = self.get_table_data_types(reset_df, force_nvarchar)

        # Columns to be used in the MERGE statement
        columns_to_insert = [c for c in reset_df.columns]
        columns_to_update = [c for c in reset_df.columns if c not in indices]

        # Build the query parts
        temp_schema = ", ".join([f"[{c}] {data_types[c]}" for c in columns_to_insert])
        temp_schema += f", PRIMARY KEY ({', '.join(['[' + k + ']' for k in indices])})"
        update = ", ".join([f"target.[{c}] = source.[{c}]" for c in columns_to_update])

        # Add COLLATE clause for string comparisons to handle collation conflicts
        match_conditions = " AND ".join(
            [
                (
                    f"source.[{k}] COLLATE SQL_Latin1_General_CP1_CI_AS = "
                    f"target.[{k}] COLLATE SQL_Latin1_General_CP1_CI_AS"
                    if data_types.get(k, "").startswith("NVARCHAR")
                    else f"source.[{k}] = target.[{k}]"
                )
                for k in indices
            ]
        )

        # Construct the complete query with temporary table
        query = f"""
        MERGE [{table_name}] AS target
        USING {{source_table}} AS source
        ON ({match_conditions})
        WHEN MATCHED THEN
            UPDATE SET {update}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(['[' + c + ']' for c in columns_to_insert])})
            VALUES ({', '.join([f'source.[{c}]' for c in columns_to_insert])});
        """
        return query, self.prepare_values(df, force_nvarchar)

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

    def execute_write_bulk_query(
        self, query: str, rows: list[dict[str, Any]] = []
    ) -> bool:
        """Delegate to the database handler"""
        return self.db.execute_write_bulk_query(query, rows)

    def execute_write_batch_query(
        self,
        query: str,
        rows: Iterator[dict[str, Any]] | list[dict[str, Any]] = [],
        batch_size: int = 1000,
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

    def split_query_statements(self, query: str) -> list[str]:
        """Delegate to the database handler"""
        return self.db.split_query_statements(query)
