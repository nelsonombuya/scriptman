try:
    from contextlib import contextmanager
    from re import IGNORECASE, search, sub
    from time import time
    from typing import Any, Iterator, Optional

    from loguru import logger
    from pandas import DataFrame

    from scriptman.powers.database._database import DatabaseHandler
    from scriptman.powers.etl._queue import _TableQueueManager
    from scriptman.powers.retry import retry
except ImportError as e:
    raise ImportError(
        f"An error occurred: {e} \n"
        "Kindly install the dependencies on your package manager using scriptman[etl]."
    )


_retry_database_errors = retry(
    retry_condition=DatabaseHandler.retry_conditions,
    base_delay=10.0,
    max_delay=60.0,
    max_retries=5,
)


class ETLDatabase:
    """📦 ETL database operations using composition instead of inheritance

    Features automatic table-level queueing to prevent deadlocks:
    - Automatically extracts table names from SQL queries
    - Queues operations on the same table to prevent conflicts
    - Allows concurrent operations on different tables
    """

    def __init__(
        self,
        database_handler: DatabaseHandler,
        auto_upgrade_to_etl: bool = True,
    ):
        """
        🚀 Initialize ETL database with a database handler and auto-upgrade to heavy ETL
        mode if the database handler supports it.

        Args:
            database_handler: DatabaseHandler object
            auto_upgrade_to_etl: Whether to automatically upgrade to heavy ETL pool
                settings.
        """
        self.db = database_handler
        self.log = logger.bind(
            database=self.db.database_name,
            handler=self.db.__class__.__name__,
        )

        if auto_upgrade_to_etl and not self.db._is_etl_mode:
            try:
                self.log.info("Auto-upgrading to heavy ETL mode...")
                self.db.upgrade_to_etl()
            except Exception as e:
                self.log.warning(f"Failed to auto-upgrade to heavy ETL mode: {e}")
                self.log.info("Continuing with current connection pool settings")

        self._database_identifier: str = f"{self.db.server}:{self.db.database}"
        self._queue_manager: _TableQueueManager = _TableQueueManager()

    @property
    def database_name(self) -> str:
        """🔍 Get the database name from the underlying handler"""
        return self.db.database_name

    @property
    def database_type(self) -> str:
        """🔍 Get the database type from the underlying handler"""
        return self.db.database_type

    def _get_table_key(self, table_name: str) -> str:
        """🔑 Generate unique key for table across database instances"""
        return f"{self._database_identifier}:{table_name}"

    def _extract_table_name_from_query(self, query: str) -> str:
        """🔍 Extract table name from SQL query for automatic queue management"""
        if not query or not isinstance(query, str):
            return ""

        # Clean up the query - remove extra whitespace and normalize
        clean_query = sub(r"\s+", " ", query.strip())

        # Store original query for case-sensitive table name extraction
        original_query = clean_query
        clean_query = clean_query.upper()

        # Patterns for different SQL operations
        patterns = [
            # MERGE patterns (for MSSQL/Oracle) - MUST be first to avoid UPDATE SET
            # confusion
            r'MERGE\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))' r"\s+AS\s+TARGET",
            r'MERGE\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # INSERT INTO patterns
            r'INSERT\s+INTO\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # UPDATE patterns (exclude UPDATE SET from MERGE queries)
            r"(?<!THEN\s)UPDATE\s+(?!\s*SET\s)" r'(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # DELETE FROM patterns
            r'DELETE\s+FROM\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # TRUNCATE patterns
            r'TRUNCATE\s+TABLE\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # DROP TABLE patterns
            r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"
            r'(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # CREATE TABLE patterns
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
            r'(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
            # ALTER TABLE patterns
            r'ALTER\s+TABLE\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',
        ]

        for pattern in patterns:
            # Search in uppercase query for pattern matching
            if search(pattern, clean_query):
                # But extract from original query to preserve case
                original_match = search(pattern, original_query, IGNORECASE)
                if original_match:
                    # Find the first non-None group (handles bracketed, quoted,
                    # and unquoted table names)
                    table_name = None
                    for group in original_match.groups():
                        if group:
                            table_name = group
                            break

                    if table_name:
                        # Remove any remaining quotes or brackets and clean up
                        table_name = table_name.strip('"[]`')
                        # Handle schema.table format - extract just the table name
                        if "." in table_name:
                            table_name = table_name.split(".")[-1]

                        self.log.debug(f"Extracted table name '{table_name}' from query")
                        return table_name

        self.log.debug("🔍 Could not extract table name from query")
        return ""

    def _init_table_queue(self, table_name: str) -> None:
        """🚦 Initialize table-specific queue if not exists"""
        table_key = self._get_table_key(table_name)
        self._queue_manager.get_semaphore(table_key)
        self.log.debug(f"🚦 ETL queue initialized for table '{table_name}'")

    @contextmanager
    def _table_operation_lock(self, operation_type: str, query: str) -> Iterator[None]:
        """🚦 Context manager for table-specific ETL operations"""
        # Extract table name from query
        table_name = self._extract_table_name_from_query(query)

        # If no table name, execute without queue
        if not table_name:
            self.log.debug("🔍 No table name available - executing without queue")
            yield
            return

        # Initialize queue for this table if needed and trigger cleanup
        self.log.debug(f"🚦 Initializing queue for table '{table_name}'")
        self._init_table_queue(table_name)
        self._queue_manager.cleanup_if_needed()

        table_key = self._get_table_key(table_name)
        operation_id = f"{operation_type}_{int(time() * 1000)}"
        semaphore = self._queue_manager.get_semaphore(table_key)
        active_ops = self._queue_manager.get_active_operations(table_key)

        self.log.debug(
            f"🎫 Requesting ETL lock for {operation_type} on table '{table_name}'"
        )

        # Acquire semaphore (blocks if table is locked by another operation)
        semaphore.acquire()
        try:
            with self._queue_manager._queue_lock:
                active_ops.add(operation_id)

            self.log.info(f"🚀 Starting {operation_type} on table '{table_name}'")

            yield

        finally:
            with self._queue_manager._queue_lock:
                active_ops.discard(operation_id)

            semaphore.release()

            self.log.info(f"✅ Completed {operation_type} on table '{table_name}'")

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
        self,
        table_name: str,
        df: DataFrame,
        force_nvarchar: bool = False,
        use_logical_keys: bool = False,
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a prepared SQL upsert query for the given table and DataFrame.

        Args:
            table_name (str): The name of the table to upsert into.
            df (DataFrame): The DataFrame containing the data to upsert.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).
            use_logical_keys (bool): If True, uses logical keys for WHERE clauses
                without database constraints.

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
            query, values = self.generate_merge_query(
                table_name, df, var, use_logical_keys
            )

        assert query is not None, "Unsupported database type"
        assert values is not None, "No values to upsert"
        return query, values

    @_retry_database_errors
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
        # Get the target schema from the DataFrame
        target_schema = self.get_table_data_types(df, force_nvarchar)

        if not self.db.table_exists(table_name):
            # Create new table with the DataFrame's schema
            return self.db.create_table(table_name, target_schema)

        # Get current table schema
        # Use ALTER TABLE syntax to trigger table extraction and queueing
        schema_query = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
        """
        # Execute through our queueing system to prevent deadlocks
        with self._table_operation_lock("schema_read", f"ALTER TABLE {table_name}"):
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

        # Add missing columns (execute through queue to prevent deadlocks)
        if missing_columns:
            alter_queries = []
            for column, data_type in missing_columns.items():
                alter_queries.append(
                    f"ALTER TABLE [{table_name}] ADD [{column}] {data_type}"
                )
            combined_query = ";".join(alter_queries)
            with self._table_operation_lock("alter_add_columns", combined_query):
                self.db.execute_multiple_write_queries(combined_query)

        # Update column types if needed (execute through queue to prevent deadlocks)
        if type_updates:
            alter_queries = []
            for column, new_type in type_updates.items():
                alter_queries.append(
                    f"ALTER TABLE [{table_name}] ALTER COLUMN [{column}] {new_type}"
                )
            combined_query = ";".join(alter_queries)
            with self._table_operation_lock("alter_update_columns", combined_query):
                self.db.execute_multiple_write_queries(combined_query)

        return True

    def generate_merge_query(
        self,
        table_name: str,
        df: DataFrame,
        force_nvarchar: bool = False,
        use_logical_keys: bool = False,
    ) -> tuple[str, Iterator[dict[str, Any]]]:
        """
        ✍🏾 Generates a SQL MERGE INTO query using a temporary table approach.

        NOTE: The source table needs to be added to the query as {{source_table}} using
        string.format.

        Args:
            table_name (str): The name of the table to merge into.
            df (DataFrame): The DataFrame containing the data to merge.
            force_nvarchar (bool): Whether to force all columns to be NVARCHAR(MAX).
            use_logical_keys (bool): If True, uses logical keys for WHERE clauses
                without database constraints.

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

        # Only add PRIMARY KEY constraint if not using logical keys
        if not use_logical_keys:
            temp_schema += f", PRIMARY KEY ({', '.join([f'[{k}]' for k in indices])})"
        update = ", ".join([f"target.[{c}] = source.[{c}]" for c in columns_to_update])

        # Add COLLATE clause for string comparisons to handle collation conflicts
        match_conditions = " AND ".join(
            [
                (
                    f"source.[{k}] COLLATE SQL_Latin1_General_CP1_CI_AS = "
                    f"target.[{k}] COLLATE SQL_Latin1_General_CP1_CI_AS"
                    if data_types.get(str(k), "").startswith("NVARCHAR")
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

    @_retry_database_errors
    def execute_read_query(
        self, query: str, params: dict[str, Any] = {}
    ) -> list[dict[str, Any]]:
        """Execute read query with automatic retry and table-level queueing"""
        # Execute with table-level queueing
        with self._table_operation_lock("read", query):
            return self.db.execute_read_query(query, params)

    @_retry_database_errors
    def execute_write_query(
        self,
        query: str,
        params: dict[str, Any] = {},
        check_affected_rows: bool = False,
    ) -> bool:
        """Execute write query with automatic retry and table-level queue management"""
        with self._table_operation_lock("write", query):
            return self.db.execute_write_query(query, params, check_affected_rows)

    @_retry_database_errors
    def execute_write_bulk_query(
        self, query: str, rows: list[dict[str, Any]] = []
    ) -> bool:
        """Execute bulk write query with automatic retry and table-level queueing"""
        with self._table_operation_lock("bulk_write", query):
            return self.db.execute_write_bulk_query(query, rows)

    @_retry_database_errors
    def execute_write_batch_query(
        self,
        query: str,
        rows: Iterator[dict[str, Any]] | list[dict[str, Any]] = [],
        batch_size: int = 1000,
    ) -> bool:
        """Execute batch write query with automatic retry and table-level queueing"""
        with self._table_operation_lock("batch_write", query):
            return self.db.execute_write_batch_query(query, rows, batch_size)

    @_retry_database_errors
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists with automatic retry on pool timeout errors"""
        return self.db.table_exists(table_name)

    @_retry_database_errors
    def create_table(
        self, table_name: str, columns: dict[str, str], keys: Optional[list[str]] = None
    ) -> bool:
        """Delegate to the database handler"""
        return self.db.create_table(table_name, columns, keys)

    @_retry_database_errors
    def create_table_with_logical_keys(
        self, table_name: str, columns: dict[str, str], logical_keys: list[str]
    ) -> bool:
        """
        🔨 Creates a table with logical keys (no database constraints) but stores the
        logical key information for use in WHERE clauses.

        Args:
            table_name (str): The name of the table.
            columns (dict[str, str]): A dictionary of column names and their data types.
            logical_keys (list[str]): A list of column names to use as logical keys
                for WHERE clauses in update/merge operations.

        Returns:
            bool: True if the table was created, False otherwise.
        """
        # Store logical keys information for later use in queries
        if not hasattr(self, "_logical_keys"):
            self._logical_keys = {}
        self._logical_keys[table_name] = logical_keys

        # Create table without actual database constraints
        return self.db.create_table(table_name, columns, keys=None)

    def get_logical_keys(self, table_name: str) -> Optional[list[str]]:
        """
        🔍 Get the logical keys for a table if they were set using
        create_table_with_logical_keys.

        Args:
            table_name (str): The name of the table.

        Returns:
            Optional[list[str]]: The logical keys for the table, or None if not set.
        """
        if hasattr(self, "_logical_keys"):
            return self._logical_keys.get(table_name)
        return None

    @_retry_database_errors
    def truncate_table(self, table_name: str) -> bool:
        """Delegate to the database handler"""
        return self.db.truncate_table(table_name)

    @_retry_database_errors
    def drop_table(self, table_name: str) -> bool:
        """Delegate to the database handler"""
        return self.db.drop_table(table_name)

    @_retry_database_errors
    def split_query_statements(self, query: str) -> list[str]:
        """Delegate to the database handler"""
        return self.db.split_query_statements(query)
