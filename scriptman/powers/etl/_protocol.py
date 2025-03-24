from typing import Any, Iterator, Optional

from typing_extensions import Protocol


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

    def execute_write_bulk_query(
        self, query: str, rows: list[dict[str, Any]] = []
    ) -> bool:
        """🔍 Execute a write bulk query on the database"""
        ...

    def execute_write_batch_query(
        self,
        query: str,
        rows: Iterator[dict[str, Any]] | list[dict[str, Any]] = [],
        batch_size: int = 1000,
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
