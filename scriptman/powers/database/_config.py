from typing import Optional

from pydantic import BaseModel


class DatabaseConfig(BaseModel):
    """⚙ Database configuration model to store the connection string for the database."""

    driver: str
    server: str
    database: str
    username: str
    password: str
    port: Optional[int] = None


class Schema(BaseModel):
    """📝 Pydantic model for database schema configuration"""

    table_name: str
    columns: dict[str, str]
    keys: list[str] | None = None
