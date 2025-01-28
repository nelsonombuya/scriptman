from typing import Optional

from pydantic import BaseModel


class DatabaseConfig(BaseModel):
    """
    Database configuration model to store the connection string for the database.
    """

    driver: str
    server: str
    database: str
    username: str
    password: str
    port: Optional[int] = None
