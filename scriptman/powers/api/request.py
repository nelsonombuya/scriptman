from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class Request(BaseModel):
    """
    🚀 Represents a request object.

    Attributes:
        request_id (str): The unique identifier for the request.
        timestamp (str): The timestamp when the request was created.
    """

    request_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    model_config = ConfigDict(extra="forbid")
