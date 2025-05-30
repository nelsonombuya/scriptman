from typing import Any

from dill import dumps
from loguru import logger
from pydantic import BaseModel

# Common serialization configurations
SERIALIZE_FOR_CACHE = {"fallback_to_str": True}  # For cache keys
SERIALIZE_FOR_JSON = {"fallback_to_str": True}  # For JSON responses
SERIALIZE_FOR_PICKLE = {"use_pickle": True, "fallback_to_str": True}  # For pickling


def serialize(
    value: Any,
    use_pickle: bool = False,
    fallback_to_str: bool = False,
) -> Any:
    """
    🔄 Serialize a value for cache key generation or pickling.

    This function handles various types of objects and converts them into
    serializable formats. It can operate in two modes:
    1. Normal serialization mode (default) - converts objects to basic Python types
    2. Pickle mode - attempts to pickle objects, with fallback to serialization

    The function can handle:
    - Single values (any type)
    - Dictionaries (with nested structures)
    - Pydantic BaseModels
    - Lists and tuples
    - Datetime objects
    - Decimal objects
    - Exceptions

    Args:
        value: The value to serialize. Can be any type, a dictionary, or a BaseModel
        use_pickle: Whether to attempt pickling first (default: False)
        fallback_to_str: Whether to convert non-serializable values to strings
            (default: False)

    Returns:
        The serialized value. If input was a dict or BaseModel, returns a dict.
        Otherwise returns the serialized value.

    Examples:
        >>> serialize({"a": 1, "b": datetime.now()})
        {'a': 1, 'b': '2024-03-14T12:00:00'}
        >>> serialize(MyBaseModel())
        {'field1': 'value1', 'field2': 2}
        >>> serialize([1, 2, datetime.now()])
        [1, 2, '2024-03-14T12:00:00']
    """
    from datetime import datetime
    from decimal import Decimal

    try:

        if use_pickle:
            try:
                dumps(value)  # Test if value is picklable
                return value  # Return original value if picklable
            except (TypeError, OverflowError):
                logger.debug(
                    "Value could not be pickled, "
                    f"falling back to value serialization: {value}"
                )

        if isinstance(value, BaseModel):
            return serialize(value.model_dump(), use_pickle, fallback_to_str)

        if isinstance(value, dict):
            return {
                k: serialize(v, use_pickle, fallback_to_str) for k, v in value.items()
            }

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, Exception):
            return {
                "type": value.__class__.__name__,
                "message": str(value),
                "args": value.args,
            }

        if isinstance(value, (list, tuple)):
            return [serialize(item, use_pickle, fallback_to_str) for item in value]

        if fallback_to_str:
            return str(value)

        return value
    except Exception as e:
        logger.error(f"Error serializing value: {e}")
        if fallback_to_str:
            return str(value)
        raise
