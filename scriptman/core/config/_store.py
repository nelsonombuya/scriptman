from typing import Any

from loguru import logger

from scriptman.core.config._defaults import ConfigModel


class ConfigStore:
    """
    📦 Base class for storing and managing configuration values with dot notation support.

    This class provides a dictionary-like interface for storing and retrieving
    configuration values. It allows you to access values using dot notation,
    such as "app.settings.debug".
    """

    def __init__(self) -> None:
        """
        🚀 Initialize the configuration store.

        This method initializes the configuration store with an empty dictionary.
        """
        object.__setattr__(self, "_store", {})

    def _traverse_dict(
        self, keys: list[str], create: bool = False
    ) -> tuple[dict[str, Any], str]:
        """
        Traverse the nested dictionary structure to find or create the parent of the
        target key.

        Args:
            keys (list[str]): List of keys to traverse.
            create (bool): Whether to create missing dictionaries along the path

        Returns:
            tuple[dict, str]: Tuple of (parent_dict, last_key)

        Raises:
            KeyError: If a key along the path doesn't exist and create=False
        """
        current = self._store
        *parent_keys, last_key = keys

        for key in parent_keys:
            if create:
                current = current.setdefault(key, {})
            else:
                try:
                    current = current[key]
                    if not isinstance(current, dict):
                        raise KeyError(f"Path element '{key}' is not a dictionary")
                except KeyError:
                    raise KeyError(f"Key not found: {key}")

        return current, last_key

    def get(self, key: str, default: Any = None) -> Any:
        """
        🔍 Retrieve a value using dot notation.

        Args:
            key (str): The key path (e.g., "app.settings.debug")
            default (Any): Value to return if path doesn't exist

        Returns:
            Any: The value at the specified path or default
        """
        try:
            keys = key.split(".")
            if len(keys) == 1:
                return self._store.get(key, default)

            parent, last_key = self._traverse_dict(keys)
            return parent.get(last_key, default)
        except (KeyError, AttributeError):
            return default

    def set(self, key: str, value: Any) -> None:
        """
        ✍🏾 Set a value using dot notation.

        Args:
            key (str): The key path (e.g., "app.settings.debug")
            value (Any): The value to set
        """
        keys = key.split(".")
        if len(keys) == 1:
            self._store[key] = value
            return

        parent, last_key = self._traverse_dict(keys, create=True)
        parent[last_key] = value

    def delete(self, key: str) -> None:
        """
        Delete a value using dot notation.

        Args:
            key (str): The key path (e.g., "app.settings.debug")

        Raises:
            KeyError: If the key path doesn't exist
        """
        try:
            keys = key.split(".")
            if len(keys) == 1:
                del self._store[key]
                return

            parent, last_key = self._traverse_dict(keys)
            del parent[last_key]
        except KeyError as e:
            logger.error(f"Error deleting key '{key}': {str(e)}")

    def __contains__(self, key: str) -> bool:
        """🔍 Check if a key exists in the configuration store using dot notation."""
        try:
            keys = key.split(".")
            if len(keys) == 1:
                return key in self._store

            parent, last_key = self._traverse_dict(keys)
            return last_key in parent
        except (KeyError, AttributeError):
            return False

    def __getitem__(self, key: str) -> Any:
        """🔍 Get a value using dictionary syntax with dot notation support."""
        value = self.get(key)
        if value is None:
            raise KeyError(f"Key not found: {key}")
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        """✍🏾 Set a value using dictionary syntax with dot notation support."""
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        """🚮 Delete a value using dictionary syntax with dot notation support."""
        self.delete(key)

    def __getattr__(self, name: str) -> Any:
        """🔍 Get a value using attribute syntax."""
        if value := self._store.get(name):
            return value
        raise AttributeError(f"Attribute not found: {name}")

    def __setattr__(self, name: str, value: Any) -> None:
        """✍🏾 Set a value using attribute syntax."""
        if name == "_store":
            object.__setattr__(self, name, value)
        else:
            self._store[name] = value

    def __delattr__(self, name: str) -> None:
        """🚮 Delete a value using attribute syntax."""
        if name == "_store":
            raise AttributeError("Cannot delete _store attribute")
        self.delete(name)

    def reset(self, key: str) -> None:
        """
        🔄 Reset a key-value pair in the configuration store.

        This method resets a key-value pair in the configuration store using the given
        key to its default value.

        Args:
            key (str): The key to reset the value for.

        Raises:
            KeyError: If the key is not present in the configuration store.
        """
        default = ConfigModel.get_default(key)
        self.set(key, default)
        logger.debug(f"Key reset: {key} = {default}")

    def reset_all(self) -> None:
        """
        🔄 Reset all key-value pairs in the configuration store.

        This method resets all key-value pairs in the configuration store to their default
        values.
        """
        # Create a copy of the keys to avoid modifying the dictionary while iterating
        for key in list(self._store.keys()):
            self.reset(key)

    def keys(self) -> Any:
        """
        🔑 Retrieve the keys from the configuration store.

        This method retrieves all the keys present in the configuration store.

        Returns:
            Any: An iterable of all the keys in the configuration store.
        """
        return self._store.keys()

    def items(self) -> Any:
        """
        📦 Retrieve the key-value pairs from the configuration store.

        This method retrieves all the key-value pairs present in the configuration store.

        Returns:
            Any: An iterable of tuples, each containing a key and its associated value in
            the configuration store.
        """
        return self._store.items()

    def values(self) -> Any:
        """
        🔓 Retrieve the values from the configuration store.

        This method retrieves all the values present in the configuration store.

        Returns:
            Any: An iterable of all the values in the configuration store.
        """
        return self._store.values()
