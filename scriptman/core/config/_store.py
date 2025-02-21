from typing import Any

from loguru import logger

from scriptman.core.defaults import ConfigModel


class ConfigStore:
    """📦 Base class for storing and managing configuration values."""

    def __init__(self) -> None:
        """
        🚀 Initialize the configuration store.

        This method initializes the configuration store with an empty dictionary.
        """
        object.__setattr__(self, "_store", {})

    def __getitem__(self, key: str) -> Any:
        """
        🔍 Retrieve a value from the configuration store.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            Any: The value associated with the given key.

        Raises:
            KeyError: If the key is not present in the configuration store.
        """
        if value := self.get(key):
            return value
        raise KeyError(f"Key not found: {key}")

    def __setitem__(self, key: str, value: Any) -> None:
        """
        ✍️ Set a configuration value in the store.

        This method sets a configuration value in the store using the given key and value.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to set for the given key.
        """
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        """
        🗑️ Delete a configuration value from the store.

        This method deletes a configuration value from the store using the given key.

        Args:
            key (str): The key to delete the value for.

        Raises:
            KeyError: If the key is not present in the configuration store.
        """
        self.delete(key)

    def __contains__(self, key: str) -> bool:
        """
        🔍 Check if a key exists in the configuration store.

        This method checks if the given key is present in the configuration store.

        Args:
            key (str): The key to check for existence.

        Returns:
            bool: True if the key exists in the store, otherwise False.
        """
        return key in self._store

    def get(self, key: str, default: Any = None) -> Any:
        """
        🔍 Retrieve a value from the configuration store.

        This method retrieves a value from the configuration store using the given key.

        Args:
            key (str): The key to retrieve the value for.
            default (Any): The default value to return if the key is not present in the
                configuration store.

        Returns:
            Any: The value associated with the given key or the default value if the key
                is not present in the configuration store.
        """
        return self._store.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        ✍️ Set a value in the configuration store.

        This method sets a value in the configuration store using the specified key.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to set for the given key.
        """
        self._store[key] = value

    def delete(self, key: str) -> None:
        """
        🗑️ Delete a key-value pair from the configuration store.

        This method deletes a key-value pair from the configuration store using the given
        key.

        Args:
            key (str): The key to delete the value for.

        Raises:
            KeyError: If the key is not present in the configuration store.
        """
        try:
            del self._store[key]
        except KeyError:
            logger.error(f"Key not found: {key}")

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
        if field := ConfigModel.model_fields.get(key):
            default = field.get_default()
        else:
            default = None

        self._store[key] = default
        logger.info(f"Config reset: {key} = {default}")

    def reset_all(self) -> None:
        """
        🔄 Reset all key-value pairs in the configuration store.

        This method resets all key-value pairs in the configuration store to their default
        values.
        """
        for key in self._store.keys():
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

    def __getattr__(self, name: str) -> Any:
        """
        🔍 Retrieve a value from the configuration store.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            Any: The value associated with the given key.
        """
        if value := self._store.get(name):
            return value
        raise AttributeError(f"Key not found: {name}")

    def __setattr__(self, name: str, value: Any) -> None:
        """
        💻 Set a value in the configuration store.

        Args:
            key (str): The key to set the value for.
            value (Any): The value to set for the given key.
        """
        if name == "_store":  # Allow setting the _store attribute directly
            object.__setattr__(self, name, value)
        else:  # Store other attributes in _store
            self._store[name] = value

    def __delattr__(self, name: str) -> None:
        """
        🗑️ Delete a value from the configuration store.

        Args:
            key (str): The key to delete the value for.
        """
        if name == "_store":
            raise AttributeError("Cannot delete _store attribute")
        self.delete(name)
