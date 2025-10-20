from threading import RLock, Semaphore
from time import time
from typing import Optional

from loguru import logger


class _TableQueueManager:
    """🚦 Singleton manager for table-level queues"""

    __instance: Optional["_TableQueueManager"] = None
    __lock: RLock = RLock()

    def __new__(cls) -> "_TableQueueManager":
        """🔍 Get or create a table queue manager instance"""
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:
                    cls.__instance = super().__new__(cls)
                    cls.__instance.__initialized = False
        return cls.__instance

    def __init__(self) -> None:
        """🔍 Initialize the table queue manager"""
        if not hasattr(self, "__initialized") or not self.__initialized:
            self.__table_semaphores: dict[str, Semaphore] = {}
            self.__active_operations: dict[str, set[str]] = {}
            self._queue_lock: RLock = RLock()
            self.__last_cleanup = time()
            self.__initialized: bool = True

    def get_semaphore(self, table_key: str) -> Semaphore:
        """🔍 Get or create a semaphore for the given table key"""
        with self._queue_lock:
            if table_key not in self.__table_semaphores:
                # Always 1 to prevent deadlocks
                self.__table_semaphores[table_key] = Semaphore(1)
                self.__active_operations[table_key] = set()
            return self.__table_semaphores[table_key]

    def get_active_operations(self, table_key: str) -> set[str]:
        """🔍 Get the active operations set for a table key"""
        with self._queue_lock:
            return self.__active_operations.get(table_key, set())

    def cleanup_if_needed(self) -> None:
        """🧹 Cleanup empty queues periodically"""
        current_time = time()
        # Cleanup every 5 minutes
        if current_time - self.__last_cleanup > 300:
            with self._queue_lock:
                empty_tables = [
                    table_key
                    for table_key, operations in self.__active_operations.items()
                    if len(operations) == 0
                ]
                for table_key in empty_tables:
                    del self.__table_semaphores[table_key]
                    del self.__active_operations[table_key]

                if empty_tables:
                    logger.debug(f"🧹 Cleaned up {len(empty_tables)} empty table queues")

                self.__last_cleanup = current_time
