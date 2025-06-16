from threading import RLock, Thread
from time import sleep

from loguru import logger

from scriptman.powers.tasks._hybrid_executor import HybridExecutor


class DynamicPoolManager:
    """🎛️ Manages dynamic creation and cleanup of hybrid executors"""

    def __init__(self, base_threads: int = 8, base_processes: int = 2):
        assert base_threads > 0, "base_threads must be greater than 0"
        assert base_processes >= 0, "base_processes must be greater than or equal to 0"

        # Initialize base threads and processes
        self.base_threads: int = base_threads
        self.base_processes: int = base_processes

        # Initialize executors and lock
        self._running: bool = True
        self._lock: RLock = RLock()
        self.executor_counter: int = 0
        self.executors: list[HybridExecutor] = []
        self._cleanup_thread: Thread | None = None

        # Create initial executor
        self.executors.append(HybridExecutor(base_threads, base_processes))

        # Start cleanup thread
        self._cleanup_thread = Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def get_available_executor(self) -> HybridExecutor:
        """🔍 Get an available executor or create a new one"""
        with self._lock:
            # Ensure we have at least one executor
            if not self.executors:
                return self.spawn_new_executor()

            # Find executor with lowest load
            best_executor = min(self.executors, key=lambda e: e.get_load())

            # If best executor is heavily loaded, spawn new one
            if best_executor.get_load() > 0.8:
                new_executor = self.spawn_new_executor()
                return new_executor

            return best_executor

    def spawn_new_executor(self) -> HybridExecutor:
        """🚀 Spawn a new hybrid executor"""
        with self._lock:
            self.executor_counter += 1

            # Scale down resources for additional executors
            threads: int = max(2, self.base_threads // (len(self.executors) + 1))
            processes: int = max(1, self.base_processes // (len(self.executors) + 1))
            executor: HybridExecutor = HybridExecutor(threads, processes)
            self.executors.append(executor)

            logger.info(
                f"Spawned new executor #{self.executor_counter} "
                f"(threads={threads}, processes={processes})"
            )
            return executor

    def cleanup_idle_executors(self) -> None:
        """🧹 Remove idle executors to free resources"""
        with self._lock:
            if len(self.executors) <= 1:
                return  # Keep at least one executor

            # Remove idle executors except the first one
            for executor in [e for e in self.executors[1:] if e.is_idle()]:
                self.executors.remove(executor)
                executor.shutdown(wait=False)
                logger.info("Cleaned up idle executor")

    def _cleanup_loop(self) -> None:
        """🔄 Background cleanup loop"""
        while self._running:
            try:
                self.cleanup_idle_executors()
                sleep(30.0)  # Check every 30 seconds
            except Exception as e:
                logger.warning(f"Cleanup loop error: {e}")

    def shutdown(self) -> None:
        """🛑 Shutdown all executors"""
        logger.info("Shutting down DynamicPoolManager")
        self._running = False

        with self._lock:
            for executor in self.executors:
                executor.shutdown(wait=False)
            self.executors.clear()
