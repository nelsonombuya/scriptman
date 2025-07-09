from threading import RLock, Thread
from time import sleep
from typing import Optional

import psutil
from loguru import logger


class ResourceMonitor:
    """📊 Monitors system resources and provides load metrics"""

    def __init__(self) -> None:
        """🔍 Initialize resource monitor"""
        self._lock: RLock = RLock()  # Lock for thread safety

        self._cpu_percent: float = 0.0  # CPU load percentage
        self._memory_percent: float = 0.0  # Memory load percentage

        self._monitoring: bool = False  # Whether monitoring is active
        self._monitor_thread: Optional[Thread] = None  # Thread for monitoring

    def start_monitoring(self) -> None:
        """🚀 Start resource monitoring in background"""
        with self._lock:
            if not self._monitoring:
                self._monitoring = True
                self._monitor_thread = Thread(
                    daemon=True,
                    target=self._monitor_loop,
                    name="TaskMaster - Resource Monitor",
                )
                self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        """🛑 Stop resource monitoring"""
        with self._lock:
            self._monitoring = False
            if self._monitor_thread:
                self._monitor_thread.join(timeout=1.0)

    def _monitor_loop(self) -> None:
        """🔄 Background monitoring loop"""
        # Initialize CPU monitoring (first call is non-blocking)
        psutil.cpu_percent()  # Initialize CPU monitoring

        while self._monitoring:
            try:
                # Use non-blocking call after initialization
                self._cpu_percent = psutil.cpu_percent()
                self._memory_percent = psutil.virtual_memory().percent
            except Exception as e:
                logger.warning(f"Resource monitoring error: {e}")
            sleep(1.0)

    def get_cpu_load(self) -> float:
        """📈 Get current CPU load (0.0 - 1.0)"""
        return self._cpu_percent / 100.0

    def get_memory_load(self) -> float:
        """📈 Get current memory load (0.0 - 1.0)"""
        return self._memory_percent / 100.0

    def get_system_load(self) -> float:
        """📈 Get combined system load metric (0.0 - 1.0)"""
        return max(self.get_cpu_load(), self.get_memory_load())
