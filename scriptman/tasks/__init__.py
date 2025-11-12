from __future__ import annotations

from threading import Event, RLock, Thread
from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Callable,
    Mapping,
    MutableMapping,
    ParamSpec,
    TypeVar,
    cast,
)

from loguru import logger

from scriptman.orchestrator import (
    RuntimeContext,
    RuntimeContextBuilder,
    RuntimeOrchestrator,
)
from scriptman.orchestrator._context import ExecutorRegistry, ExecutorStrategy
from scriptman.orchestrator._events import SynchronousEventBus
from scriptman.orchestrator._generics import Func
from scriptman.orchestrator.tasks import (
    InMemoryTaskQueue,
    InMemoryTaskRegistryStore,
    SynchronousTaskExecutor,
    TaskConfigBinding,
    TaskEntry,
    TaskExecutionResult,
)
from scriptman.orchestrator.tasks import TaskRegistry as TaskRegistryProtocol
from scriptman.orchestrator.tasks import (
    TaskResourceSpec,
    TaskRetryPolicy,
    TasksFacade,
    TaskSlaPolicy,
    TaskSubmission,
    default_task_logging,
)
from scriptman.orchestrator.tasks._registry import TaskRegistry as TaskRegistryImpl
from scriptman.orchestrator.tasks._reporter import TaskReporter
from scriptman.orchestrator.workloads import WorkloadQueue

TaskParams = ParamSpec("TaskParams")
TaskReturn = TypeVar("TaskReturn")
TaskFunc = Func[TaskParams, TaskReturn]


class _InlineExecutorStrategy(ExecutorStrategy):
    name = "inline"

    def acquire(self) -> ExecutorStrategy:
        return self

    def release(self) -> None:  # pragma: no cover - no-op
        return


class _InlineExecutorRegistry(ExecutorRegistry):
    def __init__(self) -> None:
        self._strategies: MutableMapping[str, ExecutorStrategy] = {
            "inline": _InlineExecutorStrategy()
        }

    def get(self, name: str) -> ExecutorStrategy:
        try:
            return self._strategies[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"⚠️ Unknown executor strategy '{name}'") from exc

    def register(self, strategy: ExecutorStrategy) -> None:
        self._strategies[strategy.name] = strategy


class _QueueWorker(Thread):
    """Background worker that drains the task queue."""

    def __init__(
        self,
        *,
        facade: TasksFacade,
        queue: WorkloadQueue[TaskSubmission],
        context: RuntimeContext,
        reporter: TaskReporter,
        on_complete: Callable[[str, TaskExecutionResult], None],
    ) -> None:
        super().__init__(daemon=True, name="scriptman-task-worker")
        self._facade = facade
        self._queue = queue
        self._context = context
        self._reporter = reporter
        self._on_complete = on_complete
        self._wake = Event()
        self._stopped = Event()

    def run(self) -> None:  # pragma: no cover - thin wrapper around facade
        while not self._stopped.is_set():
            item = self._queue.dequeue(context=self._context)
            if item is None:
                self._wake.wait(0.1)
                self._wake.clear()
                continue

            task_id, _submission = item
            try:
                result = self._facade.execute(task_id=task_id)
            except Exception as exc:  # pragma: no cover - defensive logging
                self._context.logger.error(
                    "⚠️ Task execution crashed", task_id=task_id, error=str(exc)
                )
                self._queue.complete(task_id, context=self._context)
                continue

            self._queue.complete(task_id, context=self._context)
            self._reporter.report(result, context=self._context)
            self._on_complete(task_id, result)

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stopped.set()
        self._wake.set()


class TaskManager:
    """Beginner-friendly façade over the Command Deck task runtime."""

    def __init__(
        self,
        *,
        orchestrator: RuntimeOrchestrator | None = None,
        context: RuntimeContext | None = None,
        queue: WorkloadQueue[TaskSubmission] | None = None,
        registry: TaskRegistryProtocol | None = None,
        executor: SynchronousTaskExecutor | None = None,
        reporter: TaskReporter | None = None,
    ) -> None:
        """
        🚀 Task Manager façade over the Command Deck task runtime.

        Args:
            orchestrator: The runtime orchestrator.
            context: The runtime context.
            queue: The task queue.
            registry: The task registry.
            executor: The task executor.
            reporter: The task reporter.
        """
        self._logger = logger.bind(component="task-manager")
        self._external_orchestrator = orchestrator is not None

        if orchestrator is not None:
            self._orchestrator = orchestrator
            self._context = orchestrator.context
        else:
            if context is None:
                context = self._build_default_context()
            self._context = context
            self._orchestrator = RuntimeOrchestrator(
                event_publisher=context.event_publisher,
                executor_registry=context.executor_registry,
                metadata=context.metadata,
                config=context.config,
            )
            self._orchestrator.bootstrap()
            self._orchestrator.hydrate()
            self._orchestrator.enter_runtime()

        self._queue = queue or InMemoryTaskQueue()
        if registry is None:
            registry_impl = TaskRegistryImpl(store=InMemoryTaskRegistryStore())
            registry_to_use = cast(TaskRegistryProtocol, registry_impl)
        else:
            registry_to_use = registry
        self._registry: TaskRegistryProtocol = registry_to_use
        self._executor = executor or SynchronousTaskExecutor()
        self._reporter = reporter or _LoggingTaskReporter()
        self._facade = TasksFacade(
            context=self._context,
            registry=self._registry,
            queue=self._queue,
            executor=self._executor,
            reporter=self._reporter,
        )

        self._task_names: MutableMapping[Callable[..., Awaitable[Any] | Any], str] = {}
        self._runs: MutableMapping[str, TaskExecutionResult] = {}
        self._run_events: MutableMapping[str, Event] = {}
        self._run_lock = RLock()
        self._closed = False

        self._worker = _QueueWorker(
            facade=self._facade,
            queue=self._queue,
            context=self._context,
            reporter=self._reporter,
            on_complete=self._complete_run,
        )
        self._worker.start()

    # --------------------------------------------------------------------- #
    # Registration helpers
    # --------------------------------------------------------------------- #
    def register(self, entry: TaskEntry) -> TaskEntry:
        """Register a pre-built task entry."""
        self._facade.register(entry)
        self._task_names[entry.target] = entry.name
        return entry

    def task(
        self,
        *,
        name: str | None = None,
        metadata: Mapping[str, object] | None = None,
        logging: Any | None = None,
        retry: TaskRetryPolicy | None = None,
        resources: TaskResourceSpec | None = None,
        sla: TaskSlaPolicy | None = None,
        config: TaskConfigBinding | None = None,
    ) -> Callable[
        [TaskFunc[TaskParams, TaskReturn]],
        TaskFunc[TaskParams, TaskReturn],
    ]:
        """Decorator for registering a callable as a task."""

        def decorator(
            func: TaskFunc[TaskParams, TaskReturn],
        ) -> TaskFunc[TaskParams, TaskReturn]:
            descriptor = TaskEntry(
                name=name or func.__name__,
                target=func,
                metadata=metadata or {},
                logging=logging or default_task_logging(),
                retry=retry or TaskRetryPolicy(),
                resources=resources or TaskResourceSpec(),
                sla=sla or TaskSlaPolicy(),
                config=config or TaskConfigBinding(),
            )
            self.register(descriptor)
            return func

        return decorator

    # --------------------------------------------------------------------- #
    # Execution API
    # --------------------------------------------------------------------- #
    def enqueue(
        self,
        task: str | Callable[..., Awaitable[Any] | Any],
        /,
        *args: Any,
        metadata: Mapping[str, object] | None = None,
        correlation_id: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Enqueue a task for asynchronous execution."""

        name = self._resolve_task_name(task)
        run_id = self._facade.enqueue(
            name,
            *args,
            metadata=metadata,
            correlation_id=correlation_id,
            **kwargs,
        )
        self._register_run(run_id)
        self._worker.wake()
        return run_id

    def run_now(
        self,
        task: str | Callable[..., Awaitable[Any] | Any],
        /,
        *args: Any,
        metadata: Mapping[str, object] | None = None,
        **kwargs: Any,
    ) -> TaskExecutionResult:
        """Execute a task immediately, bypassing the queue."""

        name = self._resolve_task_name(task)
        return self._facade.execute(
            name=name,
            args=tuple(args),
            kwargs=dict(kwargs),
            metadata=metadata or {},
        )

    def await_run(self, run_id: str, timeout: float | None = None) -> TaskExecutionResult:
        """Block until a queued run completes and return its result."""

        with self._run_lock:
            event = self._run_events.setdefault(run_id, Event())
        if not event.wait(timeout):
            raise TimeoutError(f"Timed out waiting for task run {run_id!r}")
        with self._run_lock:
            result = self._runs.pop(run_id, None)
            self._run_events.pop(run_id, None)
        if result is None:
            raise RuntimeError(f"No result recorded for task run {run_id!r}")
        return result

    def inspect(self, name: str) -> TaskEntry:
        """Return the entry for a registered task."""
        return self._facade.inspect(name)

    # --------------------------------------------------------------------- #
    # Lifecycle management
    # --------------------------------------------------------------------- #
    def shutdown(self, *, wait: bool = True) -> None:
        """Stop background workers and optionally shut down the orchestrator."""
        if self._closed:
            return
        self._worker.stop()
        if wait:
            self._worker.join()
        if not self._external_orchestrator:
            self._orchestrator.shutdown()
        self._closed = True

    def __enter__(self) -> "TaskManager":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown(wait=True)

    def __del__(self) -> None:  # pragma: no cover - best-effort cleanup
        try:
            self.shutdown(wait=False)
        except Exception:
            pass

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #
    def _build_default_context(self) -> RuntimeContext:
        bus = SynchronousEventBus()
        registry = _InlineExecutorRegistry()
        builder = RuntimeContextBuilder()
        builder.with_event_publisher(bus)
        builder.with_executor_registry(registry)
        return builder.build()

    def _resolve_task_name(self, task: str | Callable[..., Awaitable[Any] | Any]) -> str:
        if isinstance(task, str):
            return task
        try:
            return self._task_names[task]
        except KeyError as exc:
            name = getattr(task, "__name__", repr(task))
            raise RuntimeError(f"⚠️ Callable {name} is not registered as a task") from exc

    def _register_run(self, run_id: str) -> None:
        with self._run_lock:
            self._run_events.setdefault(run_id, Event())

    def _complete_run(self, run_id: str, result: TaskExecutionResult) -> None:
        with self._run_lock:
            self._runs[run_id] = result
            event = self._run_events.get(run_id)
        if event:
            event.set()

    # --------------------------------------------------------------------- #
    # Public surface re-export
    # --------------------------------------------------------------------- #
    @property
    def context(self) -> RuntimeContext:
        return self._context

    @property
    def orchestrator(self) -> RuntimeOrchestrator:
        return self._orchestrator


class _LoggingTaskReporter(TaskReporter):
    """🪵 Emit succinct summaries for each task execution using context logger."""

    def report(
        self,
        result: TaskExecutionResult,
        *,
        context: RuntimeContext,
    ) -> None:
        bound = context.logger.bind(
            component="task-reporter",
            workload=result.submission.entry.name,
            run_id=result.task_id,
            outcome=result.outcome,
        )
        if result.error:
            bound.error(
                "⚠️ Task failed",
                metadata=result.submission.metadata,
                detail=result.detail,
                error=str(result.error),
            )
            return
        bound.info(
            "✅ Task completed",
            metadata=result.submission.metadata,
            detail=result.detail,
            result=result.result,
        )


__all__ = [
    "TaskManager",
    "TaskEntry",
    "TaskExecutionResult",
    "TaskRetryPolicy",
    "TaskSlaPolicy",
    "TaskResourceSpec",
    "TaskConfigBinding",
]
