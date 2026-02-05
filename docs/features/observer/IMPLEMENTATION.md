# Observer Implementation Guide

> **Feature:** Context-aware observability system for Python automation workflows

**Status:** 🚧 In Development (v3.0)
**Last Updated:** February 5, 2026
**Implementation Phase:** Foundation

---

## 📋 Table of Contents

1. [Feature Overview](#feature-overview)
2. [Goals & Acceptance Criteria](#goals--acceptance-criteria)
3. [Architecture](#architecture)
4. [Implementation Plan](#implementation-plan)
5. [Code Examples](#code-examples)
6. [Testing Requirements](#testing-requirements)
7. [Decision Log](#decision-log)
8. [Progress Tracker](#progress-tracker)
9. [Integration Notes](#integration-notes)
10. [Performance Targets](#performance-targets)
11. [Post-Implementation Review](#post-implementation-review)

---

## 🎯 Feature Overview

### What We're Building

A **function-level observability system** that:

-   Tracks execution flow across nested function calls
-   Propagates context (tracked properties) automatically down the call stack
-   Provides beautiful console output (ASCII tree rendering)
-   Stores events in SQLite for historical analysis
-   Doubles as a testing tool (trace mode)

### The Problem It Solves

**Before Observer:**

```python
def process_invoice(invoice_id: str):
    print(f"[{datetime.now()}] Processing invoice {invoice_id}")
    validate_invoice(invoice_id)
    print(f"[{datetime.now()}] Validated invoice {invoice_id}")
    send_to_tax(invoice_id)
    print(f"[{datetime.now()}] Sent invoice {invoice_id} to tax system")

def validate_invoice(invoice_id: str):
    print(f"[{datetime.now()}] Validating invoice {invoice_id}")
    # Validation logic
    print(f"[{datetime.now()}] Invoice {invoice_id} valid")

def send_to_tax(invoice_id: str):
    print(f"[{datetime.now()}] Sending invoice {invoice_id} to tax")
    # Tax logic
    print(f"[{datetime.now()}] Invoice {invoice_id} sent")
```

**Problems:**

-   ❌ Manual `invoice_id` passing everywhere
-   ❌ Timestamp formatting repeated
-   ❌ No nesting visualization
-   ❌ No storage for historical analysis
-   ❌ Hard to test flow

---

**After Observer:**

```python
@observe(track="invoice_id")
def process_invoice(invoice_id: str):
    observe.info("Processing invoice")
    validate_invoice(invoice_id)
    send_to_tax(invoice_id)

@observe  # Inherits invoice_id from parent
def validate_invoice(invoice_id: str):
    observe.info("Validating invoice")
    # Validation logic

@observe
def send_to_tax(invoice_id: str):
    observe.info("Sending to tax system")
    # Tax logic
```

**Console Output:**

```
[11:30:00.100] ℹ️  process_invoice ▶️
  └─ invoice_id: INV-001
  [11:30:00.150] ℹ️  validate_invoice ▶️
    └─ invoice_id: INV-001
  [11:30:00.200] ✓ validate_invoice (50ms)
  [11:30:00.250] ℹ️  send_to_tax ▶️
    └─ invoice_id: INV-001
  [11:30:00.850] ✓ send_to_tax (600ms)
[11:30:00.900] ✓ process_invoice (800ms)
```

**Benefits:**

-   ✅ No manual context threading
-   ✅ Beautiful, nested output
-   ✅ Automatic timing
-   ✅ Stored in SQLite for querying
-   ✅ Testable via trace mode

---

## ✅ Goals \& Acceptance Criteria

### Primary Goals

1. **Context Propagation Without Manual Passing**
    - [ ] Tracked properties flow down call stack automatically
    - [ ] Child functions inherit parent's tracked properties
    - [ ] No need to pass tracked properties as parameters to `observe.info()`
2. **Additive Tracking**
    - [ ] Child can add NEW tracked properties to parent's context
    - [ ] Both parent and child properties visible in child's logs
    - [ ] Child's properties removed when child exits (scoped cleanup)
3. **Sticky Properties**
    - [ ] `sticky=True` flag keeps properties in context after function exits
    - [ ] Use case: `request_id`, `user_id`, `session_id` (cross-cutting concerns)
    - [ ] Sticky properties persist through entire call chain
4. **Override Behavior**
    - [ ] Child can override parent's tracked property value
    - [ ] Explicit `track` parameter takes precedence
    - [ ] Enables "switching context" mid-flow
5. **Trace Mode (Testing)**
    - [ ] `observe.trace_mode()` context manager captures events in-memory
    - [ ] Testing API: `trace.assert_operations()`, `trace.assert_tracked()`, etc.
    - [ ] Trace mode does NOT write to DB (memory-only)
6. **Beautiful Console Output**
    - [ ] ASCII tree rendering with indentation
    - [ ] Emojis for visual clarity (▶️, ✓, ❌, ℹ️)
    - [ ] Duration display (50ms, 1.2s, etc.)
    - [ ] Tracked properties displayed per operation
7. **SQLite Storage**
    - [ ] All events stored for historical analysis
    - [ ] Query API for filtering by operation, entity, date range
    - [ ] Efficient storage (WAL mode, append-only)
8. **Sync \& Async Support**
    - [ ] One decorator works for both sync and async functions
    - [ ] Context propagation works in async/await

---

### Acceptance Criteria (Must Pass Before Merge)

-   [ ] **AC1:** Context propagates without manual passing
    -   Test: Parent tracks `invoice_id`, child logs without explicit parameter
    -   Expected: Child's logs include `invoice_id`
-   [ ] **AC2:** Additive tracking works
    -   Test: Parent tracks `invoice_id`, child tracks `item_id`
    -   Expected: Child's logs show both, parent's logs show only `invoice_id` after child exits
-   [ ] **AC3:** Sticky properties persist
    -   Test: Parent tracks `request_id` with `sticky=True`, multiple children execute
    -   Expected: All children have `request_id` in logs, even after parent exits
-   [ ] **AC4:** Override works
    -   Test: Parent tracks `invoice_id="INV-001"`, child tracks `invoice_id="INV-002"`
    -   Expected: Child's logs show `INV-002`
-   [ ] **AC5:** Trace mode captures flow
    -   Test: Execute function in trace mode, assert operations list
    -   Expected: `trace.operations == ["parent", "child1", "child2"]`
-   [ ] **AC6:** Console output is beautiful
    -   Test: Visual inspection of TreeLogBackend output
    -   Expected: Indented, emojis, durations, tracked properties
-   [ ] **AC7:** SQLite storage works
    -   Test: Execute function, query events from DB
    -   Expected: Events retrievable with correct `parent_operation`, `data`, `duration_ms`
-   [ ] **AC8:** Performance targets met
    -   Test: Benchmark decorator overhead
    -   Expected: <5% overhead vs no decorator

---

## 🏗️ Architecture

### File Structure

```
scriptman/observe/
├── __init__.py              # Public API (observe.info, observe.trace_mode, etc.)
├── models.py                # ObserverEvent dataclass
├── context.py               # Context propagation (contextvars)
├── decorator.py             # @observe implementation
├── storage.py               # SQLite storage client
├── backends/
│   ├── __init__.py
│   ├── base.py              # ObserverBackend ABC
│   ├── tree.py              # TreeLogBackend (console rendering)
│   └── sqlite.py            # SQLiteBackend (database storage)
├── trace.py                 # TraceMode class (testing utilities)
└── query.py                 # Query API (find, filter, aggregate)
```

### Core Components

#### 1. ObserverEvent (models.py)

**Purpose:** Immutable event record

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

@dataclass(frozen=True)
class ObserverEvent:
    """
    Immutable event record for a single operation.

    Attributes:
        event_id: Unique identifier (UUID)
        timestamp: Event creation time (ISO 8601)
        operation: Function name
        entity: Entity type (e.g., "invoice", "customer")
        outcome: Event outcome (STARTED, SUCCESS, ERROR, INFO, DEBUG, WARNING)
        level: Log level (INFO, DEBUG, WARNING, ERROR)
        message: Human-readable message
        data: Tracked properties (dict)
        duration_ms: Execution time in milliseconds (None for STARTED events)
        parent_operation: Parent operation name (None for root)
        correlation_id: Trace ID for distributed tracing
        error_type: Exception class name (if outcome=ERROR)
        error_message: Exception message (if outcome=ERROR)
        stack_trace: Full traceback (if outcome=ERROR)
    """
    event_id: str
    timestamp: str
    operation: str
    entity: str | None
    outcome: Literal["STARTED", "SUCCESS", "ERROR", "INFO", "DEBUG", "WARNING"]
    level: Literal["INFO", "DEBUG", "WARNING", "ERROR"]
    message: str
    data: dict[str, Any]
    duration_ms: int | None
    parent_operation: str | None
    correlation_id: str | None
    error_type: str | None
    error_message: str | None
    stack_trace: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "operation": self.operation,
            "entity": self.entity,
            "outcome": self.outcome,
            "level": self.level,
            "message": self.message,
            "data": self.data,
            "duration_ms": self.duration_ms,
            "parent_operation": self.parent_operation,
            "correlation_id": self.correlation_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
        }
```

---

#### 2. Context Propagation (context.py)

**Purpose:** Manage tracked properties using `contextvars`

```python
from contextvars import ContextVar
from typing import Any

# Context variables
_tracked_properties: ContextVar[dict[str, Any]] = ContextVar("tracked_properties", default={})
_sticky_properties: ContextVar[dict[str, Any]] = ContextVar("sticky_properties", default={})
_parent_operation: ContextVar[str | None] = ContextVar("parent_operation", default=None)
_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

def get_tracked_properties() -> dict[str, Any]:
    """
    Get current tracked properties (including sticky).

    Returns:
        Merged dict of current + sticky properties
    """
    current = _tracked_properties.get()
    sticky = _sticky_properties.get()
    return {**sticky, **current}  # Current overrides sticky

def set_tracked_properties(properties: dict[str, Any], sticky: bool = False):
    """
    Set tracked properties for current context.

    Args:
        properties: Properties to track
        sticky: If True, properties persist after function exits
    """
    current = _tracked_properties.get()
    updated = {**current, **properties}  # Merge with parent
    _tracked_properties.set(updated)

    if sticky:
        # Add to sticky properties
        sticky_props = _sticky_properties.get()
        sticky_props.update(properties)
        _sticky_properties.set(sticky_props)

def remove_tracked_properties(properties: dict[str, Any]):
    """
    Remove tracked properties from current context.

    Only removes non-sticky properties.

    Args:
        properties: Properties to remove
    """
    sticky = _sticky_properties.get()
    current = _tracked_properties.get()

    # Only remove if not sticky
    for key in properties:
        if key not in sticky and key in current:
            del current[key]

    _tracked_properties.set(current)

def get_parent_operation() -> str | None:
    """Get current parent operation name."""
    return _parent_operation.get()

def set_parent_operation(operation: str):
    """Set parent operation name."""
    _parent_operation.set(operation)

def clear_parent_operation():
    """Clear parent operation."""
    _parent_operation.set(None)

def get_correlation_id() -> str | None:
    """Get current correlation ID (trace ID)."""
    return _correlation_id.get()

def set_correlation_id(correlation_id: str):
    """Set correlation ID for distributed tracing."""
    _correlation_id.set(correlation_id)
```

---

#### 3. @observe Decorator (decorator.py)

**Purpose:** Function-level observability decorator

```python
import functools
import inspect
import time
import uuid
from datetime import datetime
from typing import Any, Callable, TypeVar, ParamSpec

from scriptman.observe.models import ObserverEvent
from scriptman.observe.context import (
    get_tracked_properties,
    set_tracked_properties,
    remove_tracked_properties,
    get_parent_operation,
    set_parent_operation,
    clear_parent_operation,
    get_correlation_id,
    set_correlation_id,
)
from scriptman.observe.backends import get_active_backends

P = ParamSpec("P")
R = TypeVar("R")

def observe(
    func: Callable[P, R] | None = None,
    *,
    entity: str | None = None,
    track: str | list[str] | None = None,
    sticky: bool = False,
    capture_args: bool = False,
    capture_result: bool = False,
) -> Callable[P, R]:
    """
    Decorator for function-level observability.

    Args:
        func: Function to decorate (when used without parentheses)
        entity: Entity type (e.g., "invoice", "customer")
        track: Parameter name(s) to extract and track
        sticky: If True, tracked properties persist after function exits
        capture_args: If True, capture all function arguments
        capture_result: If True, capture return value

    Returns:
        Decorated function

    Example:
        >>> @observe(track="invoice_id")
        ... def process_invoice(invoice_id: str):
        ...     observe.info("Processing")
        ...     validate_invoice(invoice_id)
        ...
        >>> @observe  # Inherits invoice_id from parent
        ... def validate_invoice(invoice_id: str):
        ...     observe.info("Validating")
    """
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            # Extract tracked properties from function parameters
            new_tracked = {}
            if track:
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()

                track_list = [track] if isinstance(track, str) else track
                for param_name in track_list:
                    if param_name in bound_args.arguments:
                        new_tracked[param_name] = bound_args.arguments[param_name]

            # Merge with parent context (additive)
            parent_tracked = get_tracked_properties()
            current_tracked = {**parent_tracked, **new_tracked}  # Child overrides

            # Set context
            set_tracked_properties(new_tracked, sticky=sticky)

            # Set parent operation for nesting
            parent_op = get_parent_operation()
            set_parent_operation(fn.__name__)

            # Generate correlation ID if root operation
            if parent_op is None and get_correlation_id() is None:
                set_correlation_id(str(uuid.uuid4()))

            # Create STARTED event
            start_time = time.perf_counter()
            start_event = ObserverEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.utcnow().isoformat(),
                operation=fn.__name__,
                entity=entity,
                outcome="STARTED",
                level="INFO",
                message=f"Started {fn.__name__}",
                data=current_tracked,
                duration_ms=None,
                parent_operation=parent_op,
                correlation_id=get_correlation_id(),
                error_type=None,
                error_message=None,
                stack_trace=None,
            )

            # Emit to backends
            for backend in get_active_backends():
                backend.emit(start_event)

            # Execute function
            try:
                result = fn(*args, **kwargs)

                # Create SUCCESS event
                end_time = time.perf_counter()
                duration_ms = int((end_time - start_time) * 1000)

                success_event = ObserverEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.utcnow().isoformat(),
                    operation=fn.__name__,
                    entity=entity,
                    outcome="SUCCESS",
                    level="INFO",
                    message=f"Completed {fn.__name__}",
                    data=current_tracked,
                    duration_ms=duration_ms,
                    parent_operation=parent_op,
                    correlation_id=get_correlation_id(),
                    error_type=None,
                    error_message=None,
                    stack_trace=None,
                )

                for backend in get_active_backends():
                    backend.emit(success_event)

                return result

            except Exception as e:
                # Create ERROR event
                end_time = time.perf_counter()
                duration_ms = int((end_time - start_time) * 1000)

                import traceback

                error_event = ObserverEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.utcnow().isoformat(),
                    operation=fn.__name__,
                    entity=entity,
                    outcome="ERROR",
                    level="ERROR",
                    message=f"Error in {fn.__name__}: {str(e)}",
                    data=current_tracked,
                    duration_ms=duration_ms,
                    parent_operation=parent_op,
                    correlation_id=get_correlation_id(),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    stack_trace=traceback.format_exc(),
                )

                for backend in get_active_backends():
                    backend.emit(error_event)

                raise

            finally:
                # Cleanup: Remove non-sticky properties
                if not sticky:
                    remove_tracked_properties(new_tracked)

                # Restore parent operation
                set_parent_operation(parent_op)

        return wrapper

    # Handle both @observe and @observe(...)
    if func is None:
        return decorator
    else:
        return decorator(func)
```

---

#### 4. ObserverBackend ABC (backends/base.py)

**Purpose:** Base interface for storage/output backends

```python
from abc import ABC, abstractmethod
from scriptman.observe.models import ObserverEvent

class ObserverBackend(ABC):
    """
    Abstract base class for observer backends.

    Backends handle event storage/output (SQLite, console, file, etc.)
    """

    @abstractmethod
    def emit(self, event: ObserverEvent):
        """
        Emit an event to the backend.

        Args:
            event: Event to emit
        """
        pass

    @abstractmethod
    def close(self):
        """Close backend and release resources."""
        pass
```

---

#### 5. TreeLogBackend (backends/tree.py)

**Purpose:** Beautiful console output

```python
from scriptman.observe.models import ObserverEvent
from scriptman.observe.backends.base import ObserverBackend

class TreeLogBackend(ObserverBackend):
    """
    Console backend with ASCII tree rendering.

    Outputs beautiful, indented logs with emojis and durations.
    """

    def __init__(self):
        self._indent_level = 0
        self._operation_stack = []

    def emit(self, event: ObserverEvent):
        """Emit event to console."""
        indent = "  " * self._indent_level
        timestamp = event.timestamp.split("T")[:12]  # HH:MM:SS.mmm[^1]

        if event.outcome == "STARTED":
            # Print operation start
            print(f"{indent}[{timestamp}] ▶️  {event.operation}")

            # Print tracked properties
            for key, value in event.data.items():
                print(f"{indent}  └─ {key}: {value}")

            # Increase indent for children
            self._indent_level += 1
            self._operation_stack.append(event.operation)

        elif event.outcome in ("SUCCESS", "ERROR"):
            # Decrease indent
            self._indent_level = max(0, self._indent_level - 1)

            # Print operation end
            emoji = "✓" if event.outcome == "SUCCESS" else "❌"
            duration = f"({event.duration_ms}ms)" if event.duration_ms else ""
            print(f"{indent}[{timestamp}] {emoji} {event.operation} {duration}")

            if event.outcome == "ERROR":
                print(f"{indent}  └─ Error: {event.error_message}")

            # Remove from stack
            if self._operation_stack and self._operation_stack[-1] == event.operation:
                self._operation_stack.pop()

        elif event.outcome in ("INFO", "DEBUG", "WARNING"):
            # Print log message
            emoji = {"INFO": "ℹ️", "DEBUG": "🔍", "WARNING": "⚠️"}[event.level]
            print(f"{indent}[{timestamp}] {emoji} {event.message}")

    def close(self):
        """No-op for console backend."""
        pass
```

---

#### 6. SQLiteBackend (backends/sqlite.py)

**Purpose:** Persistent storage in SQLite

```python
import sqlite3
import json
from pathlib import Path
from scriptman.observe.models import ObserverEvent
from scriptman.observe.backends.base import ObserverBackend

class SQLiteBackend(ObserverBackend):
    """
    SQLite backend for persistent event storage.

    Schema:
        observer_events (
            event_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            operation TEXT NOT NULL,
            entity TEXT,
            outcome TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            data TEXT NOT NULL,  -- JSON
            duration_ms INTEGER,
            parent_operation TEXT,
            correlation_id TEXT,
            error_type TEXT,
            error_message TEXT,
            stack_trace TEXT
        )
    """

    def __init__(self, db_path: str | Path = "scriptman_observer.db"):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._init_schema()

    def _init_schema(self):
        """Create tables if not exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS observer_events (
                event_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                operation TEXT NOT NULL,
                entity TEXT,
                outcome TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                data TEXT NOT NULL,
                duration_ms INTEGER,
                parent_operation TEXT,
                correlation_id TEXT,
                error_type TEXT,
                error_message TEXT,
                stack_trace TEXT
            )
        """)

        # Indexes for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp
            ON observer_events(timestamp)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_operation
            ON observer_events(operation)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_correlation_id
            ON observer_events(correlation_id)
        """)

        self.conn.commit()

    def emit(self, event: ObserverEvent):
        """Store event in database."""
        self.conn.execute("""
            INSERT INTO observer_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.event_id,
            event.timestamp,
            event.operation,
            event.entity,
            event.outcome,
            event.level,
            event.message,
            json.dumps(event.data),
            event.duration_ms,
            event.parent_operation,
            event.correlation_id,
            event.error_type,
            event.error_message,
            event.stack_trace,
        ))
        self.conn.commit()

    def close(self):
        """Close database connection."""
        self.conn.close()
```

---

#### 7. TraceMode (trace.py)

**Purpose:** Testing utilities

```python
from typing import Any
from scriptman.observe.models import ObserverEvent
from scriptman.observe.backends.base import ObserverBackend

class TraceMode(ObserverBackend):
    """
    In-memory backend for testing and debugging.

    Captures events without writing to DB.
    """

    def __init__(self):
        self.events: list[ObserverEvent] = []
        self.start_time: float | None = None
        self.end_time: float | None = None

    def emit(self, event: ObserverEvent):
        """Capture event in memory."""
        self.events.append(event)

    def close(self):
        """No-op for in-memory backend."""
        pass

    # ========================================
    # TESTING API
    # ========================================

    @property
    def operations(self) -> list[str]:
        """List of operation names in execution order."""
        return [e.operation for e in self.events if e.outcome == "STARTED"]

    @property
    def tracked_properties(self) -> set[str]:
        """All tracked property keys across all events."""
        props = set()
        for event in self.events:
            props.update(event.data.keys())
        return props

    @property
    def total_duration_ms(self) -> int:
        """Total execution time in milliseconds."""
        root_events = [e for e in self.events if e.parent_operation is None]
        if not root_events:
            return 0

        terminal = next(
            (e for e in self.events if e.operation == root_events.operation and e.outcome in ("SUCCESS", "ERROR")),
            None
        )
        return terminal.duration_ms if terminal else 0

    def find_operation(self, operation: str) -> ObserverEvent | None:
        """Find first terminal event (SUCCESS/ERROR) for operation."""
        return next(
            (e for e in self.events if e.operation == operation and e.outcome in ("SUCCESS", "ERROR")),
            None
        )

    def find_errors(self) -> list[ObserverEvent]:
        """Get all error events."""
        return [e for e in self.events if e.outcome == "ERROR"]

    def has_errors(self) -> bool:
        """Check if any errors occurred."""
        return len(self.find_errors()) > 0

    def get_property(self, key: str) -> Any:
        """Get tracked property value (from first event that has it)."""
        for event in self.events:
            if key in event.data:
                return event.data[key]
        return None

    # ========================================
    # ASSERTION HELPERS
    # ========================================

    def assert_operations(self, expected: list[str]):
        """Assert operations match expected order."""
        actual = self.operations
        if actual != expected:
            raise AssertionError(
                f"Operation mismatch!\n"
                f"Expected: {expected}\n"
                f"Actual:   {actual}"
            )

    def assert_tracked(self, *properties: str):
        """Assert specific properties were tracked."""
        tracked = self.tracked_properties
        missing = set(properties) - tracked
        if missing:
            raise AssertionError(
                f"Missing tracked properties: {missing}\n"
                f"Tracked: {tracked}"
            )

    def assert_no_errors(self):
        """Assert no errors occurred."""
        errors = self.find_errors()
        if errors:
            error_summary = "\n".join(
                f"  - {e.operation}: {e.error_type} - {e.error_message}"
                for e in errors
            )
            raise AssertionError(
                f"Errors occurred during trace:\n{error_summary}"
            )

    def assert_duration_under(self, max_ms: int):
        """Assert total duration is under threshold."""
        actual = self.total_duration_ms
        if actual > max_ms:
            raise AssertionError(
                f"Duration exceeded threshold!\n"
                f"Expected: <{max_ms}ms\n"
                f"Actual:   {actual}ms"
            )

    def to_tree(self) -> str:
        """Render ASCII tree (for console output)."""
        # TODO: Implement tree rendering
        pass

    def to_json(self) -> dict:
        """Export as JSON (for snapshot testing)."""
        return {
            "operations": self.operations,
            "tracked": list(self.tracked_properties),
            "duration_ms": self.total_duration_ms,
            "errors": [
                {
                    "operation": e.operation,
                    "error_type": e.error_type,
                    "error_message": e.error_message,
                }
                for e in self.find_errors()
            ],
            "events": [e.to_dict() for e in self.events]
        }
```

---

#### 8. Public API (**init**.py)

**Purpose:** Expose simple API for users

```python
from contextlib import contextmanager
from typing import Any

from scriptman.observe.decorator import observe as _observe_decorator
from scriptman.observe.models import ObserverEvent
from scriptman.observe.context import (
    get_tracked_properties,
    set_tracked_properties,
    get_correlation_id,
    set_correlation_id,
)
from scriptman.observe.backends.base import ObserverBackend
from scriptman.observe.backends.tree import TreeLogBackend
from scriptman.observe.backends.sqlite import SQLiteBackend
from scriptman.observe.trace import TraceMode

# ========================================
# GLOBAL STATE
# ========================================

_backends: list[ObserverBackend] = [
    TreeLogBackend(),  # Console output by default
    SQLiteBackend(),   # SQLite storage by default
]

def get_active_backends() -> list[ObserverBackend]:
    """Get list of active backends."""
    return _backends

def add_backend(backend: ObserverBackend):
    """Add a custom backend."""
    _backends.append(backend)

def clear_backends():
    """Remove all backends."""
    _backends.clear()

# ========================================
# DECORATOR
# ========================================

# Re-export decorator
observe = _observe_decorator

# ========================================
# MANUAL LOGGING
# ========================================

def _emit_manual_event(level: str, message: str, **data):
    """Emit a manual log event."""
    import uuid
    from datetime import datetime
    from scriptman.observe.context import get_parent_operation

    event = ObserverEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.utcnow().isoformat(),
        operation=get_parent_operation() or "manual",
        entity=None,
        outcome=level,
        level=level,
        message=message,
        data={**get_tracked_properties(), **data},
        duration_ms=None,
        parent_operation=get_parent_operation(),
        correlation_id=get_correlation_id(),
        error_type=None,
        error_message=None,
        stack_trace=None,
    )

    for backend in get_active_backends():
        backend.emit(event)

def info(message: str, **data):
    """Log an info message."""
    _emit_manual_event("INFO", message, **data)

def debug(message: str, **data):
    """Log a debug message."""
    _emit_manual_event("DEBUG", message, **data)

def warning(message: str, **data):
    """Log a warning message."""
    _emit_manual_event("WARNING", message, **data)

def error(message: str, **data):
    """Log an error message."""
    _emit_manual_event("ERROR", message, **data)

# ========================================
# TRACE MODE
# ========================================

@contextmanager
def trace_mode():
    """
    Context manager for trace mode (testing).

    Captures events in-memory without writing to DB.

    Example:
        >>> with observe.trace_mode() as trace:
        ...     process_invoice("INV-001")
        >>> trace.assert_operations(["process_invoice", "validate_invoice"])
    """
    # Create trace backend
    trace = TraceMode()

    # Replace backends temporarily
    original_backends = _backends.copy()
    _backends.clear()
    _backends.append(trace)

    try:
        yield trace
    finally:
        # Restore original backends
        _backends.clear()
        _backends.extend(original_backends)

# ========================================
# EXPORTS
# ========================================

__all__ = [
    "observe",
    "info",
    "debug",
    "warning",
    "error",
    "trace_mode",
    "ObserverEvent",
    "ObserverBackend",
    "TreeLogBackend",
    "SQLiteBackend",
    "TraceMode",
    "add_backend",
    "clear_backends",
]
```

---

## 📝 Implementation Plan

### Phase 1: Foundation (Days 1-3)

**Goal:** Core data models and context propagation

-   [ ] **Task 1.1:** Define `ObserverEvent` dataclass (models.py)
    -   Frozen dataclass with all fields
    -   `to_dict()` method for serialization
    -   Type hints for all attributes
-   [ ] **Task 1.2:** Implement context propagation (context.py)
    -   `ContextVar` for tracked properties
    -   `ContextVar` for sticky properties
    -   `get_tracked_properties()` - merge current + sticky
    -   `set_tracked_properties()` - with sticky flag
    -   `remove_tracked_properties()` - only non-sticky
    -   `get/set_parent_operation()` - for nesting
    -   `get/set_correlation_id()` - for tracing
-   [ ] **Task 1.3:** Write tests for context propagation
    -   Test basic get/set
    -   Test sticky properties persist
    -   Test non-sticky removed on cleanup
    -   Test parent operation nesting
    -   Test correlation ID propagation

**Acceptance:** Context tests pass, tracked properties flow correctly

---

### Phase 2: Decorator (Days 4-6)

**Goal:** Implement `@observe` decorator

-   [ ] **Task 2.1:** Basic decorator structure (decorator.py)
    -   Function wrapping with `@functools.wraps`
    -   Handle both `@observe` and `@observe(...)`
    -   Generate STARTED/SUCCESS/ERROR events
    -   Measure execution time
-   [ ] **Task 2.2:** Track parameter extraction
    -   Use `inspect.signature()` to get parameters
    -   Extract specified parameters with `sig.bind()`
    -   Store in context via `set_tracked_properties()`
-   [ ] **Task 2.3:** Sticky flag support
    -   Pass `sticky` flag to `set_tracked_properties()`
    -   Verify sticky properties persist after function exit
-   [ ] **Task 2.4:** Write decorator tests
    -   Test basic function wrapping
    -   Test parameter extraction (single + multiple)
    -   Test context propagation (parent → child)
    -   Test additive tracking (child adds properties)
    -   Test override (child overrides parent's value)
    -   Test sticky properties persist
    -   Test error handling (exception captured)

**Acceptance:** Decorator tests pass, all tracking scenarios work

---

### Phase 3: Backends (Days 7-9)

**Goal:** Storage and output backends

-   [ ] **Task 3.1:** ObserverBackend ABC (backends/base.py)
    -   Abstract `emit()` method
    -   Abstract `close()` method
-   [ ] **Task 3.2:** TreeLogBackend (backends/tree.py)
    -   Render events to console
    -   ASCII tree indentation
    -   Emojis for visual clarity
    -   Duration formatting (ms, s)
    -   Tracked properties display
-   [ ] **Task 3.3:** SQLiteBackend (backends/sqlite.py)
    -   Create schema (`observer_events` table)
    -   `emit()` stores event in DB
    -   Indexes for common queries
    -   WAL mode for performance
-   [ ] **Task 3.4:** Write backend tests
    -   Test TreeLogBackend output (visual inspection)
    -   Test SQLiteBackend storage (query events from DB)
    -   Test multiple backends active simultaneously

**Acceptance:** Console output is beautiful, SQLite storage works

---

### Phase 4: Trace Mode (Days 10-12)

**Goal:** Testing utilities

-   [ ] **Task 4.1:** TraceMode class (trace.py)
    -   In-memory event storage
    -   `.operations` property
    -   `.tracked_properties` property
    -   `.total_duration_ms` property
    -   `find_operation()` method
    -   `find_errors()` / `has_errors()` methods
    -   `get_property()` method
-   [ ] **Task 4.2:** Assertion helpers
    -   `assert_operations()` - check call order
    -   `assert_tracked()` - verify properties tracked
    -   `assert_no_errors()` - ensure no exceptions
    -   `assert_duration_under()` - performance check
-   [ ] **Task 4.3:** `observe.trace_mode()` context manager
    -   Replace backends temporarily
    -   Restore original backends on exit
    -   Return TraceMode instance
-   [ ] **Task 4.4:** Write trace mode tests
    -   Test event capture in trace mode
    -   Test all assertion helpers
    -   Test trace mode doesn't write to DB
    -   Test `to_json()` export

**Acceptance:** Trace mode tests pass, testing API is intuitive

---

### Phase 5: Manual Logging (Days 13-14)

**Goal:** `observe.info()`, `observe.debug()`, etc.

-   [ ] **Task 5.1:** Implement manual logging functions (**init**.py)
    -   `info()`, `debug()`, `warning()`, `error()`
    -   Create events with current context
    -   Emit to active backends
-   [ ] **Task 5.2:** Write manual logging tests
    -   Test each log level
    -   Verify tracked properties included
    -   Test logging inside decorated function

**Acceptance:** Manual logging works, context included automatically

---

### Phase 6: Integration \& Documentation (Days 15-17)

**Goal:** Finalize, test end-to-end, write docs

-   [ ] **Task 6.1:** Integration tests
    -   Full workflow: parent → child → grandchild
    -   Test with real-world scenario (invoice processing)
    -   Test sticky properties across multiple operations
    -   Test error propagation
-   [ ] **Task 6.2:** Performance benchmarks
    -   Measure decorator overhead
    -   Measure context propagation time
    -   Measure SQLite write time
    -   Target: <5% overhead
-   [ ] **Task 6.3:** Documentation
    -   README with quick start
    -   Usage examples (all tracking scenarios)
    -   API reference (all functions/classes)
    -   Testing guide (trace mode usage)
-   [ ] **Task 6.4:** Code review \& refinement
    -   Run through post-implementation review checklist
    -   Optimize bottlenecks
    -   Fix edge cases

**Acceptance:** All tests pass, docs complete, ready for merge

---

## 💻 Code Examples

### Example 1: Basic Context Propagation

```python
from scriptman import observe

@observe(track="invoice_id")
def process_invoice(invoice_id: str):
    observe.info("Starting invoice processing")
    validate_invoice(invoice_id)
    send_to_tax(invoice_id)
    observe.info("Invoice processing complete")

@observe  # Inherits invoice_id from parent
def validate_invoice(invoice_id: str):
    observe.info("Validating invoice structure")
    # Validation logic
    observe.info("Invoice is valid")

@observe
def send_to_tax(invoice_id: str):
    observe.info("Sending invoice to tax system")
    # Tax API call
    observe.info("Invoice sent to tax system")

# Usage
process_invoice("INV-12345")
```

**Expected Console Output:**

```
[11:30:00.100] ▶️  process_invoice
  └─ invoice_id: INV-12345
  [11:30:00.110] ℹ️ Starting invoice processing
  [11:30:00.120] ▶️  validate_invoice
    └─ invoice_id: INV-12345
    [11:30:00.130] ℹ️ Validating invoice structure
    [11:30:00.150] ℹ️ Invoice is valid
  [11:30:00.160] ✓ validate_invoice (40ms)
  [11:30:00.170] ▶️  send_to_tax
    └─ invoice_id: INV-12345
    [11:30:00.180] ℹ️ Sending invoice to tax system
    [11:30:00.780] ℹ️ Invoice sent to tax system
  [11:30:00.790] ✓ send_to_tax (620ms)
  [11:30:00.800] ℹ️ Invoice processing complete
[11:30:00.810] ✓ process_invoice (710ms)
```

---

### Example 2: Additive Tracking

```python
@observe(track="credit_note_id")
def process_credit_note(credit_note_id: str):
    observe.info("Processing credit note")
    fetch_related_invoice(credit_note_id)

@observe(track="invoice_id")  # Adds invoice_id to context
def fetch_related_invoice(credit_note_id: str):
    invoice_id = get_invoice_from_credit_note(credit_note_id)
    observe.info("Fetched related invoice", invoice_id=invoice_id)
    validate_invoice(invoice_id)

@observe
def validate_invoice(invoice_id: str):
    observe.info("Validating invoice")
    # At this point, BOTH credit_note_id and invoice_id are tracked

# Usage
process_credit_note("CN-001")
```

**Expected Console Output:**

```
[11:30:00.100] ▶️  process_credit_note
  └─ credit_note_id: CN-001
  [11:30:00.110] ℹ️ Processing credit note
  [11:30:00.120] ▶️  fetch_related_invoice
    └─ credit_note_id: CN-001
    └─ invoice_id: INV-456
    [11:30:00.130] ℹ️ Fetched related invoice
    [11:30:00.140] ▶️  validate_invoice
      └─ credit_note_id: CN-001
      └─ invoice_id: INV-456
      [11:30:00.150] ℹ️ Validating invoice
    [11:30:00.200] ✓ validate_invoice (60ms)
  [11:30:00.210] ✓ fetch_related_invoice (90ms)
[11:30:00.220] ✓ process_credit_note (120ms)
  └─ credit_note_id: CN-001  ← invoice_id is GONE
```

---

### Example 3: Sticky Properties

```python
@observe(track="request_id", sticky=True)
def handle_request(request_id: str):
    observe.info("Handling request")
    process_invoice("INV-001")
    process_payment("PAY-001")

@observe(track="invoice_id")
def process_invoice(invoice_id: str):
    observe.info("Processing invoice")
    # request_id is STILL in context (sticky)

@observe(track="payment_id")
def process_payment(payment_id: str):
    observe.info("Processing payment")
    # request_id is STILL in context (sticky)
    # invoice_id is GONE (not sticky)

# Usage
handle_request("REQ-789")
```

**Expected Console Output:**

```
[11:30:00.100] ▶️  handle_request
  └─ request_id: REQ-789
  [11:30:00.110] ℹ️ Handling request
  [11:30:00.120] ▶️  process_invoice
    └─ request_id: REQ-789  ← Sticky!
    └─ invoice_id: INV-001
    [11:30:00.130] ℹ️ Processing invoice
  [11:30:00.200] ✓ process_invoice (80ms)
  [11:30:00.210] ▶️  process_payment
    └─ request_id: REQ-789  ← Still sticky!
    └─ payment_id: PAY-001
    [11:30:00.220] ℹ️ Processing payment  ← invoice_id is GONE
  [11:30:00.300] ✓ process_payment (90ms)
[11:30:00.310] ✓ handle_request (210ms)
```

---

### Example 4: Testing with Trace Mode

```python
import pytest
from scriptman import observe

def test_invoice_processing_flow():
    """Verify invoice processing calls correct operations."""

    with observe.trace_mode() as trace:
        process_invoice("INV-001")

    # Assert operation order
    trace.assert_operations([
        "process_invoice",
        "validate_invoice",
        "send_to_tax",
    ])

    # Assert properties tracked
    trace.assert_tracked("invoice_id")

    # Assert no errors
    trace.assert_no_errors()

    # Assert performance
    trace.assert_duration_under(1000)  # Under 1 second

def test_context_propagation():
    """Verify invoice_id propagates to children."""

    with observe.trace_mode() as trace:
        process_invoice("INV-001")

    # Every event should have invoice_id
    for event in trace.events:
        assert "invoice_id" in event.data
        assert event.data["invoice_id"] == "INV-001"

def test_error_handling():
    """Verify errors are captured."""

    with observe.trace_mode() as trace:
        with pytest.raises(TaxSystemError):
            process_invoice("INV-BAD")

    # Assert error was captured
    assert trace.has_errors()

    # Find the error
    error_event = trace.find_operation("send_to_tax")
    assert error_event.outcome == "ERROR"
    assert error_event.error_type == "TaxSystemError"
```

---

## 🧪 Testing Requirements

### Unit Tests

**File:** `tests/observe/test_context.py`

-   [ ] Test `get_tracked_properties()` returns merged dict
-   [ ] Test `set_tracked_properties()` merges with parent
-   [ ] Test `remove_tracked_properties()` only removes non-sticky
-   [ ] Test sticky properties persist after removal
-   [ ] Test parent operation get/set
-   [ ] Test correlation ID propagation

**File:** `tests/observe/test_decorator.py`

-   [ ] Test basic function wrapping (function executes correctly)
-   [ ] Test STARTED/SUCCESS events created
-   [ ] Test ERROR event created on exception
-   [ ] Test duration measurement
-   [ ] Test single parameter extraction
-   [ ] Test multiple parameters extraction
-   [ ] Test context propagation (parent → child)
-   [ ] Test additive tracking (child adds properties)
-   [ ] Test override (child overrides parent)
-   [ ] Test sticky flag (properties persist)
-   [ ] Test decorator works without parentheses (`@observe`)
-   [ ] Test decorator works with parentheses (`@observe(...)`)

**File:** `tests/observe/test_backends.py`

-   [ ] Test TreeLogBackend renders correctly (visual inspection)
-   [ ] Test SQLiteBackend stores events
-   [ ] Test SQLiteBackend retrieves events
-   [ ] Test multiple backends active simultaneously

**File:** `tests/observe/test_trace.py`

-   [ ] Test TraceMode captures events
-   [ ] Test `.operations` property
-   [ ] Test `.tracked_properties` property
-   [ ] Test `.total_duration_ms` calculation
-   [ ] Test `find_operation()` method
-   [ ] Test `find_errors()` method
-   [ ] Test `assert_operations()` success
-   [ ] Test `assert_operations()` failure
-   [ ] Test `assert_tracked()` success
-   [ ] Test `assert_tracked()` failure
-   [ ] Test `assert_no_errors()` success
-   [ ] Test `assert_no_errors()` failure
-   [ ] Test `assert_duration_under()` success
-   [ ] Test `assert_duration_under()` failure
-   [ ] Test `to_json()` export

**File:** `tests/observe/test_manual_logging.py`

-   [ ] Test `observe.info()` emits event
-   [ ] Test `observe.debug()` emits event
-   [ ] Test `observe.warning()` emits event
-   [ ] Test `observe.error()` emits event
-   [ ] Test tracked properties included in manual logs
-   [ ] Test manual logging inside decorated function

---

### Integration Tests

**File:** `tests/observe/test_integration.py`

-   [ ] Test full workflow: parent → child → grandchild
-   [ ] Test sticky properties across 3 levels
-   [ ] Test additive tracking with 3 levels
-   [ ] Test error in child propagates correctly
-   [ ] Test invoice processing scenario (realistic workflow)
-   [ ] Test trace mode with complex flow

---

### Performance Tests

**File:** `tests/observe/test_performance.py`

-   [ ] Benchmark decorator overhead (< 5%)
-   [ ] Benchmark context propagation (< 1μs)
-   [ ] Benchmark SQLite write (< 10ms per event)
-   [ ] Benchmark trace mode overhead (< 5%)

---

## 📊 Decision Log

### 2026-02-05: Sticky Flag Design

**Decision:** Use separate `ContextVar` for sticky properties

**Rationale:**

-   Cleaner separation between scoped and persistent properties
-   Easier to implement scoped cleanup (just remove non-sticky)
-   Explicit tracking of what persists vs what doesn't

**Alternative Considered:**

-   Single `ContextVar` with metadata (e.g., `{"invoice_id": {"value": "INV-001", "sticky": False}}`)
-   **Why rejected:** Too complex, harder to merge/cleanup

---

### 2026-02-05: No Magic Parameter Injection

**Decision:** Users must pass parameters explicitly to functions

**Rationale:**

-   Maintains normal Python semantics
-   Type checkers work correctly
-   Code is readable by non-framework users
-   Avoids confusion about where parameters come from

**Alternative Considered:**

-   Magic injection: `validate_invoice()` gets `invoice_id` automatically
-   **Why rejected:** Too magical, breaks type hints, confusing

---

### 2026-02-05: Trace Mode Doesn't Write to DB

**Decision:** Trace mode is memory-only, doesn't write to SQLite

**Rationale:**

-   Keeps test DB clean (no pollution)
-   Faster (no I/O)
-   Enables "dry run" debugging

**Alternative Considered:**

-   Dual mode: trace AND write to DB with flag
-   **Why rejected:** YAGNI (we can add later if needed)

---

### 2026-02-05: One Decorator for Sync \& Async

**Decision:** Single `@observe` decorator works for both sync and async

**Rationale:**

-   Consistent API (no `@observe_sync` vs `@observe_async`)
-   Easier to learn
-   Less code duplication

**Implementation:** Detect `asyncio.iscoroutinefunction()` and wrap accordingly

---

## 📈 Progress Tracker

### Phase 1: Foundation

-   [ ] Task 1.1: ObserverEvent model
-   [ ] Task 1.2: Context propagation
-   [ ] Task 1.3: Context tests

### Phase 2: Decorator

-   [ ] Task 2.1: Basic decorator
-   [ ] Task 2.2: Parameter extraction
-   [ ] Task 2.3: Sticky flag
-   [ ] Task 2.4: Decorator tests

### Phase 3: Backends

-   [ ] Task 3.1: ObserverBackend ABC
-   [ ] Task 3.2: TreeLogBackend
-   [ ] Task 3.3: SQLiteBackend
-   [ ] Task 3.4: Backend tests

### Phase 4: Trace Mode

-   [ ] Task 4.1: TraceMode class
-   [ ] Task 4.2: Assertion helpers
-   [ ] Task 4.3: `trace_mode()` context manager
-   [ ] Task 4.4: Trace mode tests

### Phase 5: Manual Logging

-   [ ] Task 5.1: Manual logging functions
-   [ ] Task 5.2: Manual logging tests

### Phase 6: Integration

-   [ ] Task 6.1: Integration tests
-   [ ] Task 6.2: Performance benchmarks
-   [ ] Task 6.3: Documentation
-   [ ] Task 6.4: Code review

---

## 🔌 Integration Notes

### How Observer Fits in Scriptman

**Current Scriptman Structure:**

```
scriptman/
├── core/         # Utilities
├── powers/       # Features (selenium, etl, database, cache)
└── observe/      # ← NEW!
```

**Observer integrates with:**

-   **Database:** Uses same SQLite infrastructure
-   **Cache:** Can observe cache hits/misses (future)
-   **ETL:** Can observe data transformations (future)
-   **Selenium:** Can observe web interactions (future)

**No breaking changes:** Observer is additive, existing code works unchanged.

---

### Public API Surface

**What users import:**

```python
from scriptman import observe

# Decorator
@observe(track="invoice_id")
def my_function(invoice_id: str):
    pass

# Manual logging
observe.info("Message")
observe.debug("Debug info")
observe.warning("Warning")
observe.error("Error occurred")

# Testing
with observe.trace_mode() as trace:
    my_function("INV-001")
    trace.assert_operations([...])
```

**What's internal (not imported):**

-   `ObserverEvent` (model)
-   `ObserverBackend` (ABC)
-   `TreeLogBackend`, `SQLiteBackend` (implementations)
-   Context management functions

---

## 🎯 Performance Targets

### Decorator Overhead

-   **Target:** <5% overhead vs non-decorated function
-   **Measurement:** Benchmark 10,000 function calls with/without decorator
-   **Acceptable:** Up to 10% for complex tracking (multiple properties, sticky)

### Context Propagation

-   **Target:** <1μs per get/set operation
-   **Measurement:** Benchmark `get_tracked_properties()` 1,000,000 times
-   **Rationale:** Context is accessed frequently, must be fast

### SQLite Write

-   **Target:** <10ms per event
-   **Measurement:** Benchmark `backend.emit()` 1,000 times
-   **Optimization:** Batch writes, WAL mode, prepared statements

### Trace Mode Overhead

-   **Target:** <5% overhead vs normal execution
-   **Measurement:** Compare execution time with/without trace mode
-   **Rationale:** Trace mode used in testing, must not slow tests significantly

---

## 🔍 Post-Implementation Review

### Review Checklist (Complete AFTER Implementation)

**After implementation, Cursor should:**

1. **Code Quality Review**
    - [ ] All functions have docstrings
    - [ ] All public APIs have type hints
    - [ ] No `# type: ignore` comments (except justified)
    - [ ] No `# TODO` comments (move to GitHub issues)
    - [ ] Consistent naming (follow Vision Board conventions)
2. **Performance Review**
    - [ ] Run benchmarks, verify targets met
    - [ ] Profile hot paths (context propagation, event emission)
    - [ ] Identify optimization opportunities
    - [ ] Document performance characteristics
3. **Test Coverage Review**
    - [ ] Run `pytest --cov=scriptman.observe`
    - [ ] Verify >95% coverage on core modules
    - [ ] Verify >85% coverage on utilities
    - [ ] Identify uncovered edge cases
4. **Integration Review**
    - [ ] Test with real-world scenario (invoice processing)
    - [ ] Verify console output is beautiful
    - [ ] Verify SQLite storage works
    - [ ] Test trace mode in actual tests
5. **Documentation Review**
    - [ ] README has quick start
    - [ ] All usage patterns documented
    - [ ] API reference complete
    - [ ] Testing guide written
6. **Edge Cases Review**
    - [ ] Test recursive functions (function calls itself)
    - [ ] Test concurrent execution (threading)
    - [ ] Test async/await (if implemented)
    - [ ] Test deeply nested calls (>10 levels)
    - [ ] Test large tracked properties (dicts, lists)
7. **Error Handling Review**
    - [ ] Test decorator with invalid parameters
    - [ ] Test context propagation with exceptions
    - [ ] Test SQLite errors (disk full, permissions)
    - [ ] Test graceful degradation (SQLite fails → fallback to console)
8. **Optimization Opportunities**
    - [ ] Can context propagation be faster?
    - [ ] Can SQLite writes be batched?
    - [ ] Can we reduce memory allocations?
    - [ ] Can we lazy-load expensive operations?
9. **Refactoring Opportunities**
    - [ ] Are there code duplications?
    - [ ] Can complex functions be simplified?
    - [ ] Are there better abstractions?
    - [ ] Should any code be extracted to utilities?
10. **Future Considerations**
    - [ ] What would async support require?
    - [ ] What would remote storage require?
    - [ ] What would custom backends need?
    - [ ] What would CLI integration need?

---

### Review Output Format

**Cursor should provide:**

1. **Summary Report**
    - What was implemented
    - What works well
    - What needs improvement
    - Blockers/risks
2. **Performance Report**
    - Benchmark results (vs targets)
    - Hot paths identified
    - Optimization suggestions
3. **Coverage Report**
    - Overall coverage percentage
    - Uncovered lines/branches
    - Missing test cases
4. **Edge Cases Report**
    - Edge cases discovered during implementation
    - Edge cases tested
    - Edge cases deferred (with justification)
5. **Refactoring Suggestions**
    - Code that should be refactored
    - Better abstractions available
    - Technical debt incurred

---

## 🚀 Next Steps (After Observer Complete)

### v3.1 Features (Planned)

1. **CLI Integration**
    - `scriptman observe trace` - visualize flow
    - `scriptman observe query` - search events
    - `scriptman observe analyze` - static analysis
2. **Web UI**
    - Interactive trace viewer
    - Search and filter events
    - Flame graph rendering
3. **Advanced Features**
    - Sampling (trace 1% of requests)
    - Remote storage (PostgreSQL, Elasticsearch)
    - OpenTelemetry compatibility

---

## 📝 Notes \& Considerations

### Why SQLite?

-   **Zero-config:** No server to run
-   **Portable:** Single file, easy to share
-   **Fast:** WAL mode, append-only writes
-   **Queryable:** SQL for historical analysis

### Why contextvars?

-   **Thread-safe:** Unlike thread locals
-   **Async-safe:** Works with async/await
-   **Clean:** No global state pollution

### Why ABC Pattern?

-   **Extensibility:** Users can add custom backends
-   **Testability:** Easy to mock backends
-   **Flexibility:** Swap backends without changing code

---

## 🎓 Learning Resources (For Cursor)

### Context Management

-   [PEP 567 - Context Variables](https://peps.python.org/pep-0567/)
-   [contextvars Documentation](https://docs.python.org/3/library/contextvars.html)

### Decorators

-   [PEP 318 - Decorators](https://peps.python.org/pep-0318/)
-   [functools.wraps](https://docs.python.org/3/library/functools.html#functools.wraps)

### SQLite Performance

-   [SQLite WAL Mode](https://www.sqlite.org/wal.html)
-   [SQLite Performance Tuning](https://www.sqlite.org/pragma.html#pragma_optimize)

### Testing

-   [pytest Documentation](https://docs.pytest.org/)
-   [pytest Fixtures](https://docs.pytest.org/en/latest/explanation/fixtures.html)

---

**Remember:** This is a living document. Update as implementation progresses!

---

_"First, solve the problem. Then, write the code." - John Johnson_
