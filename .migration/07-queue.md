# 📬 Scriptman Queue Module — Implementation Plan

## Overview

This plan guides the implementation of Scriptman's **Kafka-inspired SQLite Queue** — a local-first message queue with consumer groups, position tracking, dead letter queues, and full observability integration.

**Primary Goals:**
- Simple, beginner-friendly API with clear terminology
- Production-ready reliability (idempotency, DLQ, retries, visibility timeout)
- Consumer groups for parallel processing pipelines
- Full Observer integration for tracing
- SQLite-backed with topic-level sharding
- Async support for FastAPI/async frameworks

---

## ⚠️ Pre-Implementation Checklist

Before implementing, these changes must be made to existing modules:

### 1. Add `"queue"` to DataCategory

**File:** `scriptman/config/schema/data.py`

```python
# Current:
DataCategory = Literal["logs", "db", "cache", "artifacts", "observe"]

# Update to:
DataCategory = Literal["logs", "db", "cache", "artifacts", "observe", "queue"]
```

Also add to `DataConfig`:

```python
class DataConfig(BaseModel):
    # ... existing fields ...

    queue: Path = Field(
        default=Path("queue"),
        description="Subdirectory name for queue databases",
    )
```

And update `get_path()`:

```python
subdirs: dict[DataCategory, Path] = {
    "db": self.db,
    "logs": self.logs,
    "cache": self.cache,
    "observe": self.observe,
    "artifacts": self.artifacts,
    "queue": self.queue,  # Add this
}
```

### 2. Update Directory Structure Documentation

Update `.data/` structure in docs:

```
.data/                          # Base data directory
├── db/                         # Database files
├── cache/                      # Cache storage
├── observe/                    # Observer storage
├── queue/                      # Queue storage (NEW)
│   ├── orders.db               # Topic: orders
│   ├── payments.db             # Topic: payments
│   └── _groups.db              # Consumer positions & DLQ
├── logs/                       # Log files
└── artifacts/                  # Build artifacts
```

---

## 📋 Design Principles

### Kafka-Inspired, Scriptman-Simplified

| Kafka Concept      | Scriptman Equivalent        | Simplification                   |
| ------------------ | --------------------------- | -------------------------------- |
| Topics             | Queue names                 | ✅ Same                           |
| Partitions         | Topic sharding              | Simpler: 1 SQLite file per topic |
| Consumer Groups    | Consumer groups             | ✅ Same concept                   |
| Offsets            | **Position**                | Beginner-friendly name           |
| ack/nack           | **accept/reject/retry**     | Clearer intent                   |
| At-least-once      | At-least-once + idempotency | Idempotency ON by default        |
| Visibility timeout | **Visibility timeout**      | Prevents stuck messages          |

### Core Terminology

| Term                   | Meaning                                                   |
| ---------------------- | --------------------------------------------------------- |
| **Topic**              | Named queue (e.g., "orders", "emails")                    |
| **Message**            | Data payload in a queue                                   |
| **Position**           | Where you are in the queue (like a bookmark)              |
| **Consumer Group**     | Named set of workers sharing the workload                 |
| **DLQ**                | Dead Letter Queue — parking lot for failed messages       |
| **Idempotency Key**    | Unique ID preventing duplicate messages                   |
| **Visibility Timeout** | Time before stuck "processing" message returns to pending |

### Backend Strategy

Scriptman Queue uses swappable backends, following the same pattern as Cache:

| Backend              | Use Case                    | Status    |
| -------------------- | --------------------------- | --------- |
| `SQLiteQueueBackend` | Local/dev/simple production | 📋 Planned |
| `RedisQueueBackend`  | Distributed, multi-worker   | 💡 Future  |

**Why not Celery/RabbitMQ backends?**
- Celery and Dramatiq are **complete task frameworks**, not just queue backends
- They have unique features (routing, priorities, canvas) that don't map cleanly
- Abstracting over them creates leaky abstractions

**Recommendation:**
- Use SQLite for local development and simple production
- Use Redis when you need multiple worker processes
- For enterprise needs, use Celery/Dramatiq directly with Observer middleware integration

---

## 📁 File Structure

```
scriptman/queue/
├── __init__.py           # Queue class, decorators, exports
├── message.py            # Message model, MessageStatus enum
├── group.py              # ConsumerGroup, position tracking
├── worker.py             # Worker registration, execution loop
├── dlq.py                # Dead Letter Queue handling
├── backends/
│   ├── __init__.py       # QueueBackend ABC, registry
│   ├── sqlite.py         # SQLiteQueueBackend (default)
│   └── redis.py          # RedisQueueBackend (future)
└── schema.py             # Config schema (QueueConfig)
```

---

## 🗃️ SQLite Schema

### File: `scriptman/queue/backends/sqlite.py`

Each topic gets its own SQLite file for isolation:

```
.data/queue/
├── orders.db
├── payments.db
├── tims_submissions.db
└── _groups.db            # Consumer group positions (shared)
```

### Messages Table (per-topic DB)

```sql
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position INTEGER NOT NULL,              -- Sequential within topic

    -- Message content
    data BLOB NOT NULL,                     -- Serialized payload
    headers TEXT,                           -- JSON metadata

    -- Idempotency
    idempotency_key TEXT UNIQUE,            -- Prevents duplicates (NULL = disabled)

    -- Status tracking
    status TEXT DEFAULT 'pending',          -- pending, processing, completed, failed
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,

    -- Timestamps (Unix epoch)
    created_at REAL NOT NULL,
    scheduled_at REAL,                      -- For delayed messages
    started_at REAL,                        -- When processing began
    completed_at REAL,
    next_retry_at REAL,                     -- For retry backoff
    expires_at REAL,                        -- Message TTL (optional)

    -- Visibility timeout
    visibility_timeout REAL,                -- Seconds before returning to pending
    lease_expires_at REAL,                  -- When current processing lease expires

    -- Error tracking
    error TEXT,                             -- Last error message

    -- Observability
    correlation_id TEXT,                    -- Links to Observer spans

    -- Deferred function calls
    function_path TEXT,                     -- For @defer: "myapp.tasks.send_email"
    function_args TEXT,                     -- JSON: positional args
    function_kwargs TEXT                    -- JSON: keyword args
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_messages_status
    ON messages(status, position);
CREATE INDEX IF NOT EXISTS idx_messages_scheduled
    ON messages(scheduled_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_messages_retry
    ON messages(next_retry_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_messages_idempotency
    ON messages(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_lease
    ON messages(lease_expires_at) WHERE status = 'processing';
```

### Consumer Groups Table (_groups.db)

```sql
CREATE TABLE IF NOT EXISTS consumer_positions (
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,    -- Last processed position
    updated_at REAL NOT NULL,
    PRIMARY KEY (group_name, topic)
);

CREATE TABLE IF NOT EXISTS dlq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    original_id INTEGER NOT NULL,           -- Reference to original message
    original_position INTEGER NOT NULL,
    data BLOB NOT NULL,
    headers TEXT,
    error TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    failed_at REAL NOT NULL,
    correlation_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_topic ON dlq(topic);
```

---

## 📦 Message Model

### File: `scriptman/queue/message.py`

```python
"""📬 Message model and status tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from scriptman.queue.backends import QueueBackend


class MessageStatus(str, Enum):
    """📊 Message lifecycle status."""

    PENDING = "pending"          # Waiting to be processed
    PROCESSING = "processing"    # Currently being processed
    COMPLETED = "completed"      # Successfully processed
    FAILED = "failed"            # Failed, moved to DLQ


@dataclass
class Message:
    """📬 A message in the queue.

    Attributes:
        id: Unique message identifier
        topic: Queue name this message belongs to
        position: Sequential position within topic
        data: The actual message payload (deserialized)
        headers: Optional metadata dict
        status: Current lifecycle status
        attempts: Number of processing attempts
        created_at: When message was pushed
        correlation_id: For Observer tracing
        error: Last error message (if any)

    Example:
        >>> @queue.consumer("orders")
        ... def process_order(msg: Message):
        ...     order = msg.data
        ...     if order["total"] > 10000:
        ...         msg.reject(reason="Amount exceeds limit")
        ...         return
        ...     process(order)
        ...     msg.accept()
    """

    id: int
    topic: str
    position: int
    data: Any
    headers: dict[str, str] = field(default_factory=dict)

    status: MessageStatus = MessageStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3

    created_at: datetime | None = None
    correlation_id: str | None = None
    error: str | None = None

    # Internal: reference to backend for ack/nack
    _backend: "QueueBackend | None" = field(default=None, repr=False)
    _group: str | None = field(default=None, repr=False)

    def accept(self) -> None:
        """✅ Mark message as successfully processed.

        Commits the consumer group's position and marks message completed.

        Example:
            >>> def process(msg: Message):
            ...     do_work(msg.data)
            ...     msg.accept()  # Done!
        """
        if self._backend is None:
            raise RuntimeError("Message not bound to backend")
        self._backend.accept(self, self._group)
        self.status = MessageStatus.COMPLETED

    def reject(self, reason: str) -> None:
        """❌ Reject message permanently — moves to Dead Letter Queue.

        Use when the message is invalid or cannot ever be processed.

        Args:
            reason: Why the message was rejected (stored in DLQ)

        Example:
            >>> def process(msg: Message):
            ...     if not is_valid(msg.data):
            ...         msg.reject(reason="Invalid payload format")
            ...         return
        """
        if self._backend is None:
            raise RuntimeError("Message not bound to backend")
        self._backend.reject(self, reason, self._group)
        self.status = MessageStatus.FAILED
        self.error = reason

    def retry(self, delay: int | None = None) -> None:
        """🔄 Request retry — message will be reprocessed later.

        Use for temporary failures (network issues, rate limits, etc.)
        After max_attempts, automatically moves to DLQ.

        Args:
            delay: Seconds to wait before retry (None = exponential backoff)

        Example:
            >>> def process(msg: Message):
            ...     try:
            ...         call_external_api(msg.data)
            ...     except RateLimitError:
            ...         msg.retry(delay=60)  # Try again in 1 minute
        """
        if self._backend is None:
            raise RuntimeError("Message not bound to backend")
        self._backend.retry(self, delay, self._group)
        self.status = MessageStatus.PENDING

    def extend_lease(self, seconds: int) -> None:
        """⏱️ Extend the visibility timeout for long-running processing.

        Call this periodically during long operations to prevent
        the message from being returned to the queue.

        Args:
            seconds: Additional seconds to extend the lease

        Example:
            >>> def process(msg: Message):
            ...     for chunk in large_data:
            ...         process_chunk(chunk)
            ...         msg.extend_lease(30)  # Keep alive
            ...     msg.accept()
        """
        if self._backend is None:
            raise RuntimeError("Message not bound to backend")
        self._backend.extend_lease(self, seconds)


@dataclass
class DeferredCall:
    """📦 A deferred function call stored in the queue.

    Used internally by @queue.defer decorator.
    """

    function_path: str      # e.g., "myapp.tasks.send_email"
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass
class DeferredResult:
    """📦 Result of a deferred function call.

    Returned by @queue.defer decorated functions instead of executing.
    """

    topic: str
    position: int
    function_path: str

    def __repr__(self) -> str:
        return f"<Deferred {self.function_path} → {self.topic}[{self.position}]>"
```

---

## 🔧 Queue Backend ABC

### File: `scriptman/queue/backends/__init__.py`

```python
"""📬 Queue backend abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from scriptman.queue.message import Message


@dataclass(slots=True)
class QueueStats:
    """📊 Queue statistics."""

    topic: str
    pending: int
    processing: int
    completed: int
    failed: int
    dlq_count: int

    @property
    def total(self) -> int:
        return self.pending + self.processing + self.completed + self.failed


class QueueBackend(ABC):
    """📬 Abstract base class for queue implementations.

    Default implementation is SQLiteQueueBackend.
    """

    # ─────────────────────────────────────────────────────────────
    # Producer API
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def push(
        self,
        topic: str,
        data: Any,
        *,
        idempotency_key: str | None | bool = None,
        headers: dict[str, str] | None = None,
        delay: int | None = None,
        ttl: int | None = None,
        max_attempts: int = 3,
        correlation_id: str | None = None,
    ) -> int:
        """📤 Push a message to a topic.

        Args:
            topic: Queue name
            data: Message payload (will be serialized)
            idempotency_key: Unique key to prevent duplicates:
                - None: Auto-generate from content hash (default)
                - str: Use provided key
                - False: Disable idempotency check
            headers: Optional metadata
            delay: Seconds to wait before processing
            ttl: Message time-to-live in seconds (None = forever)
            max_attempts: Max retry attempts before DLQ
            correlation_id: For Observer tracing

        Returns:
            Message position in topic
        """
        ...

    @abstractmethod
    def push_many(
        self,
        topic: str,
        messages: list[dict[str, Any]],
    ) -> list[int]:
        """📤 Push multiple messages atomically.

        Args:
            topic: Queue name
            messages: List of dicts with 'data' and optional 'idempotency_key', 'headers'

        Returns:
            List of message positions
        """
        ...

    @abstractmethod
    def push_deferred(
        self,
        topic: str,
        function_path: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        delay: int | None = None,
    ) -> int:
        """📤 Push a deferred function call.

        Used internally by @queue.defer decorator.
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Consumer API (Sync)
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def pull(
        self,
        topic: str,
        group: str,
        batch_size: int = 1,
        visibility_timeout: float = 300.0,
    ) -> list[Message]:
        """📥 Pull messages for processing (sync).

        Args:
            topic: Queue name
            group: Consumer group name
            batch_size: Max messages to return
            visibility_timeout: Seconds before message returns to pending if not acked

        Returns:
            List of messages (empty if none available)
        """
        ...

    @abstractmethod
    async def pull_async(
        self,
        topic: str,
        group: str,
        batch_size: int = 1,
        visibility_timeout: float = 300.0,
    ) -> list[Message]:
        """📥 Pull messages for processing (async).

        Args:
            topic: Queue name
            group: Consumer group name
            batch_size: Max messages to return
            visibility_timeout: Seconds before message returns to pending if not acked

        Returns:
            List of messages (empty if none available)
        """
        ...

    @abstractmethod
    def accept(self, message: Message, group: str | None) -> None:
        """✅ Mark message as successfully processed."""
        ...

    @abstractmethod
    def reject(self, message: Message, reason: str, group: str | None) -> None:
        """❌ Reject message and move to DLQ."""
        ...

    @abstractmethod
    def retry(self, message: Message, delay: int | None, group: str | None) -> None:
        """🔄 Schedule message for retry."""
        ...

    @abstractmethod
    def extend_lease(self, message: Message, seconds: int) -> None:
        """⏱️ Extend visibility timeout for a message."""
        ...

    # ─────────────────────────────────────────────────────────────
    # Position Management
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def current_position(self, topic: str, group: str = "default") -> int:
        """📍 Get current position for consumer group."""
        ...

    @abstractmethod
    def save_position(self, topic: str, position: int, group: str = "default") -> None:
        """💾 Save consumer group position."""
        ...

    @abstractmethod
    def jump_to_position(self, topic: str, position: int, group: str = "default") -> None:
        """⏭️ Jump to specific position (for replay)."""
        ...

    # ─────────────────────────────────────────────────────────────
    # Visibility Timeout Management
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def reclaim_expired_leases(self, topic: str) -> int:
        """🔄 Return expired processing messages to pending.

        Called periodically to handle crashed consumers.

        Returns:
            Number of messages reclaimed
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Observability
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def pending(self, topic: str, group: str = "default") -> int:
        """📊 Get count of pending messages for group."""
        ...

    @abstractmethod
    def stats(self, topic: str) -> QueueStats:
        """📊 Get queue statistics."""
        ...

    @abstractmethod
    def list_topics(self) -> list[str]:
        """📋 List all topics."""
        ...

    # ─────────────────────────────────────────────────────────────
    # DLQ Management
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def dlq(self, topic: str, limit: int = 100) -> list[Message]:
        """💀 Get messages from Dead Letter Queue."""
        ...

    @abstractmethod
    def dlq_count(self, topic: str) -> int:
        """💀 Get count of failed messages."""
        ...

    @abstractmethod
    def retry_dlq(self, topic: str) -> int:
        """🔄 Move all DLQ messages back to main queue."""
        ...

    @abstractmethod
    def purge_dlq(self, topic: str) -> int:
        """🧹 Clear DLQ for topic."""
        ...

    # ─────────────────────────────────────────────────────────────
    # Maintenance
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def cleanup(self, older_than_hours: int = 24) -> int:
        """🧹 Remove old completed messages."""
        ...

    @abstractmethod
    def purge(self, topic: str) -> int:
        """🧹 Clear all messages from topic."""
        ...

    @abstractmethod
    def close(self) -> None:
        """🔌 Close backend connections."""
        ...


# ═══════════════════════════════════════════════════════════════════════════════
# BACKEND REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

_backends: dict[str, type[QueueBackend]] = {}


def register_backend(name: str) -> Callable[[type[QueueBackend]], type[QueueBackend]]:
    """🏷️ Decorator to register a queue backend."""

    def decorator(cls: type[QueueBackend]) -> type[QueueBackend]:
        _backends[name] = cls
        return cls

    return decorator


def get_backend(name: str) -> type[QueueBackend]:
    """🔍 Get a registered backend by name."""
    if name not in _backends:
        available = ", ".join(_backends.keys()) or "(none registered)"
        raise ValueError(f"Unknown backend '{name}'. Available: {available}")
    return _backends[name]


__all__ = ["QueueBackend", "QueueStats", "register_backend", "get_backend"]
```

---

## 🏗️ SQLite Backend Implementation

### File: `scriptman/queue/backends/sqlite.py`

Key implementation points:

```python
"""📬 SQLite queue backend — one file per topic."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from pathlib import Path
from typing import Any

from scriptman._internal import log
from scriptman.database import SQLiteClient
from scriptman.queue.backends import QueueBackend, QueueStats, register_backend
from scriptman.queue.message import Message, MessageStatus
from scriptman.serialization import serialize


@register_backend("sqlite")
class SQLiteQueueBackend(QueueBackend):
    """📬 SQLite-backed queue with topic-level sharding.

    Each topic gets its own SQLite file for isolation:
    - .data/queue/orders.db
    - .data/queue/payments.db
    - .data/queue/_groups.db (shared position tracking)

    Features:
    - Visibility timeout to prevent stuck messages
    - Automatic lease expiration and reclaim
    - Idempotency with optional disable
    """

    def __init__(
        self,
        base_path: Path | None = None,
        *,
        default_visibility_timeout: float = 300.0,
    ) -> None:
        """🚀 Initialize SQLite queue backend.

        Args:
            base_path: Directory for queue databases (default: from config)
            default_visibility_timeout: Default seconds before reclaiming (300 = 5 min)
        """
        if base_path is None:
            from scriptman import config
            base_path = config.ensure_path("queue")

        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._default_visibility_timeout = default_visibility_timeout

        # Shared database for consumer group positions
        self._groups_db = SQLiteClient(self._base_path / "_groups.db")
        self._ensure_groups_schema()

        # Topic databases (lazy loaded)
        self._topic_dbs: dict[str, SQLiteClient] = {}

        log.debug(f"📬 Queue backend initialized: {self._base_path}")

    def _get_topic_db(self, topic: str) -> SQLiteClient:
        """🗄️ Get or create database for topic."""
        if topic not in self._topic_dbs:
            db_path = self._base_path / f"{topic}.db"
            self._topic_dbs[topic] = SQLiteClient(db_path)
            self._ensure_topic_schema(topic)
        return self._topic_dbs[topic]

    def _ensure_groups_schema(self) -> None:
        """📋 Create consumer groups tables if needed."""
        self._groups_db.execute("""
            CREATE TABLE IF NOT EXISTS consumer_positions (
                group_name TEXT NOT NULL,
                topic TEXT NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                PRIMARY KEY (group_name, topic)
            )
        """)
        self._groups_db.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                original_id INTEGER NOT NULL,
                original_position INTEGER NOT NULL,
                data BLOB NOT NULL,
                headers TEXT,
                error TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                failed_at REAL NOT NULL,
                correlation_id TEXT
            )
        """)
        self._groups_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dlq_topic ON dlq(topic)
        """)

    def _ensure_topic_schema(self, topic: str) -> None:
        """📋 Create messages table for topic."""
        db = self._topic_dbs[topic]
        db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position INTEGER NOT NULL,
                data BLOB NOT NULL,
                headers TEXT,
                idempotency_key TEXT UNIQUE,
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 3,
                created_at REAL NOT NULL,
                scheduled_at REAL,
                started_at REAL,
                completed_at REAL,
                next_retry_at REAL,
                expires_at REAL,
                visibility_timeout REAL,
                lease_expires_at REAL,
                error TEXT,
                correlation_id TEXT,
                function_path TEXT,
                function_args TEXT,
                function_kwargs TEXT
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_status
            ON messages(status, position)
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_scheduled
            ON messages(scheduled_at) WHERE status = 'pending'
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_idempotency
            ON messages(idempotency_key) WHERE idempotency_key IS NOT NULL
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_lease
            ON messages(lease_expires_at) WHERE status = 'processing'
        """)

    def _generate_idempotency_key(self, topic: str, data: Any) -> str:
        """🔑 Auto-generate idempotency key from content."""
        content = f"{topic}:{json.dumps(serialize(data), sort_keys=True)}"
        # Use 32 chars (128-bit) to reduce collision risk
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def _get_next_position(self, topic: str) -> int:
        """📍 Get next position for topic."""
        db = self._get_topic_db(topic)
        result = db.query_value(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM messages"
        )
        return result or 0

    # ─────────────────────────────────────────────────────────────
    # Producer API
    # ─────────────────────────────────────────────────────────────

    def push(
        self,
        topic: str,
        data: Any,
        *,
        idempotency_key: str | None | bool = None,
        headers: dict[str, str] | None = None,
        delay: int | None = None,
        ttl: int | None = None,
        max_attempts: int = 3,
        correlation_id: str | None = None,
    ) -> int:
        """📤 Push a message to a topic."""
        db = self._get_topic_db(topic)
        now = time.time()

        # Handle idempotency_key
        actual_key: str | None = None
        if idempotency_key is False:
            # Explicitly disabled
            actual_key = None
        elif idempotency_key is None:
            # Auto-generate from content
            actual_key = self._generate_idempotency_key(topic, data)
        else:
            # Use provided key
            actual_key = idempotency_key

        # Check for duplicate (only if idempotency enabled)
        if actual_key is not None:
            existing = db.query_value(
                "SELECT position FROM messages WHERE idempotency_key = :key",
                {"key": actual_key},
            )
            if existing is not None:
                log.debug(f"📬 Duplicate message ignored: {actual_key}")
                return existing

        position = self._get_next_position(topic)
        scheduled_at = now + delay if delay else now
        expires_at = now + ttl if ttl else None

        db.execute(
            """
            INSERT INTO messages (
                position, data, headers, idempotency_key, max_attempts,
                created_at, scheduled_at, expires_at, correlation_id
            ) VALUES (
                :position, :data, :headers, :key, :max_attempts,
                :created_at, :scheduled_at, :expires_at, :correlation_id
            )
            """,
            {
                "position": position,
                "data": json.dumps(serialize(data)),
                "headers": json.dumps(headers) if headers else None,
                "key": actual_key,
                "max_attempts": max_attempts,
                "created_at": now,
                "scheduled_at": scheduled_at,
                "expires_at": expires_at,
                "correlation_id": correlation_id,
            },
        )

        log.debug(f"📤 Message pushed: {topic}[{position}]")
        log.event(f"Message pushed to {topic}", "queue.push", topic=topic, position=position)

        return position

    def pull(
        self,
        topic: str,
        group: str,
        batch_size: int = 1,
        visibility_timeout: float = 300.0,
    ) -> list[Message]:
        """📥 Pull messages for processing."""
        # First, reclaim any expired leases
        self.reclaim_expired_leases(topic)

        db = self._get_topic_db(topic)
        current_pos = self.current_position(topic, group)
        now = time.time()
        lease_expires = now + visibility_timeout

        # Find available messages (pending OR processing with expired lease)
        rows = db.query(
            """
            SELECT * FROM messages
            WHERE position > :pos
              AND (
                  (status = 'pending'
                   AND (scheduled_at IS NULL OR scheduled_at <= :now)
                   AND (next_retry_at IS NULL OR next_retry_at <= :now)
                   AND (expires_at IS NULL OR expires_at > :now))
              )
            ORDER BY position
            LIMIT :limit
            """,
            {"pos": current_pos, "now": now, "limit": batch_size},
        )

        messages = []
        for row in rows:
            # Mark as processing with lease
            db.execute(
                """
                UPDATE messages
                SET status = 'processing',
                    started_at = :now,
                    attempts = attempts + 1,
                    visibility_timeout = :timeout,
                    lease_expires_at = :lease_expires
                WHERE id = :id AND status = 'pending'
                """,
                {
                    "id": row["id"],
                    "now": now,
                    "timeout": visibility_timeout,
                    "lease_expires": lease_expires,
                },
            )

            msg = Message(
                id=row["id"],
                topic=topic,
                position=row["position"],
                data=json.loads(row["data"]),
                headers=json.loads(row["headers"]) if row["headers"] else {},
                status=MessageStatus.PROCESSING,
                attempts=row["attempts"] + 1,
                max_attempts=row["max_attempts"],
                created_at=row["created_at"],
                correlation_id=row["correlation_id"],
                error=row["error"],
                _backend=self,
                _group=group,
            )
            messages.append(msg)

        return messages

    async def pull_async(
        self,
        topic: str,
        group: str,
        batch_size: int = 1,
        visibility_timeout: float = 300.0,
    ) -> list[Message]:
        """📥 Pull messages for processing (async wrapper)."""
        # Run sync pull in executor to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.pull, topic, group, batch_size, visibility_timeout
        )

    def reclaim_expired_leases(self, topic: str) -> int:
        """🔄 Return expired processing messages to pending."""
        db = self._get_topic_db(topic)
        now = time.time()

        result = db.execute(
            """
            UPDATE messages
            SET status = 'pending', started_at = NULL, lease_expires_at = NULL
            WHERE status = 'processing'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < :now
            """,
            {"now": now},
        )

        if result > 0:
            log.info(f"🔄 Reclaimed {result} expired leases in {topic}")
            log.event("Leases reclaimed", "queue.reclaim", topic=topic, count=result)

        return result

    def extend_lease(self, message: Message, seconds: int) -> None:
        """⏱️ Extend visibility timeout for a message."""
        db = self._get_topic_db(message.topic)
        now = time.time()

        db.execute(
            """
            UPDATE messages
            SET lease_expires_at = :new_expiry
            WHERE id = :id AND status = 'processing'
            """,
            {"id": message.id, "new_expiry": now + seconds},
        )

        log.debug(f"⏱️ Lease extended: {message.topic}[{message.position}] +{seconds}s")

    def accept(self, message: Message, group: str | None) -> None:
        """✅ Mark message as completed."""
        db = self._get_topic_db(message.topic)
        now = time.time()

        db.execute(
            """
            UPDATE messages
            SET status = 'completed', completed_at = :now, lease_expires_at = NULL
            WHERE id = :id
            """,
            {"id": message.id, "now": now},
        )

        # Update consumer position
        if group:
            self.save_position(message.topic, message.position, group)

        log.debug(f"✅ Message accepted: {message.topic}[{message.position}]")
        log.event("Message accepted", "queue.accept", topic=message.topic, position=message.position)

    def reject(self, message: Message, reason: str, group: str | None) -> None:
        """❌ Reject message and move to DLQ."""
        db = self._get_topic_db(message.topic)
        now = time.time()

        # Update message status
        db.execute(
            """
            UPDATE messages
            SET status = 'failed', error = :error, completed_at = :now, lease_expires_at = NULL
            WHERE id = :id
            """,
            {"id": message.id, "error": reason, "now": now},
        )

        # Add to DLQ (store reference, not duplicate data)
        self._groups_db.execute(
            """
            INSERT INTO dlq (topic, original_id, original_position, data, headers, error, attempts, failed_at, correlation_id)
            VALUES (:topic, :original_id, :position, :data, :headers, :error, :attempts, :failed_at, :correlation_id)
            """,
            {
                "topic": message.topic,
                "original_id": message.id,
                "position": message.position,
                "data": json.dumps(serialize(message.data)),
                "headers": json.dumps(message.headers) if message.headers else None,
                "error": reason,
                "attempts": message.attempts,
                "failed_at": now,
                "correlation_id": message.correlation_id,
            },
        )

        # Update consumer position (skip this message)
        if group:
            self.save_position(message.topic, message.position, group)

        log.warning(f"❌ Message rejected: {message.topic}[{message.position}] - {reason}")
        log.event("Message rejected", "queue.reject", topic=message.topic, position=message.position, reason=reason)

    def retry(self, message: Message, delay: int | None, group: str | None) -> None:
        """🔄 Schedule message for retry."""
        db = self._get_topic_db(message.topic)
        now = time.time()

        # Check if max attempts reached
        if message.attempts >= message.max_attempts:
            self.reject(message, f"Max attempts ({message.max_attempts}) reached", group)
            return

        # Calculate backoff delay
        if delay is None:
            # Exponential backoff: 1, 2, 4, 8, 16... seconds
            delay = 2 ** (message.attempts - 1)

        next_retry = now + delay

        db.execute(
            """
            UPDATE messages
            SET status = 'pending', next_retry_at = :retry_at, lease_expires_at = NULL
            WHERE id = :id
            """,
            {"id": message.id, "retry_at": next_retry},
        )

        log.debug(f"🔄 Message scheduled for retry: {message.topic}[{message.position}] in {delay}s")
        log.event("Message retry scheduled", "queue.retry", topic=message.topic, position=message.position, delay=delay)

    # ─────────────────────────────────────────────────────────────
    # Position Management
    # ─────────────────────────────────────────────────────────────

    def current_position(self, topic: str, group: str = "default") -> int:
        """📍 Get current position for consumer group."""
        result = self._groups_db.query_value(
            """
            SELECT position FROM consumer_positions
            WHERE group_name = :group AND topic = :topic
            """,
            {"group": group, "topic": topic},
        )
        return result if result is not None else -1

    def save_position(self, topic: str, position: int, group: str = "default") -> None:
        """💾 Save consumer group position."""
        now = time.time()
        self._groups_db.execute(
            """
            INSERT INTO consumer_positions (group_name, topic, position, updated_at)
            VALUES (:group, :topic, :position, :now)
            ON CONFLICT(group_name, topic) DO UPDATE SET
                position = :position, updated_at = :now
            """,
            {"group": group, "topic": topic, "position": position, "now": now},
        )

    def jump_to_position(self, topic: str, position: int, group: str = "default") -> None:
        """⏭️ Jump to specific position (for replay)."""
        self.save_position(topic, position - 1, group)
        log.info(f"📍 Consumer {group} jumped to position {position} in {topic}")

    # ─────────────────────────────────────────────────────────────
    # Observability & Maintenance (implement remaining methods)
    # ─────────────────────────────────────────────────────────────

    def pending(self, topic: str, group: str = "default") -> int:
        """📊 Get count of pending messages for group."""
        db = self._get_topic_db(topic)
        current_pos = self.current_position(topic, group)
        now = time.time()

        result = db.query_value(
            """
            SELECT COUNT(*) FROM messages
            WHERE position > :pos
              AND status = 'pending'
              AND (scheduled_at IS NULL OR scheduled_at <= :now)
              AND (next_retry_at IS NULL OR next_retry_at <= :now)
              AND (expires_at IS NULL OR expires_at > :now)
            """,
            {"pos": current_pos, "now": now},
        )
        return result or 0

    def stats(self, topic: str) -> QueueStats:
        """📊 Get queue statistics."""
        db = self._get_topic_db(topic)

        counts = db.query_one(
            """
            SELECT
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM messages
            """
        )

        dlq_count = self._groups_db.query_value(
            "SELECT COUNT(*) FROM dlq WHERE topic = :topic",
            {"topic": topic},
        )

        return QueueStats(
            topic=topic,
            pending=counts["pending"] or 0,
            processing=counts["processing"] or 0,
            completed=counts["completed"] or 0,
            failed=counts["failed"] or 0,
            dlq_count=dlq_count or 0,
        )

    def list_topics(self) -> list[str]:
        """📋 List all topics."""
        topics = []
        for path in self._base_path.glob("*.db"):
            name = path.stem
            if not name.startswith("_"):  # Exclude _groups.db
                topics.append(name)
        return sorted(topics)

    def dlq(self, topic: str, limit: int = 100) -> list[Message]:
        """💀 Get messages from Dead Letter Queue."""
        rows = self._groups_db.query(
            """
            SELECT * FROM dlq WHERE topic = :topic ORDER BY failed_at DESC LIMIT :limit
            """,
            {"topic": topic, "limit": limit},
        )

        messages = []
        for row in rows:
            msg = Message(
                id=row["original_id"],
                topic=topic,
                position=row["original_position"],
                data=json.loads(row["data"]),
                headers=json.loads(row["headers"]) if row["headers"] else {},
                status=MessageStatus.FAILED,
                attempts=row["attempts"],
                error=row["error"],
                correlation_id=row["correlation_id"],
            )
            messages.append(msg)

        return messages

    def dlq_count(self, topic: str) -> int:
        """💀 Get count of failed messages."""
        result = self._groups_db.query_value(
            "SELECT COUNT(*) FROM dlq WHERE topic = :topic",
            {"topic": topic},
        )
        return result or 0

    def retry_dlq(self, topic: str) -> int:
        """🔄 Move all DLQ messages back to main queue."""
        rows = self._groups_db.query(
            "SELECT * FROM dlq WHERE topic = :topic",
            {"topic": topic},
        )

        count = 0
        for row in rows:
            # Re-push to queue (disable idempotency to allow re-processing)
            self.push(
                topic,
                json.loads(row["data"]),
                idempotency_key=False,  # Allow duplicate
                headers=json.loads(row["headers"]) if row["headers"] else None,
                correlation_id=row["correlation_id"],
            )
            count += 1

        # Clear DLQ entries
        self._groups_db.execute(
            "DELETE FROM dlq WHERE topic = :topic",
            {"topic": topic},
        )

        log.info(f"🔄 Retried {count} DLQ messages for {topic}")
        return count

    def purge_dlq(self, topic: str) -> int:
        """🧹 Clear DLQ for topic."""
        result = self._groups_db.execute(
            "DELETE FROM dlq WHERE topic = :topic",
            {"topic": topic},
        )
        log.info(f"🧹 Purged {result} DLQ entries for {topic}")
        return result

    def cleanup(self, older_than_hours: int = 24) -> int:
        """🧹 Remove old completed messages."""
        cutoff = time.time() - (older_than_hours * 3600)
        total = 0

        for topic in self.list_topics():
            db = self._get_topic_db(topic)
            result = db.execute(
                """
                DELETE FROM messages
                WHERE status = 'completed' AND completed_at < :cutoff
                """,
                {"cutoff": cutoff},
            )
            total += result

        if total > 0:
            log.info(f"🧹 Cleaned up {total} completed messages")
        return total

    def purge(self, topic: str) -> int:
        """🧹 Clear all messages from topic."""
        db = self._get_topic_db(topic)
        result = db.execute("DELETE FROM messages")
        log.warning(f"🧹 Purged {result} messages from {topic}")
        return result

    def close(self) -> None:
        """🔌 Close all database connections."""
        for db in self._topic_dbs.values():
            db.close()
        self._groups_db.close()
        self._topic_dbs.clear()
        log.debug("📬 Queue backend closed")
```

---

## 🎀 Queue Manager and Decorators

### File: `scriptman/queue/__init__.py`

```python
"""📬 Scriptman Queue — Kafka-inspired message queue made simple.

Quick Start:
    >>> from scriptman import queue
    >>>
    >>> # Push a message
    >>> queue.push("orders", {"order_id": "123", "items": [...]})
    >>>
    >>> # Process messages
    >>> @queue.consumer("orders")
    ... def process_order(msg: Message):
    ...     do_work(msg.data)
    ...     msg.accept()
    >>>
    >>> # Fire-and-forget from API
    >>> @queue.producer("events")
    ... def log_event(event: dict):
    ...     return event  # Automatically queued
    >>>
    >>> # Defer function execution
    >>> @queue.defer("background")
    ... def send_email(to: str, subject: str):
    ...     smtp.send(to, subject)
    >>>
    >>> send_email("user@example.com", "Hello!")  # Queued, not executed

Features:
    - SQLite-backed persistent storage
    - Consumer groups for parallel processing
    - Idempotency by default (no duplicates)
    - Dead Letter Queue for failed messages
    - Position tracking for replay
    - Visibility timeout for stuck messages
    - Full Observer integration
    - Async support for FastAPI
"""

from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Callable, Coroutine
from functools import wraps
from threading import Lock
from typing import Any, TypeVar, cast, overload

from scriptman._internal import log
from scriptman.queue.backends import QueueBackend, QueueStats
from scriptman.queue.backends.sqlite import SQLiteQueueBackend
from scriptman.queue.message import DeferredResult, Message, MessageStatus
from scriptman.types import P, R, T, is_async

__all__ = [
    "queue",
    "Queue",
    "Message",
    "MessageStatus",
    "QueueBackend",
    "QueueStats",
    "DeferredResult",
]


class Queue:
    """📬 Scriptman queue manager — message queuing made simple.

    Provides:
    - Producer API: push(), push_many()
    - Consumer API: @consumer decorator
    - Fire-and-forget: @producer decorator
    - Deferred execution: @defer decorator
    - Position management: current_position(), jump_to_position()
    - DLQ management: dlq(), retry_dlq()

    The Queue class is a singleton — all imports share the same instance.
    """

    _instance: "Queue | None" = None
    _lock: Lock = Lock()
    __initialized: bool = False

    def __new__(cls) -> "Queue":
        """🔒 Thread-safe singleton."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """🚀 Initialize queue manager."""
        if self.__initialized:
            return

        self._backend: QueueBackend | None = None
        self._consumers: dict[str, list[tuple[str, Callable]]] = {}  # topic -> [(group, handler)]
        self._running: bool = False

        self.__initialized = True

    @property
    def backend(self) -> QueueBackend:
        """🗄️ Get queue backend (lazy initialization)."""
        if self._backend is None:
            self._backend = SQLiteQueueBackend()
        return self._backend

    # ═══════════════════════════════════════════════════════════════
    # PRODUCER API
    # ═══════════════════════════════════════════════════════════════

    def push(
        self,
        topic: str,
        data: Any,
        *,
        idempotency_key: str | None | bool = None,
        headers: dict[str, str] | None = None,
        delay: int | None = None,
        ttl: int | None = None,
        max_attempts: int = 3,
    ) -> int:
        """📤 Push a message to a topic.

        Args:
            topic: Queue name (e.g., "orders", "emails")
            data: Message payload (any serializable data)
            idempotency_key: Duplicate prevention:
                - None: Auto-generate from content (default)
                - str: Use provided key
                - False: Disable idempotency (allow duplicates)
            headers: Optional metadata dict
            delay: Seconds to wait before processing
            ttl: Message time-to-live in seconds
            max_attempts: Max retry attempts before moving to DLQ

        Returns:
            Message position in topic

        Example:
            >>> queue.push("orders", {"order_id": "123", "items": [...]})
            0
            >>> queue.push("payments", data, idempotency_key=f"pay:{order_id}")
            0
            >>> queue.push("events", data, idempotency_key=False)  # Allow duplicates
            1
        """
        from scriptman.observe import observe

        ctx = observe.get_current_context()
        correlation_id = ctx.correlation_id if ctx else None

        return self.backend.push(
            topic,
            data,
            idempotency_key=idempotency_key,
            headers=headers,
            delay=delay,
            ttl=ttl,
            max_attempts=max_attempts,
            correlation_id=correlation_id,
        )

    def push_many(
        self,
        topic: str,
        messages: list[dict[str, Any]],
    ) -> list[int]:
        """📤 Push multiple messages atomically.

        Args:
            topic: Queue name
            messages: List of dicts, each with 'data' and optional 'idempotency_key', 'headers'

        Returns:
            List of message positions

        Example:
            >>> queue.push_many("events", [
            ...     {"data": {"type": "login", "user": "alice"}},
            ...     {"data": {"type": "purchase", "amount": 99}},
            ... ])
            [0, 1]
        """
        return self.backend.push_many(topic, messages)

    # ═══════════════════════════════════════════════════════════════
    # DECORATORS (with @overload for proper typing)
    # ═══════════════════════════════════════════════════════════════

    # --- @producer decorator ---

    @overload
    def producer(
        self, topic: str
    ) -> Callable[
        [Callable[P, Coroutine[Any, Any, T]]], Callable[P, Coroutine[Any, Any, T]]
    ]: ...

    @overload
    def producer(self, topic: str) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def producer(self, topic: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """🎀 Decorator: Queue the function's return value.

        Perfect for API endpoints where you want to return immediately
        and process the data asynchronously.

        Args:
            topic: Queue to push to

        Example:
            >>> @app.post("/orders")
            ... @queue.producer("orders")
            ... async def create_order(order: OrderRequest):
            ...     return order.dict()  # ← This gets queued
            ...
            >>> # Request returns immediately
            >>> # Order processed by @queue.consumer("orders") later
        """

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            if is_async(func):

                @wraps(func)
                async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                    result = await func(*args, **kwargs)  # type: ignore[misc]
                    self.push(topic, result)
                    return result

                return cast(Callable[P, R], async_wrapper)
            else:

                @wraps(func)
                def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                    result = func(*args, **kwargs)
                    self.push(topic, result)
                    return result

                return sync_wrapper

        return decorator

    # --- @consumer decorator ---

    def consumer(
        self,
        topic: str,
        *,
        group: str | None = None,
        batch_size: int = 1,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
    ) -> Callable[[Callable[[Message], None]], Callable[[Message], None]]:
        """🎀 Decorator: Register a message consumer.

        The decorated function is called for each message. Handles:
        - Auto-accept on success
        - Auto-retry on exception
        - Auto-DLQ after max retries

        Args:
            topic: Queue to consume from
            group: Consumer group name (default: topic name)
            batch_size: Messages to process per pull (1 = one at a time)
            max_retries: Max retry attempts before DLQ
            retry_backoff: Base delay for exponential backoff

        Example:
            >>> @queue.consumer("orders")
            ... def process_order(msg: Message):
            ...     order = msg.data
            ...     validate(order)
            ...     fulfill(order)
            ...     # Auto-accepts if no exception
            ...
            >>> @queue.consumer("orders", group="analytics")
            ... def track_order(msg: Message):
            ...     metrics.record(msg.data)
            ...     # Different group = processes all orders independently
        """
        actual_group = group or topic

        def decorator(func: Callable[[Message], None]) -> Callable[[Message], None]:
            # Register consumer
            if topic not in self._consumers:
                self._consumers[topic] = []
            self._consumers[topic].append((actual_group, func))

            log.debug(f"📬 Consumer registered: {func.__name__} for {topic}[{actual_group}]")

            @wraps(func)
            def wrapper(msg: Message) -> None:
                try:
                    func(msg)
                    # Auto-accept if not already handled
                    if msg.status == MessageStatus.PROCESSING:
                        msg.accept()
                except Exception as e:
                    log.exception(f"❌ Consumer error: {e}", topic=topic, position=msg.position)
                    # Auto-retry
                    if msg.status == MessageStatus.PROCESSING:
                        msg.retry()

            return wrapper

        return decorator

    # --- @defer decorator ---

    def defer(
        self, topic: str = "deferred", *, delay: int | None = None
    ) -> Callable[[Callable[P, R]], Callable[P, DeferredResult]]:
        """🎀 Decorator: Queue the function call itself for later execution.

        When the decorated function is called, instead of executing immediately,
        it queues the call (function path + args) for a worker to execute later.

        Note: The decorated function returns DeferredResult instead of the
        original return type. This is intentional — the actual execution
        happens later in a worker.

        Args:
            topic: Queue for deferred calls (default: "deferred")
            delay: Optional delay in seconds before execution

        Example:
            >>> @queue.defer("emails")
            ... def send_welcome_email(user_id: str, template: str) -> bool:
            ...     user = get_user(user_id)
            ...     return email_service.send(user.email, render(template))
            ...
            >>> # This returns DeferredResult immediately, doesn't send email
            >>> result = send_welcome_email("user_123", "welcome")
            >>> print(result)  # <Deferred myapp.send_welcome_email → emails[0]>
            ...
            >>> # Worker processes later:
            >>> # 1. Deserializes: function="myapp.send_welcome_email", args=["user_123", "welcome"]
            >>> # 2. Imports and calls the actual function
        """

        def decorator(func: Callable[P, R]) -> Callable[P, DeferredResult]:
            function_path = f"{func.__module__}.{func.__qualname__}"

            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> DeferredResult:
                position = self.backend.push_deferred(
                    topic,
                    function_path,
                    args,
                    kwargs,
                    delay=delay,
                )
                log.debug(f"📬 Deferred: {function_path}")
                return DeferredResult(
                    topic=topic,
                    position=position,
                    function_path=function_path,
                )

            return wrapper

        return decorator

    # ═══════════════════════════════════════════════════════════════
    # POSITION MANAGEMENT
    # ═══════════════════════════════════════════════════════════════

    def current_position(self, topic: str, group: str = "default") -> int:
        """📍 Get current position for consumer group."""
        return self.backend.current_position(topic, group)

    def save_position(self, topic: str, position: int, group: str = "default") -> None:
        """💾 Manually save consumer group position."""
        self.backend.save_position(topic, position, group)

    def jump_to_position(self, topic: str, position: int, group: str = "default") -> None:
        """⏭️ Jump to specific position (for replay or skip)."""
        self.backend.jump_to_position(topic, position, group)

    def restart(self, topic: str, group: str = "default") -> None:
        """⏮️ Restart from beginning (replay all messages)."""
        self.jump_to_position(topic, 0, group)

    def skip_to_end(self, topic: str, group: str = "default") -> None:
        """⏭️ Skip to end (ignore backlog)."""
        stats = self.backend.stats(topic)
        self.backend.save_position(topic, stats.total, group)

    # ═══════════════════════════════════════════════════════════════
    # OBSERVABILITY
    # ═══════════════════════════════════════════════════════════════

    def pending(self, topic: str, group: str = "default") -> int:
        """📊 Get count of pending messages for consumer group."""
        return self.backend.pending(topic, group)

    def stats(self, topic: str) -> QueueStats:
        """📊 Get queue statistics."""
        return self.backend.stats(topic)

    def list_topics(self) -> list[str]:
        """📋 List all topics."""
        return self.backend.list_topics()

    # ═══════════════════════════════════════════════════════════════
    # DLQ MANAGEMENT
    # ═══════════════════════════════════════════════════════════════

    def dlq(self, topic: str, limit: int = 100) -> list[Message]:
        """💀 Get messages from Dead Letter Queue."""
        return self.backend.dlq(topic, limit)

    def dlq_count(self, topic: str) -> int:
        """💀 Get count of failed messages."""
        return self.backend.dlq_count(topic)

    def retry_dlq(self, topic: str) -> int:
        """🔄 Move all DLQ messages back to main queue for retry."""
        return self.backend.retry_dlq(topic)

    def purge_dlq(self, topic: str) -> int:
        """🧹 Clear DLQ for topic."""
        return self.backend.purge_dlq(topic)

    # ═══════════════════════════════════════════════════════════════
    # MAINTENANCE
    # ═══════════════════════════════════════════════════════════════

    def cleanup(self, older_than_hours: int = 24) -> int:
        """🧹 Remove old completed messages."""
        return self.backend.cleanup(older_than_hours)

    def purge(self, topic: str) -> int:
        """🧹 Clear all messages from topic. ⚠️ Destructive!"""
        return self.backend.purge(topic)

    # ═══════════════════════════════════════════════════════════════
    # WORKER EXECUTION
    # ═══════════════════════════════════════════════════════════════

    def run_workers(
        self,
        topics: list[str] | None = None,
        poll_interval: float = 1.0,
        visibility_timeout: float = 300.0,
    ) -> None:
        """🏃 Start processing messages with registered consumers (sync).

        Runs until interrupted (Ctrl+C or SIGTERM).

        Args:
            topics: Topics to process (None = all registered)
            poll_interval: Seconds between polls
            visibility_timeout: Seconds before unacked messages return to pending

        Example:
            >>> @queue.consumer("orders")
            ... def process(msg): ...
            >>>
            >>> queue.run_workers()  # Blocks, processes messages
        """
        import time

        if topics is None:
            topics = list(self._consumers.keys())

        self._running = True

        # Handle graceful shutdown
        def shutdown_handler(signum: int, frame: Any) -> None:
            log.info("🛑 Shutdown signal received, stopping workers...")
            self._running = False

        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)

        log.info(f"🏃 Starting workers for: {topics}")

        while self._running:
            for topic in topics:
                if not self._running:
                    break

                if topic not in self._consumers:
                    continue

                for group, handler in self._consumers[topic]:
                    messages = self.backend.pull(
                        topic, group, batch_size=1, visibility_timeout=visibility_timeout
                    )
                    for msg in messages:
                        handler(msg)

            time.sleep(poll_interval)

        log.info("🛑 Workers stopped")

    async def run_workers_async(
        self,
        topics: list[str] | None = None,
        poll_interval: float = 1.0,
        visibility_timeout: float = 300.0,
    ) -> None:
        """🏃 Start processing messages with registered consumers (async).

        Runs until cancelled.

        Args:
            topics: Topics to process (None = all registered)
            poll_interval: Seconds between polls
            visibility_timeout: Seconds before unacked messages return to pending

        Example:
            >>> @queue.consumer("orders")
            ... def process(msg): ...
            >>>
            >>> # In async context:
            >>> await queue.run_workers_async()
        """
        if topics is None:
            topics = list(self._consumers.keys())

        self._running = True

        log.info(f"🏃 Starting async workers for: {topics}")

        try:
            while self._running:
                for topic in topics:
                    if not self._running:
                        break

                    if topic not in self._consumers:
                        continue

                    for group, handler in self._consumers[topic]:
                        messages = await self.backend.pull_async(
                            topic, group, batch_size=1, visibility_timeout=visibility_timeout
                        )
                        for msg in messages:
                            # Run sync handler in executor if needed
                            if is_async(handler):
                                await handler(msg)  # type: ignore
                            else:
                                await asyncio.get_event_loop().run_in_executor(
                                    None, handler, msg
                                )

                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            log.info("🛑 Async workers cancelled")
            self._running = False

        log.info("🛑 Async workers stopped")

    def stop_workers(self) -> None:
        """🛑 Signal workers to stop gracefully."""
        self._running = False


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════

queue = Queue()
```

---

## ⚙️ Config Schema

### File: `scriptman/queue/schema.py`

```python
"""📬 Queue configuration schema."""

from pydantic import BaseModel, Field


class QueueConfig(BaseModel):
    """📬 Queue configuration.

    Example (scriptman.toml):
        [queue]
        default_max_attempts = 3
        default_visibility_timeout = 300
        retention_hours = 168
        cleanup_interval_hours = 1

    Note: Queue storage path is determined by data.queue setting,
    which defaults to ".data/queue".
    """

    default_max_attempts: int = Field(
        default=3,
        description="Default max retry attempts before DLQ",
        ge=1,
    )

    default_visibility_timeout: float = Field(
        default=300.0,
        description="Default seconds before stuck messages return to pending",
        ge=1.0,
    )

    default_retry_backoff: float = Field(
        default=1.0,
        description="Base delay for exponential backoff (seconds)",
        ge=0.1,
    )

    retention_hours: int = Field(
        default=168,  # 7 days
        description="How long to keep completed messages",
        ge=1,
    )

    cleanup_interval_hours: int = Field(
        default=1,
        description="How often to run cleanup",
        ge=1,
    )

    idempotency_enabled: bool = Field(
        default=True,
        description="Enable idempotency by default",
    )

    idempotency_window_hours: int = Field(
        default=24,
        description="How long idempotency keys are valid",
        ge=1,
    )

    reclaim_interval_seconds: float = Field(
        default=60.0,
        description="How often to reclaim expired leases",
        ge=1.0,
    )
```

---

## ✅ Testing Checklist

### File: `tests/test_queue.py`

```python
# Test categories:

class TestProducerAPI:
    """📤 Test push(), push_many()"""
    def test_push_basic(self): ...
    def test_push_with_delay(self): ...
    def test_push_with_ttl(self): ...
    def test_push_idempotency_auto(self): ...
    def test_push_idempotency_explicit(self): ...
    def test_push_idempotency_duplicate_ignored(self): ...
    def test_push_idempotency_disabled(self): ...
    def test_push_many(self): ...

class TestConsumerAPI:
    """📥 Test pull, accept, reject, retry"""
    def test_pull_returns_messages(self): ...
    def test_pull_async(self): ...
    def test_accept_marks_completed(self): ...
    def test_reject_moves_to_dlq(self): ...
    def test_retry_schedules_retry(self): ...
    def test_retry_max_attempts_to_dlq(self): ...

class TestVisibilityTimeout:
    """⏱️ Test visibility timeout and lease management"""
    def test_visibility_timeout_default(self): ...
    def test_visibility_timeout_custom(self): ...
    def test_expired_lease_reclaimed(self): ...
    def test_extend_lease(self): ...
    def test_reclaim_expired_leases(self): ...

class TestConsumerGroups:
    """👥 Test group isolation and position tracking"""
    def test_groups_track_position_independently(self): ...
    def test_same_group_shares_work(self): ...
    def test_different_groups_see_all_messages(self): ...

class TestPositionManagement:
    """📍 Test position tracking and replay"""
    def test_current_position(self): ...
    def test_save_position(self): ...
    def test_jump_to_position(self): ...
    def test_restart_replays_all(self): ...
    def test_skip_to_end(self): ...

class TestDLQ:
    """💀 Test Dead Letter Queue"""
    def test_dlq_contains_failed(self): ...
    def test_retry_dlq_moves_back(self): ...
    def test_purge_dlq(self): ...
    def test_dlq_preserves_headers(self): ...

class TestDecorators:
    """🎀 Test @producer, @consumer, @defer"""
    def test_producer_queues_return_value(self): ...
    def test_producer_async(self): ...
    def test_consumer_auto_accepts(self): ...
    def test_consumer_auto_retries_on_error(self): ...
    def test_defer_queues_function_call(self): ...
    def test_defer_returns_deferred_result(self): ...
    def test_defer_worker_executes_call(self): ...

class TestWorkers:
    """🏃 Test worker execution"""
    def test_run_workers_processes_messages(self): ...
    def test_run_workers_async(self): ...
    def test_graceful_shutdown(self): ...
    def test_stop_workers(self): ...

class TestObservability:
    """📊 Test Observer integration"""
    def test_push_emits_event(self): ...
    def test_accept_emits_event(self): ...
    def test_reject_emits_event(self): ...
    def test_correlation_id_flows(self): ...

class TestMessageTTL:
    """⏰ Test message expiration"""
    def test_expired_message_not_pulled(self): ...
    def test_ttl_respected(self): ...
```

---

## 📊 Implementation Summary

| Component                   | Lines (est.) | Complexity |
| --------------------------- | ------------ | ---------- |
| `message.py`                | ~150         | Low        |
| `backends/__init__.py`      | ~200         | Low        |
| `backends/sqlite.py`        | ~500         | Medium     |
| `__init__.py` (Queue class) | ~450         | Medium     |
| `schema.py`                 | ~60          | Low        |
| Tests                       | ~700         | Medium     |

**Total:** ~2060 lines
**Duration:** ~10-12 days

---

## 🚀 Implementation Order

1. **Pre-requisites** (0.5 day)
   - Add `"queue"` to `DataCategory`
   - Update `DataConfig` with queue path

2. **Message model + enums** (`message.py`) — 0.5 day

3. **QueueBackend ABC** (`backends/__init__.py`) — 0.5 day

4. **SQLiteQueueBackend** (`backends/sqlite.py`) — 4 days
   - Schema creation with visibility timeout fields
   - Push with idempotency control
   - Pull with lease management
   - Accept/reject/retry with lease cleanup
   - Reclaim expired leases
   - Position management
   - Stats and observability

5. **Queue manager** (`__init__.py`) — 2.5 days
   - Singleton pattern
   - Decorators with @overload: @producer, @consumer, @defer
   - DeferredResult return type for @defer
   - Sync worker loop with graceful shutdown
   - Async worker loop

6. **Config schema** (`schema.py`) — 0.5 day

7. **Update main `__init__.py`** — Add queue export

8. **Tests** — 3 days

---

## 🔗 Integration with Scheduler (Future)

When the Scheduler module is implemented, add these scheduled tasks:

```python
from scriptman import scheduler, queue

@scheduler.interval(seconds=60)
def reclaim_expired_leases():
    """🔄 Reclaim stuck messages every minute."""
    for topic in queue.list_topics():
        queue.backend.reclaim_expired_leases(topic)

@scheduler.interval(hours=1)
def cleanup_completed():
    """🧹 Clean up old completed messages hourly."""
    queue.cleanup(older_than_hours=24)
```

---

## 🎯 TIMS Integration Example

```python
from scriptman import queue, observe
from fastapi import FastAPI

app = FastAPI()

# ═══════════════════════════════════════════════════════════════
# API ENDPOINT — Queue invoice for async processing
# ═══════════════════════════════════════════════════════════════

@app.post("/invoices")
@queue.producer("tims_submissions")
@observe(entity="invoice")
async def submit_invoice(invoice: InvoiceRequest):
    """Invoice is validated and queued. Returns immediately."""
    validate_invoice(invoice)
    return invoice.dict()

# ═══════════════════════════════════════════════════════════════
# WORKER — Submit to KRA (slow, needs retries)
# ═══════════════════════════════════════════════════════════════

@queue.consumer("tims_submissions", group="kra-submitters")
@observe(entity="invoice")
def submit_to_kra(msg: Message):
    """Submit invoice to KRA. Retries on temporary failures."""
    invoice = msg.data

    try:
        # Extend lease for long-running operations
        msg.extend_lease(120)

        response = kra_api.submit(invoice)
        store_kra_response(invoice["id"], response)

        # Queue notification (allow duplicate notifications)
        queue.push("notifications", {
            "type": "invoice_submitted",
            "invoice_id": invoice["id"],
            "cu_number": response["cu_number"],
        }, idempotency_key=False)

        msg.accept()

    except KRATemporaryError as e:
        # Network issue, rate limit, etc.
        msg.retry(delay=300)  # Try again in 5 minutes

    except KRAValidationError as e:
        # Invalid data — won't ever succeed
        msg.reject(reason=f"KRA validation failed: {e}")

# ═══════════════════════════════════════════════════════════════
# WORKER — Send notifications (different group, independent)
# ═══════════════════════════════════════════════════════════════

@queue.consumer("notifications", group="notifiers")
def send_notification(msg: Message):
    """Send SMS/email notifications."""
    notification = msg.data

    if notification["type"] == "invoice_submitted":
        send_sms(f"Invoice submitted. CU: {notification['cu_number']}")

    msg.accept()

# ═══════════════════════════════════════════════════════════════
# RUN WORKERS
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    queue.run_workers()

# Or for async (e.g., in FastAPI startup):
# @app.on_event("startup")
# async def start_workers():
#     asyncio.create_task(queue.run_workers_async())
```

---

## 📝 Notes

1. **Topic sharding** — Each topic is a separate SQLite file. This provides natural isolation and allows different topics to have different I/O patterns.

2. **Consumer groups** — The killer feature. Multiple groups = parallel pipelines. Same group = load sharing.

3. **Idempotency control** — Auto-generated keys prevent accidental duplicates. Use `idempotency_key=False` to allow duplicates when needed.

4. **Position-based terminology** — "Position" is clearer than "offset" for beginners. The queue is like a book, position is your bookmark.

5. **Three decorators** — @producer (queue return value), @consumer (process messages), @defer (queue function call). Clear purposes, no confusion.

6. **Observer integration** — Correlation IDs flow from producer → message → consumer. Full traceability.

7. **Visibility timeout** — Prevents stuck messages. If a consumer crashes, the message automatically returns to pending after the timeout expires.

8. **Async support** — `pull_async()` and `run_workers_async()` for FastAPI and other async frameworks.

9. **Graceful shutdown** — Workers respond to SIGTERM/SIGINT and stop cleanly.

10. **DeferredResult** — The @defer decorator returns a DeferredResult object instead of the original return type, making it clear the function wasn't executed yet.
