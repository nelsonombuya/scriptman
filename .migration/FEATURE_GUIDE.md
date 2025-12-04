# 🦸‍♂️ Scriptman v3 Feature Guide

> **Last Updated:** 2024-12-04
> **Version:** 3.0.0-alpha

This document tracks all implemented features, planned features, and future ideas for Scriptman v3. It serves as a living reference for development and migration.

---

## 📊 Status Legend

| Status | Meaning                   |
| ------ | ------------------------- |
| ✅      | Implemented and tested    |
| 🚧      | In progress               |
| 📋      | Planned (committed to v3) |
| 💡      | Idea (not committed)      |
| ❌      | Removed/deprecated        |

---

## 📦 Core Modules

### Config ✅

**Location:** `scriptman/config/`

Configuration management with multi-format support and priority chain.

| Feature                         | Status | Description                          |
| ------------------------------- | ------ | ------------------------------------ |
| **Core API**                    |        |                                      |
| `config.get(key, default)`      | ✅      | Get config value with dot notation   |
| `config.set(key, value)`        | ✅      | Set and persist config value         |
| `config["key"]`                 | ✅      | Bracket notation access              |
| `config.keys()`                 | ✅      | List all valid config keys           |
| `config.items()`                | ✅      | Get all key-value pairs              |
| **Overrides**                   |        |                                      |
| `config.override(**kwargs)`     | ✅      | Runtime overrides (not persisted)    |
| `config.temporary(**kwargs)`    | ✅      | Scoped overrides via context manager |
| `config.clear_overrides()`      | ✅      | Clear all runtime overrides          |
| **Readers**                     |        |                                      |
| TOML reader                     | ✅      | `scriptman.toml`, `pyproject.toml`   |
| ENV reader                      | ✅      | `SCRIPTMAN_*` environment variables  |
| Auto-discovery                  | ✅      | Automatically finds config files     |
| Reader migration                | ✅      | `config.migrate_to(new_reader)`      |
| JSON reader                     | 📋      | Planned                              |
| YAML reader                     | 📋      | Planned                              |
| **Schema**                      |        |                                      |
| Pydantic validation             | ✅      | Type-safe config values              |
| Schema defaults                 | ✅      | Sensible defaults for all keys       |
| Field introspection             | ✅      | `ConfigSchema.get_field_info()`      |
| Example generation              | ✅      | `config.generate_example()`          |
| **Path Resolution**             |        |                                      |
| `config.resolve_path(category)` | ✅      | Resolve data directory paths         |
| `config.ensure_path(category)`  | ✅      | Resolve and create if needed         |
| **Secrets**                     |        |                                      |
| Secrets instance                | ✅      | `config.secrets.get()`               |
| Separate secrets file           | ✅      | `.secrets.toml` / `.secrets.yaml`    |
| **Observer Integration**        |        |                                      |
| Config change events            | ✅      | Emits events on set/override         |

**Config Schema Sections:**
- `data.*` - Data storage paths (dir, logs, db, cache, artifacts)
- `logging.*` - Logging configuration (level)
- `execution.*` - Execution configuration (concurrent)
- `observe.*` - Observer configuration (enabled, store path)

---

### Observer ✅

**Location:** `scriptman/observe/`

Telemetry and observability for tracking operations, correlating events, and querying history.

| Feature                                  | Status | Description                                 |
| ---------------------------------------- | ------ | ------------------------------------------- |
| **Logging**                              |        |                                             |
| `observe.debug(msg, **data)`             | ✅      | Debug level log                             |
| `observe.info(msg, **data)`              | ✅      | Info level log                              |
| `observe.warning(msg, **data)`           | ✅      | Warning level log                           |
| `observe.error(msg, **data)`             | ✅      | Error level log                             |
| `observe.success(msg, **data)`           | ✅      | Success log (custom level)                  |
| `observe.critical(msg, **data)`          | ✅      | Critical level log                          |
| `observe.exception(msg, **data)`         | ✅      | Exception with traceback                    |
| **Spans**                                |        |                                             |
| `observe.span()`                         | ✅      | Context manager for tracking operations     |
| `Span.set_data(**data)`                  | ✅      | Add data to span                            |
| `Span.set_metric(name, value)`           | ✅      | Set metric on span                          |
| `Span.increment(name, amount)`           | ✅      | Increment counter                           |
| Nested spans                             | ✅      | Parent-child correlation                    |
| Duration tracking                        | ✅      | Automatic timing                            |
| Outcome tracking                         | ✅      | SUCCESS/ERROR/STARTED                       |
| **Decorator**                            |        |                                             |
| `@observe`                               | ✅      | Auto-track function execution               |
| `@observe(operation=...)`                | ✅      | Custom operation name                       |
| `@observe(entity=..., entity_param=...)` | ✅      | Entity tracking                             |
| `@observe(capture_args=True)`            | ✅      | Capture function arguments                  |
| `@observe(capture_result=True)`          | ✅      | Capture return value                        |
| Async support                            | 📋      | Currently sync only                         |
| **Events**                               |        |                                             |
| `observe.event(msg, **data)`             | ✅      | Emit custom event                           |
| `observe.metric(name, value)`            | ✅      | Emit metric event                           |
| Event types                              | ✅      | LOG, SPAN_START, SPAN_END, METRIC, CONFIG_* |
| **Queries**                              |        |                                             |
| `observe.find(**filters)`                | ✅      | Find events with filters                    |
| `observe.find_one(**filters)`            | ✅      | Find single event                           |
| `observe.stats(**filters)`               | ✅      | Aggregate statistics                        |
| `observe.errors(**filters)`              | ✅      | Error statistics                            |
| `observe.context_tree(id)`               | ✅      | Get full context tree                       |
| **Context**                              |        |                                             |
| Correlation IDs                          | ✅      | Automatic correlation                       |
| Parent-child linking                     | ✅      | Nested context tracking                     |
| `get_current_context()`                  | ✅      | Access current context                      |
| **Storage**                              |        |                                             |
| SQLite store                             | ✅      | Default persistent storage                  |
| `get_store()` / `set_store()`            | ✅      | Custom store backends                       |

**Event Fields:**
- `id`, `timestamp`, `type`, `level`, `message`
- `correlation_id`, `parent_id` (for nesting)
- `entity_type`, `entity_id`, `operation`
- `outcome`, `duration_ms`
- `error_type`, `error_message`
- `data` (JSON metadata)

---

### Database ✅

**Location:** `scriptman/database/`

Simple database operations with consistent API across backends.

| Feature                             | Status | Description                            |
| ----------------------------------- | ------ | -------------------------------------- |
| **Core API**                        |        |                                        |
| `db.query(sql, params)`             | ✅      | SELECT returning list of dicts         |
| `db.query_one(sql, params)`         | ✅      | SELECT returning first row             |
| `db.query_value(sql, params)`       | ✅      | SELECT returning single value          |
| `db.execute(sql, params)`           | ✅      | INSERT/UPDATE/DELETE                   |
| `db.execute_many(sql, params_list)` | ✅      | Bulk operations                        |
| `db.table_exists(name)`             | ✅      | Check if table exists                  |
| **Connection**                      |        |                                        |
| `db.connect()`                      | ✅      | Establish connection                   |
| `db.close()`                        | ✅      | Close connection                       |
| `db.is_connected`                   | ✅      | Connection status                      |
| Context manager                     | ✅      | `with SQLiteClient() as db:`           |
| **Transactions**                    |        |                                        |
| `db.transaction()`                  | ✅      | Transaction context manager            |
| Auto-commit                         | ✅      | Per execute by default                 |
| Auto-rollback                       | ✅      | On exception in transaction            |
| **SQLite Client**                   |        |                                        |
| WAL mode                            | ✅      | Better concurrency                     |
| Thread safety                       | ✅      | `check_same_thread=False`              |
| Config-based paths                  | ✅      | Uses `config.ensure_path("db")`        |
| Auto-reconnect                      | ✅      | Reconnects on stale connection         |
| **Backends**                        |        |                                        |
| `SQLiteClient`                      | ✅      | Built-in SQLite                        |
| `PostgresClient`                    | 📋      | Planned (requires scriptman[postgres]) |
| `MSSQLClient`                       | 📋      | Planned (requires scriptman[mssql])    |
| **Query Portability**               |        |                                        |
| `:name` parameter syntax            | ✅      | Universal syntax                       |
| `_prepare_query()` hook             | ✅      | Convert to backend-native format       |

---

### CLI ✅

**Location:** `scriptman/cli/`

Command-line interface for configuration, development, and observability.

| Feature                              | Status | Description                  |
| ------------------------------------ | ------ | ---------------------------- |
| **Core**                             |        |                              |
| `scriptman` entry point              | ✅      | Main CLI command             |
| Subcommand routing                   | ✅      | `scriptman <command>`        |
| Help text                            | ✅      | `scriptman --help`           |
| **Config Commands**                  |        |                              |
| `scriptman config get <key>`         | ✅      | Get config value             |
| `scriptman config set <key> <value>` | ✅      | Set config value             |
| `scriptman config list`              | ✅      | List all config              |
| `scriptman config reset <key>`       | ✅      | Reset to default             |
| `scriptman config migrate <format>`  | 📋      | Migrate config format        |
| **Observe Commands**                 |        |                              |
| `scriptman observe query`            | ✅      | Query events                 |
| `scriptman observe stats`            | ✅      | Show statistics              |
| `scriptman observe errors`           | 📋      | Show error summary           |
| **Dev Commands**                     |        |                              |
| `scriptman dev lint`                 | ✅      | Run linting (ruff)           |
| `scriptman dev bump <part>`          | ✅      | Bump version                 |
| `scriptman dev publish`              | ✅      | Publish to PyPI              |
| Dev-only restriction                 | ✅      | Only works in scriptman repo |

---

### Serialization ✅

**Location:** `scriptman/serialization.py`

Simple type conversion utilities for config, APIs, and caching.

| Feature            | Status | Description                |
| ------------------ | ------ | -------------------------- |
| `serialize(value)` | ✅      | Convert to JSON-compatible |
| Path support       | ✅      | `Path` → string            |
| DateTime support   | ✅      | `datetime` → ISO string    |
| Enum support       | ✅      | `Enum` → value             |
| UUID support       | ✅      | `UUID` → string            |
| Decimal support    | ✅      | `Decimal` → string         |
| Pydantic support   | ✅      | `BaseModel` → dict         |
| Nested structures  | ✅      | Recursive handling         |

---

### Internal Safe Logging ✅

**Location:** `scriptman/_internal/log.py`

Safe internal logging that handles circular imports gracefully.

| Feature                    | Status | Description                      |
| -------------------------- | ------ | -------------------------------- |
| `log.trace/debug/info/...` | ✅      | All log levels                   |
| `log.exception()`          | ✅      | Error with traceback             |
| `log.event()`              | ✅      | Custom event emission            |
| Observe delegation         | ✅      | Uses observe when available      |
| Loguru fallback            | ✅      | Falls back during initialization |
| Recursion prevention       | ✅      | `_current_state` sentinel        |
| Format integration         | ✅      | Respects configured presets      |

---

## 📋 Planned Modules

### Cache ✅

**Location:** `scriptman/cache/`

SQLite-backed caching with TTL, tags, LRU eviction, and sharding.

| Feature                            | Status | Description                         |
| ---------------------------------- | ------ | ----------------------------------- |
| **Core API**                       |        |                                     |
| `cache.get(key)`                   | ✅      | Retrieve cached value               |
| `cache.set(key, value, ttl, tags)` | ✅      | Store with TTL and tags             |
| `cache.delete(key)`                | ✅      | Delete cached value                 |
| `cache.exists(key)`                | ✅      | Check if key exists                 |
| `cache.clear()`                    | ✅      | Clear all cached values             |
| **Bulk Operations**                |        |                                     |
| `cache.get_many(keys)`             | ✅      | Get multiple values                 |
| `cache.set_many(items, ttl, tags)` | ✅      | Set multiple values                 |
| `cache.delete_many(keys)`          | ✅      | Delete multiple values              |
| **Tags**                           |        |                                     |
| Tag support                        | ✅      | Group related items                 |
| `cache.invalidate_tag(tag)`        | ✅      | Delete all tagged items             |
| `cache.get_by_tag(tag)`            | ✅      | Get all tagged items                |
| **Decorator**                      |        |                                     |
| `@cache.result(ttl, tags)`         | ✅      | Cache function results              |
| Sync support                       | ✅      | Synchronous functions               |
| Async support                      | ✅      | Asynchronous functions              |
| Key generation                     | ✅      | Auto-generate cache keys            |
| Custom key function                | ✅      | `key_fn` parameter                  |
| **Expiration**                     |        |                                     |
| TTL (time-to-live)                 | ✅      | Expire after N seconds              |
| Lazy expiration                    | ✅      | Check on read                       |
| Active expiration                  | ✅      | Background cleanup (via scheduler)  |
| **Eviction**                       |        |                                     |
| LRU eviction                       | ✅      | Least recently used                 |
| Size limits                        | ✅      | Max cache size in bytes             |
| `cache.evict_lru(count)`           | ✅      | Manual LRU eviction                 |
| **Stampede Prevention**            |        |                                     |
| Lock on compute                    | ✅      | Prevent thundering herd             |
| `cache.get_or_set(key, factory)`   | ✅      | Atomic get-or-compute               |
| Async locks                        | ✅      | For async functions                 |
| **Statistics**                     |        |                                     |
| Hit count                          | ✅      | Successful gets                     |
| Miss count                         | ✅      | Failed gets                         |
| Hit ratio                          | ✅      | hits / (hits + misses)              |
| Size in bytes                      | ✅      | Total cache size                    |
| Key count                          | ✅      | Number of entries                   |
| **Sharding**                       |        |                                     |
| `ShardedSQLiteBackend`             | ✅      | Multiple SQLite files               |
| Consistent hashing                 | ✅      | Key-based shard selection           |
| **Backends**                       |        |                                     |
| `CacheBackend` ABC                 | ✅      | Extensible backend interface        |
| `SQLiteCacheBackend`               | ✅      | Default SQLite backend              |
| `MemoryCacheBackend`               | 💡      | For testing                         |
| `RedisCacheBackend`                | 💡      | Distributed caching                 |
| **Observer Integration**           |        |                                     |
| Auto-log operations                | ✅      | Log hits/misses via `_internal.log` |

---

### Queue 💡

**Location:** `scriptman/queue/` (future)

SQLite-backed message queue for reliable task processing.

| Feature              | Status | Description                    |
| -------------------- | ------ | ------------------------------ |
| `queue.push(data)`   | 💡      | Add message to queue           |
| `queue.consume()`    | 💡      | Get messages for processing    |
| `@queue.worker()`    | 💡      | Process messages automatically |
| Retry with backoff   | 💡      | Automatic retries              |
| Dead letter queue    | 💡      | Failed message handling        |
| Observer integration | 💡      | Correlation with spans         |

---

### Scheduler 💡

**Location:** `scriptman/scheduler/` (future, migrate from powers)

Task scheduling with interval, time-of-day, and cron-like triggers.

| Feature              | Status | Description                 |
| -------------------- | ------ | --------------------------- |
| Interval triggers    | 💡      | Run every N seconds/minutes |
| Time-of-day triggers | 💡      | Run at specific time        |
| Cron-like triggers   | 💡      | Complex schedules           |
| One-time triggers    | 💡      | Run once at time            |
| Observer integration | 💡      | Track scheduled executions  |

---

### Retry 💡

**Location:** `scriptman/retry/` (future, migrate from powers)

Retry decorator with exponential backoff.

| Feature                | Status | Description                   |
| ---------------------- | ------ | ----------------------------- |
| `@retry(max_attempts)` | 💡      | Retry on failure              |
| Exponential backoff    | 💡      | Increasing delays             |
| Custom exceptions      | 💡      | Retry only on specific errors |
| Sync/async support     | 💡      | Works with both               |

---

### API 💡

**Location:** `scriptman/api/` (future, migrate from powers)

FastAPI-based REST API utilities.

| Feature              | Status | Description            |
| -------------------- | ------ | ---------------------- |
| Quick API setup      | 💡      | Simple API creation    |
| Auth decorators      | 💡      | Authentication helpers |
| Rate limiting        | 💡      | Request throttling     |
| Observer integration | 💡      | Request tracing        |

---

### Dashboard 💡

**Location:** `scriptman/dashboard/` (future)

Web UI for monitoring and managing Scriptman.

| Feature          | Status | Description              |
| ---------------- | ------ | ------------------------ |
| Cache inspector  | 💡      | View/manage cached items |
| Event viewer     | 💡      | Query and view events    |
| Metrics graphs   | 💡      | Visualize statistics     |
| Config editor    | 💡      | Edit configuration       |
| Docs renderer    | 💡      | Markdown documentation   |
| HTMX/Jinja2      | 💡      | Lightweight frontend     |
| Streamlit option | 💡      | Alternative frontend     |

---

## 🔧 Utilities

### Types 🚧

**Location:** `scriptman/types.py`

Reusable type definitions for sync/async function handling. Uses `@overload` pattern for decorators.

| Feature                      | Status | Description                                         |
| ---------------------------- | ------ | --------------------------------------------------- |
| Type aliases                 | 🚧      | `T`, `P`, `R`, `C`, `Func`, `AsyncFunc` (simplified) |
| `is_async()`                 | ✅      | Check if function is async def                      |
| `@overload` pattern          | 📋      | Documented pattern for sync/async decorators        |
| `StampedeLock`               | ✅      | Per-key locks for stampede prevention               |
| `AsyncLock`                  | ✅      | Dual sync/async lock                                |

**Migration needed:**
- Remove `SyncFunc` (redundant, same as `Func`)
- Remove `SyncOrAsyncFunc` (replaced by `@overload` pattern)
- Remove `await_async` (no longer sync-ifying)
- Remove `wrap_function`, `wrap_function_in_context`, `make_sync_async_decorator` (use `@overload` directly)
- Update `AsyncFunc` to use `Coroutine[Any, Any, R]` instead of `Awaitable[R]`

**Documentation:** See `docs/sync-async-decorators.md` for the new pattern.

---

## 🔀 Migration from Powers

The `powers/` module from v2 contains valuable features to migrate:

| Module                      | Status | Target Location                  |
| --------------------------- | ------ | -------------------------------- |
| `powers/database/`          | ✅      | `scriptman/database/` (migrated) |
| `powers/cache/`             | ✅      | `scriptman/cache/` (migrated)    |
| `powers/tasks/`             | 💡      | `scriptman/tasks/`               |
| `powers/scheduler/`         | 💡      | `scriptman/scheduler/`           |
| `powers/retry.py`           | 💡      | `scriptman/retry.py`             |
| `powers/api/`               | 💡      | `scriptman/api/`                 |
| `powers/service/`           | 💡      | `scriptman/services/`            |
| `powers/time_calculator.py` | 💡      | `scriptman/utils/time.py`        |

---

## 📁 Data Directory Structure

```
.data/                          # Base data directory
├── db/                         # Database files
│   └── default.db              # Default SQLite database
├── cache/                      # Cache storage
│   ├── cache.db                # Single-shard cache
│   └── cache_*.db              # Sharded cache files
├── observe/                    # Observer storage
│   └── events.db               # Event store
├── logs/                       # Log files
└── artifacts/                  # Build artifacts
```

---

## 📝 Changelog

### v3.0.0-alpha (Current)

**New:**
- Config module with multi-format readers
- Observer module for telemetry
- Database module with SQLiteClient
- CLI with config, observe, and dev commands
- Serialization utilities

**Removed:**
- `core/` directory (flattened structure)
- `orchestrator/` (over-engineered)
- Old `tasks/` module (will be redesigned)

**Changed:**
- Flat module structure (`scriptman.config` not `scriptman.core.config`)
- ABCs over Protocols for internal abstractions
- Explicit method access over magic attributes

---

## 🎯 Design Principles

1. **Simple Things Simple** — Zero-config for common use cases
2. **Complex Things Possible** — Advanced options available
3. **ABCs Over Protocols** — Clear contracts with enforcement
4. **Explicit Over Magic** — `config.get()` not `config.key`
5. **Observer by Default** — Built-in telemetry everywhere
6. **SQLite First** — Local-first, Redis/Postgres for scale
7. **Sync-First** — Async is opt-in enhancement

---

## 📚 References

- **Migration Plans:** See `.migration/` folder
- **Development Rules:** See `.cursor/rules/scriptman.mdc`
- **API Documentation:** Coming soon
