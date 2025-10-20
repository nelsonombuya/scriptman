# ETL Architecture Documentation

Internal architecture and design documentation for the Scriptman ETL module.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Component Architecture](#component-architecture)
3. [Queue Management System](#queue-management-system)
4. [Deadlock Prevention](#deadlock-prevention)
5. [Retry Mechanism](#retry-mechanism)
6. [Table Name Extraction](#table-name-extraction)
7. [Performance Considerations](#performance-considerations)
8. [Thread Safety](#thread-safety)

---

## System Overview

The ETL module is designed around three core principles:

1. **Safety First**: Prevent deadlocks through table-level queueing
2. **Simplicity**: Automatic detection and configuration
3. **Performance**: Parallel execution where safe, optimized batch operations

### Design Philosophy

```
┌─────────────────────────────────────────────────────────┐
│                    User-Facing API                      │
│  ┌──────────────────────────────────────────────────┐  │
│  │ ETL Class                                        │  │
│  │ - Simple, chainable methods                      │  │
│  │ - from_db(), to_db(), transform()                │  │
│  │ - Hides complexity from users                    │  │
│  └─────────────┬────────────────────────────────────┘  │
└────────────────┼───────────────────────────────────────┘
                 │
┌────────────────▼───────────────────────────────────────┐
│               Orchestration Layer                      │
│  ┌──────────────────────────────────────────────────┐ │
│  │ ETLDatabase Class                                │ │
│  │ - Wraps DatabaseHandler (composition)            │ │
│  │ - Coordinates queue, retry, extraction           │ │
│  │ - Automatic table name detection                 │ │
│  └─────────────┬────────────────────────────────────┘ │
└────────────────┼───────────────────────────────────────┘
                 │
┌────────────────▼───────────────────────────────────────┐
│              Infrastructure Layer                      │
│  ┌──────────────────┐  ┌──────────────────────────┐   │
│  │_TableQueueManager│  │ DatabaseHandler          │   │
│  │- Singleton       │  │ - Connection pools       │   │
│  │- Semaphores      │  │ - Query execution        │   │
│  │- Cleanup         │  │ - Database-specific logic│   │
│  └──────────────────┘  └──────────────────────────┘   │
└───────────────────────────────────────────────────────┘
```

---

## Component Architecture

### 1. ETL Class (`__init__.py`)

**Purpose**: High-level user interface for ETL operations

**Key Responsibilities**:
- Data extraction from multiple sources
- Transformation operations (filtering, merging, etc.)
- Data loading to multiple destinations
- Column name transformations
- Nested data handling

**Design Pattern**: Fluent interface with method chaining

```python
# Example of method chaining
result = (
    ETL.from_db(db, query)
    .to_snake_case()
    .filter(condition)
    .set_index('id', inplace=True)
    .to_db(db, "table")
)
```

**Internal State**:
- `_data`: pandas DataFrame (core data storage)
- `columns`, `empty`, `index`: Delegated DataFrame properties

---

### 2. ETLDatabase Class (`_database.py`)

**Purpose**: Composition wrapper around DatabaseHandler with ETL-specific features

**Key Responsibilities**:
- Automatic table name extraction from SQL queries
- Queue management coordination
- Retry logic application
- Schema synchronization
- Query generation for different database types

**Design Pattern**: Composition (wraps DatabaseHandler)

```python
class ETLDatabase:
    def __init__(self, database_handler: DatabaseHandler):
        self.db = database_handler  # Composition, not inheritance
        self._queue_manager = _TableQueueManager()  # Singleton
```

**Why Composition?**:
- Doesn't modify DatabaseHandler behavior
- Can wrap any DatabaseHandler implementation
- Clear separation of concerns
- Easy to test and maintain

**Key Methods**:

| Method | Purpose |
|--------|---------|
| `_extract_table_name_from_query()` | Parse SQL to find table name |
| `_table_operation_lock()` | Context manager for queue coordination |
| `execute_read_query()` | Read with retry + queue |
| `execute_write_query()` | Write with retry + queue |
| `synchronize_table_schema()` | Auto-add missing columns |

---

### 3. _TableQueueManager Class (`_queue.py`)

**Purpose**: Singleton manager for table-level semaphores

**Key Responsibilities**:
- Create and manage per-table semaphores
- Track active operations per table
- Automatic cleanup of unused semaphores
- Thread-safe operations

**Design Pattern**: Thread-safe Singleton with double-checked locking

```python
class _TableQueueManager:
    __instance: Optional["_TableQueueManager"] = None
    __lock: RLock = RLock()

    def __new__(cls) -> "_TableQueueManager":
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:  # Double-check
                    cls.__instance = super().__new__(cls)
        return cls.__instance
```

**Internal State**:
- `__table_semaphores`: dict[str, Semaphore] - One semaphore per table
- `__active_operations`: dict[str, set[str]] - Track active operation IDs
- `_queue_lock`: RLock for thread-safe access
- `__last_cleanup`: Timestamp for periodic cleanup

**Semaphore Management**:
```python
# Table key format: "server:database:table_name"
table_key = "prod_server:sales_db:customers"

# Each table gets its own semaphore (always max_concurrent=1)
semaphore = Semaphore(1)
```

---

## Queue Management System

### How It Works

```
┌─────────────────────────────────────────────────────┐
│ Thread 1: INSERT INTO customers                     │
│                                                     │
│  1. Extract table name → "customers"                │
│  2. Get table key → "server:db:customers"           │
│  3. Get/create semaphore for this table             │
│  4. semaphore.acquire() → SUCCESS (no one using it) │
│  5. Execute query                                   │
│  6. semaphore.release()                             │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ Thread 2: UPDATE customers (concurrent)             │
│                                                     │
│  1. Extract table name → "customers"                │
│  2. Get table key → "server:db:customers"           │
│  3. Get semaphore (same as Thread 1)                │
│  4. semaphore.acquire() → BLOCKED (Thread 1 using)  │
│  5. ⏳ WAITING...                                    │
│  6. Thread 1 releases → Thread 2 acquires           │
│  7. Execute query                                   │
│  8. semaphore.release()                             │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ Thread 3: INSERT INTO orders (concurrent)           │
│                                                     │
│  1. Extract table name → "orders"                   │
│  2. Get table key → "server:db:orders"              │
│  3. Get/create semaphore (DIFFERENT from customers) │
│  4. semaphore.acquire() → SUCCESS                   │
│  5. Execute query (PARALLEL with Thread 1 & 2!)     │
│  6. semaphore.release()                             │
└─────────────────────────────────────────────────────┘
```

### Table Key Generation

```python
def _get_table_key(self, table_name: str) -> str:
    """Generate unique key for table across database instances"""
    return f"{self._database_identifier}:{table_name}"
    # Example: "prod_server:sales_db:customers"
```

**Why include server and database?**
- Same table name in different databases should have separate queues
- `customers` table in `sales_db` vs `customers` table in `warehouse_db`
- Prevents unnecessary blocking across unrelated databases

### Cleanup Mechanism

```python
def cleanup_if_needed(self) -> None:
    """Cleanup empty queues periodically"""
    current_time = time()

    # Cleanup every 5 minutes (300 seconds)
    if current_time - self.__last_cleanup > 300:
        # Find tables with no active operations
        empty_tables = [
            table_key for table_key, operations
            in self.__active_operations.items()
            if len(operations) == 0
        ]

        # Remove semaphores and operation trackers
        for table_key in empty_tables:
            del self.__table_semaphores[table_key]
            del self.__active_operations[table_key]
```

**Benefits**:
- Prevents memory leaks in long-running applications
- Automatic, no user intervention needed
- Conservative 5-minute window ensures cleanup doesn't interfere

---

## Deadlock Prevention

### The Problem

```python
# ❌ WITHOUT queue management (potential deadlock):

Thread 1: UPDATE customers SET status = 'active' WHERE id = 1
Thread 2: UPDATE customers SET status = 'inactive' WHERE id = 2

# Both threads try to lock rows in the customers table
# Database may escalate to table-level locks
# Deadlock can occur if they're waiting on each other
```

### The Solution

```python
# ✅ WITH queue management (safe):

Thread 1: Acquires "customers" semaphore
         → UPDATE customers ...
         → Releases semaphore

Thread 2: Waits for "customers" semaphore
         → Acquires when Thread 1 releases
         → UPDATE customers ...
         → Releases semaphore

# Only one operation on "customers" table at a time
# Impossible to deadlock!
```

### Why `max_concurrent_per_table = 1`?

**Hard-coded to 1 because:**

1. **Row-level locks can escalate**:
   ```python
   # Both update different rows, but...
   Thread 1: UPDATE customers SET ... WHERE id = 1
   Thread 2: UPDATE customers SET ... WHERE id = 2
   # Database might escalate to table lock → deadlock
   ```

2. **Index contention**:
   ```python
   # Different rows, same index pages
   Thread 1: INSERT INTO orders VALUES (...)
   Thread 2: INSERT INTO orders VALUES (...)
   # Both need to update index → lock contention
   ```

3. **MERGE/UPSERT complexity**:
   ```python
   # MERGE involves READ + WRITE
   Thread 1: MERGE INTO products ...
   Thread 2: MERGE INTO products ...
   # Complex locking patterns → deadlock risk
   ```

4. **Simplicity > Micro-optimization**:
   - Performance gain from `max_concurrent=2` is minimal
   - Deadlock risk is significant
   - One simple rule: One operation per table

---

## Retry Mechanism

### Decorator Pattern

```python
_retry_database_errors = retry(
    retry_condition=DatabaseHandler.retry_conditions,
    base_delay=10.0,
    max_delay=60.0,
    max_retries=5,
)

@_retry_database_errors
def execute_write_query(self, query: str, params: dict = {}) -> bool:
    with self._table_operation_lock("write", query):
        return self.db.execute_write_query(query, params)
```

### Retry Flow

```
┌──────────────────────────────────────────────────┐
│ Attempt 1: Execute query                        │
│ ❌ ConnectionTimeout                             │
└──────────────┬───────────────────────────────────┘
               │
               ▼ Wait 10 seconds (base_delay)
┌──────────────────────────────────────────────────┐
│ Attempt 2: Execute query                        │
│ ❌ PoolExhausted                                 │
└──────────────┬───────────────────────────────────┘
               │
               ▼ Wait 20 seconds (exponential backoff)
┌──────────────────────────────────────────────────┐
│ Attempt 3: Execute query                        │
│ ✅ SUCCESS                                       │
└──────────────────────────────────────────────────┘
```

**Important**: Semaphore is held during retries!
```python
# Semaphore acquired once, held through all retries
semaphore.acquire()
try:
    for attempt in range(max_retries):
        try:
            return execute_query()  # Retry on failure
        except RetriableError:
            if attempt < max_retries - 1:
                sleep(backoff_delay)
finally:
    semaphore.release()  # Only released after all retries
```

**Implications**:
- Other operations on same table wait during retries
- Max wait time: 5 retries × 60s = ~5 minutes
- Queue can back up if database is slow/down

---

## Table Name Extraction

### Regex Patterns

```python
patterns = [
    # MERGE (must be first to avoid UPDATE SET confusion)
    r'MERGE\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))\s+AS\s+TARGET',
    r'MERGE\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',

    # INSERT INTO
    r'INSERT\s+INTO\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',

    # UPDATE (exclude "UPDATE SET" from MERGE)
    r'(?<!THEN\s)UPDATE\s+(?!\s*SET\s)(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',

    # DELETE FROM
    r'DELETE\s+FROM\s+(?:\[([^\]]+)\]|"([^"]+)"|([^\s,()]+))',

    # TRUNCATE, DROP, CREATE, ALTER
    ...
]
```

### Supported Formats

| Format | Example | Extracted |
|--------|---------|-----------|
| Unquoted | `INSERT INTO customers` | `customers` |
| Brackets | `UPDATE [dbo].[orders]` | `orders` |
| Quotes | `DELETE FROM "products"` | `products` |
| Schema.Table | `INSERT INTO dbo.customers` | `customers` |
| Mixed | `MERGE [dbo].[customers] AS TARGET` | `customers` |

### Extraction Process

```python
def _extract_table_name_from_query(self, query: str) -> str:
    # 1. Normalize whitespace
    clean_query = sub(r"\s+", " ", query.strip())
    original_query = clean_query  # Preserve case
    clean_query = clean_query.upper()  # For matching

    # 2. Try each pattern
    for pattern in patterns:
        if search(pattern, clean_query):
            # Match in uppercase, extract from original (preserves case)
            original_match = search(pattern, original_query, IGNORECASE)

            # 3. Extract table name
            table_name = extract_from_match(original_match)

            # 4. Clean up
            table_name = table_name.strip('"[]`')

            # 5. Handle schema.table format
            if "." in table_name:
                table_name = table_name.split(".")[-1]

            return table_name

    # No pattern matched
    return ""  # Execute without queue
```

### What Doesn't Work

**Complex CTEs**:
```sql
WITH cte AS (
    SELECT * FROM source
)
INSERT INTO target  -- ❌ Not extracted
SELECT * FROM cte
```

**Stored Procedures**:
```sql
EXEC sp_insert_customer @id=1  -- ❌ Not extracted
```

**Subqueries**:
```sql
INSERT INTO (
    SELECT * FROM customers WHERE ...
)  -- ❌ Not extracted
```

**Impact**: These execute **without queue protection**. Ensure they don't conflict.

---

## Performance Considerations

### Parallel vs Serial Execution

```python
# ✅ PARALLEL (different tables)
Thread 1: INSERT INTO customers     → Own semaphore
Thread 2: INSERT INTO orders        → Own semaphore
Thread 3: INSERT INTO products      → Own semaphore
# All execute simultaneously

# ⚠️ SERIAL (same table)
Thread 1: INSERT INTO customers (batch 1)  → Acquires semaphore
Thread 2: INSERT INTO customers (batch 2)  → Waits
Thread 3: INSERT INTO customers (batch 3)  → Waits
# Execute one at a time
```

### Optimization Strategies

**1. Batch Within Same Table**:
```python
# ❌ SLOW (3 queue acquisitions)
for batch in batches:
    ETL(batch).to_db(db, "customers")

# ✅ FAST (1 queue acquisition)
all_data = pd.concat(batches)
ETL(all_data).to_db(db, "customers")
```

**2. Distribute Across Tables**:
```python
# Design schema to spread load
# Instead of: all_logs table
# Use: logs_2024_01, logs_2024_02, logs_2024_03
# Each is a different table = parallel execution
```

**3. Tune Batch Size**:
```python
# Test to find optimal batch size
etl.to_db(db, "table", batch_size=5000)  # Test different values
```

### Memory Management

**Semaphore Dictionary Growth**:
```python
# Each unique table creates a semaphore
# Example: Daily partitioned tables
for i in range(365):
    etl.to_db(db, f"logs_2024_{i:03d}")
# Creates 365 semaphores

# ✅ Cleanup every 5 minutes removes unused ones
```

**Monitoring**:
```python
# Add to ETLDatabase for debugging
def get_queue_stats(self) -> dict:
    return {
        "total_semaphores": len(self._queue_manager._TableQueueManager__table_semaphores),
        "active_tables": sum(
            1 for ops in self._queue_manager._TableQueueManager__active_operations.values()
            if ops
        )
    }
```

---

## Thread Safety

### Thread-Safe Components

**1. _TableQueueManager**:
```python
# All operations protected by locks
with self._queue_lock:
    if table_key not in self.__table_semaphores:
        self.__table_semaphores[table_key] = Semaphore(1)
```

**2. Semaphore Operations**:
```python
# Semaphore.acquire() and release() are thread-safe (built-in)
semaphore.acquire()  # Atomic operation
try:
    # Critical section
finally:
    semaphore.release()  # Atomic operation
```

**3. DatabaseHandler Connection Pool**:
- SQLAlchemy/pyodbc pools are thread-safe
- Each thread gets its own connection from pool
- Connections returned to pool after use

### Not Thread-Safe

**ETL Instance Data**:
```python
# ❌ Don't share ETL instance across threads
etl = ETL(data)
thread1.start(lambda: etl.to_db(db, "table1"))
thread2.start(lambda: etl.to_db(db, "table2"))  # Race condition!

# ✅ Create separate instances
thread1.start(lambda: ETL(data1).to_db(db, "table1"))
thread2.start(lambda: ETL(data2).to_db(db, "table2"))
```

---

## Design Decisions

### Why Composition Over Inheritance?

```python
# ❌ Could have done (but didn't):
class ETLDatabase(DatabaseHandler):
    pass

# ✅ Did instead:
class ETLDatabase:
    def __init__(self, database_handler: DatabaseHandler):
        self.db = database_handler
```

**Reasons**:
1. **Flexibility**: Can wrap any DatabaseHandler implementation
2. **Separation**: ETL concerns separate from database concerns
3. **Testing**: Easy to mock DatabaseHandler
4. **Maintainability**: Changes to DatabaseHandler don't break ETL

### Why Remove Caching?

**Removed in v2.0.0**:
- **Complexity**: Cache invalidation is hard
- **Edge cases**: JOIN queries need all table tags
- **Memory**: Cache can grow large
- **Flexibility**: Users can implement custom caching if needed
- **Simplicity**: Less code = fewer bugs

### Why Auto-Extract Table Names?

**Instead of requiring users to specify**:
- **User convenience**: One less parameter
- **Less error-prone**: No typos in table names
- **Automatic**: Works for 95% of cases
- **Safe fallback**: Executes without queue if extraction fails

---

## Future Considerations

### Potential Enhancements

1. **Manual Table Override**:
   ```python
   # For complex queries where extraction fails
   etl.to_db(db, "table", _force_table_name="actual_table")
   ```

2. **Queue Statistics**:
   ```python
   stats = db.get_queue_statistics()
   # {"active_tables": 5, "waiting_operations": 2}
   ```

3. **Timeout on Semaphore Acquire**:
   ```python
   if not semaphore.acquire(timeout=300):  # 5 min timeout
       raise TimeoutError("Could not acquire table lock")
   ```

4. **Per-Database Queue Managers**:
   ```python
   # Currently: One global singleton
   # Could be: One per database for isolation
   ```

---

## See Also

- [ETL README](./ETL_README.md) - User documentation
- [ETL API Reference](./ETL_API_Reference.md) - API docs
- [ETL Examples](./ETL_Examples.md) - Usage examples
- [Main Documentation](../../README.md) - Back to documentation index

