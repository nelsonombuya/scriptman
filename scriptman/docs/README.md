# Scriptman Documentation

Welcome to the Scriptman documentation! This directory contains comprehensive documentation for all Scriptman modules.

---

## ETL Module Documentation

Complete documentation for the **Extract, Transform, Load (ETL)** module.

### Quick Links

| Document | Description |
|----------|-------------|
| [**ETL README**](./powers/etl/ETL_README.md) | 📘 Overview, quick start, and best practices |
| [**ETL API Reference**](./powers/etl/ETL_API_Reference.md) | 📖 Complete API documentation with all methods |
| [**ETL Examples**](./powers/etl/ETL_Examples.md) | 💡 Real-world usage examples and patterns |
| [**ETL Architecture**](./powers/etl/ETL_Architecture.md) | 🏗️ Internal architecture and design decisions |

---

## Getting Started with ETL

### Installation

```bash
# Install with ETL dependencies
pip install scriptman[etl]

# Or with poetry
poetry add scriptman[etl]
```

### Quick Example

```python
from scriptman.powers.etl import ETL
from scriptman.powers.database import DatabaseHandler

# Initialize database
db = DatabaseHandler(server="localhost", database="mydb")

# Extract → Transform → Load
(
    ETL.from_db(db, "SELECT * FROM customers WHERE active = 1")
    .to_snake_case()
    .set_index('customer_id', inplace=True)
    .to_db(db, "active_customers", method="upsert")
)
```

---

## Documentation Structure

### For Users

Start here if you're **using** the ETL module:

1. **[ETL README](./powers/etl/ETL_README.md)** - Start here for overview and quick start
2. **[ETL Examples](./powers/etl/ETL_Examples.md)** - See real-world usage patterns
3. **[ETL API Reference](./powers/etl/ETL_API_Reference.md)** - Look up specific methods

### For Contributors

Start here if you're **developing** or **contributing** to the ETL module:

1. **[ETL Architecture](./powers/etl/ETL_Architecture.md)** - Understand the internal design
2. **[ETL API Reference](./powers/etl/ETL_API_Reference.md)** - See method signatures
3. **Source Code** - Dive into `scriptman/powers/etl/`

---

## Key Features

### 🔒 Deadlock Prevention
- Automatic table-level queueing
- One operation per table at a time
- Parallel execution across different tables

### 🔄 Automatic Retry
- Retries transient database failures
- Exponential backoff (10s → 60s)
- Up to 5 retry attempts

### 🎯 Smart Detection
- Automatic table name extraction from SQL
- No manual configuration needed
- Works with INSERT, UPDATE, DELETE, MERGE, etc.

### 📊 Data Transformation
- Column name conversions (snake_case, camelCase)
- Nested data flattening
- Merge, filter, concat operations
- SQL-safe name sanitization

### 🚀 Performance
- Batch operations support
- Connection pool optimization
- Parallel table operations
- Memory-efficient processing

---

## Common Use Cases

### Database Synchronization
```python
# Sync data between databases
ETL.from_db(source_db, "SELECT * FROM table").to_db(target_db, "table")
```

### Data Warehousing
```python
# Extract from source, transform, load to warehouse
etl = (
    ETL.from_db(prod_db, "SELECT * FROM sales")
    .transform(add_calculated_fields)
    .to_db(warehouse_db, "sales_fact", method="upsert")
)
```

### Data Migration
```python
# Migrate from CSV to database with schema sync
ETL.from_csv("legacy_data.csv").to_db(
    db, "new_table",
    method="upsert",
    synchronize_schema=True
)
```

### Daily Reports
```python
# Generate and load daily aggregates
daily_report = (
    ETL.from_db(db, "SELECT * FROM transactions WHERE date = :date",
                params={"date": today})
    .transform(aggregate_metrics)
    .to_db(db, "daily_reports", method="insert")
)
```

---

## Version Information

**Current Version**: v2.0.0

### Breaking Changes from v1.x

- ❌ Removed `from_db_cached()` and `from_db_fresh()` methods
- ❌ Removed `use_cache` and `cache_ttl` parameters
- ❌ Removed `table_name` parameter from ETL methods
- ❌ Removed `max_concurrent_per_table` parameter

See [ETL README - Migration Guide](./powers/etl/ETL_README.md#migration-from-v1x) for details.

---

## Support and Contributing

### Found a Bug?
Please report issues in the project's issue tracker.

### Have a Question?
Check the [Troubleshooting section](./powers/etl/ETL_README.md#troubleshooting) in the ETL README.

### Want to Contribute?
Read the [Architecture documentation](./powers/etl/ETL_Architecture.md) to understand the internals.

---

## Module Documentation (Coming Soon)

- **Database Module** - DatabaseHandler and connection management
- **Cache Module** - Caching utilities and backends
- **Tasks Module** - Parallel execution and task management
- **Selenium Module** - Browser automation and workflows
- **API Module** - REST API utilities

---

## License

See project LICENSE file for details.

---

**Last Updated**: October 2024 | **Version**: 2.0.0

