# ETL Usage Examples

Comprehensive examples demonstrating the Scriptman ETL module capabilities.

---

## Table of Contents

1. [Basic Examples](#basic-examples)
2. [Data Extraction](#data-extraction)
3. [Data Transformation](#data-transformation)
4. [Data Loading](#data-loading)
5. [Complete ETL Pipelines](#complete-etl-pipelines)
6. [Advanced Patterns](#advanced-patterns)
7. [Performance Optimization](#performance-optimization)
8. [Error Handling](#error-handling)

---

## Basic Examples

### Simple Database to Database

```python
from scriptman.powers.etl import ETL
from scriptman.powers.database import DatabaseHandler

# Setup connections
source_db = DatabaseHandler(server="source_server", database="source_db")
target_db = DatabaseHandler(server="target_server", database="target_db")

# Extract → Transform → Load
(
    ETL.from_db(source_db, "SELECT * FROM customers WHERE active = 1")
    .to_snake_case()
    .set_index('customer_id', inplace=True)
    .to_db(target_db, "active_customers", method="upsert")
)
```

### CSV to Database

```python
# Load CSV, clean data, and insert to database
(
    ETL.from_csv("data/sales.csv")
    .sanitize_names()
    .filter(lambda df: df['amount'] > 0)  # Remove invalid amounts
    .set_index('transaction_id', inplace=True)
    .to_db(db, "sales_transactions", method="upsert")
)
```

---

## Data Extraction

### Parameterized Database Queries

```python
# Extract with parameters
etl = ETL.from_db(
    db=db,
    query="""
        SELECT *
        FROM orders
        WHERE order_date >= :start_date
          AND order_date < :end_date
          AND status = :status
    """,
    params={
        "start_date": "2024-01-01",
        "end_date": "2024-02-01",
        "status": "completed"
    }
)
```

### Multiple CSV Files

```python
from pathlib import Path

# Find all CSV files
csv_files = ETL.search_files("data/monthly_reports/", "*.csv")

# Load and concatenate
monthly_data = ETL.from_csv(csv_files[0])
for file in csv_files[1:]:
    monthly_data = monthly_data.concat(ETL.from_csv(file))

# Process combined data
monthly_data.to_db(db, "all_monthly_reports", method="replace")
```

### From API with Custom Extractor

```python
import requests

def fetch_api_data(endpoint: str, api_key: str) -> list[dict]:
    """Custom extractor for API data"""
    response = requests.get(
        endpoint,
        headers={"Authorization": f"Bearer {api_key}"}
    )
    response.raise_for_status()
    return response.json()['data']

# Use custom extractor
etl = ETL.from_extractor(
    fetch_api_data,
    "https://api.example.com/customers",
    api_key="your_api_key"
)

etl.to_db(db, "api_customers", method="upsert")
```

---

## Data Transformation

### Column Name Transformations

```python
# CamelCase to snake_case
etl = (
    ETL.from_db(db, "SELECT CustomerID, FirstName, LastName FROM Customers")
    .to_snake_case()  # customer_id, first_name, last_name
)

# snake_case to camelCase
etl = (
    ETL.from_csv("data.csv")
    .to_camel_case()  # customerId, firstName, lastName
)

# SQL-safe sanitization
etl = (
    ETL.from_json("messy_data.json")
    .sanitize_names()  # Removes special characters, quotes, etc.
)
```

### Filtering and Conditional Logic

```python
# Simple filter
active_customers = etl.filter(etl['status'] == 'active')

# Multiple conditions
high_value_customers = etl.filter(
    (etl['total_purchases'] > 1000) &
    (etl['account_type'] == 'premium')
)

# Complex filtering
def is_valid_record(df):
    return (
        (df['email'].str.contains('@')) &
        (df['age'] >= 18) &
        (df['age'] <= 120) &
        (df['balance'] >= 0)
    )

clean_data = etl.filter(is_valid_record(etl.data))
```

### Custom Transformations

```python
def calculate_metrics(df):
    """Add calculated columns"""
    df['profit_margin'] = (df['revenue'] - df['cost']) / df['revenue']
    df['customer_lifetime_value'] = df['average_order'] * df['order_count']
    df['risk_score'] = df['late_payments'] / df['total_payments']
    return df

etl = etl.transform(calculate_metrics, context="Calculating business metrics")
```

### Merging Datasets

```python
# Load customers and orders
customers = ETL.from_db(db, "SELECT * FROM customers")
orders = ETL.from_db(db, "SELECT * FROM orders")

# Left join
customer_orders = customers.merge(
    orders,
    how="left",
    left_on="customer_id",
    right_on="customer_id",
    suffixes=("_customer", "_order")
)

# Inner join with multiple keys
result = etl1.merge(
    etl2,
    how="inner",
    left_on=["customer_id", "product_id"],
    right_on=["customer_id", "product_id"]
)
```

### Flattening Nested Data

```python
# Sample data with nested JSON
# {"id": 1, "user": {"name": "John", "email": "john@example.com"}, "status": "active"}

etl = (
    ETL.from_json("nested_data.json")
    .flatten('user', case="snake")  # Creates user_name, user_email columns
)

# Flatten all nested columns automatically
etl = ETL.from_json("complex_data.json").flatten_all_nested_columns()
```

### Working with Nested Lists

```python
# Data: {"customer_id": 1, "purchases": [{"item": "A", "price": 10}, {"item": "B", "price": 20}]}

# Extract nested list column
purchases = etl.pop_nested_column('purchases', drop=True)
# Result: Each purchase becomes a row with customer_id preserved

# Get all nested list columns
nested_data = etl.get_nested_list_columns(pop=True)
for column_name, column_etl in nested_data.items():
    column_etl.to_db(db, f"extracted_{column_name}")
```

---

## Data Loading

### Upsert Pattern (Recommended)

```python
# Upsert: Insert new records, update existing ones
etl = (
    ETL.from_csv("daily_data.csv")
    .set_index(['customer_id', 'date'], inplace=True)  # Composite key
    .to_db(
        db_handler=db,
        table_name="customer_daily_metrics",
        method="upsert",
        batch_size=5000
    )
)
```

### Truncate and Load

```python
# Clear table and reload (full refresh)
etl.to_db(
    db_handler=db,
    table_name="staging_table",
    method="truncate"  # Deletes all data, then inserts
)
```

### Insert Only (Append)

```python
# Append new records only
etl.to_db(
    db_handler=db,
    table_name="audit_log",
    method="insert"  # Fails if duplicate keys exist
)
```

### Update Existing Records

```python
# Update existing records only
etl = (
    ETL.from_csv("updated_prices.csv")
    .set_index('product_id', inplace=True)
    .to_db(
        db_handler=db,
        table_name="products",
        method="update"  # Only updates, doesn't insert new records
    )
)
```

### Replace Table

```python
# Drop and recreate table
etl.to_db(
    db_handler=db,
    table_name="lookup_table",
    method="replace"  # Drops table and recreates it
)
```

### Logical Keys (No Database Constraints)

```python
# Use logical keys without creating PRIMARY KEY constraint
# Useful when you can't modify database schema
etl = (
    ETL.from_csv("data.csv")
    .set_index(['key1', 'key2'], inplace=True)
    .to_db(
        db_handler=db,
        table_name="external_table",
        method="upsert",
        use_logical_keys=True  # No PRIMARY KEY constraint created
    )
)
```

---

## Complete ETL Pipelines

### Daily Sales Report Pipeline

```python
from datetime import datetime, timedelta
from scriptman.powers.etl import ETL
from scriptman.powers.database import DatabaseHandler

def generate_daily_sales_report(date: str):
    """Generate and load daily sales report"""

    # Connect to databases
    source_db = DatabaseHandler(server="prod_server", database="sales_db")
    warehouse_db = DatabaseHandler(server="warehouse_server", database="analytics_db")

    # Extract sales data
    sales = ETL.from_db(
        db=source_db,
        query="""
            SELECT
                s.sale_id,
                s.customer_id,
                s.product_id,
                s.quantity,
                s.unit_price,
                s.sale_date,
                c.customer_name,
                c.region,
                p.product_name,
                p.category
            FROM sales s
            JOIN customers c ON s.customer_id = c.customer_id
            JOIN products p ON s.product_id = p.product_id
            WHERE s.sale_date = :date
        """,
        params={"date": date}
    )

    # Transform
    report = sales.transform(
        lambda df: df.assign(
            total_amount=df['quantity'] * df['unit_price'],
            processed_date=datetime.now()
        ),
        context="Calculating totals"
    ).to_snake_case()

    # Load to warehouse
    report.set_index('sale_id', inplace=True).to_db(
        db_handler=warehouse_db,
        table_name="daily_sales_fact",
        method="upsert",
        batch_size=10000
    )

    print(f"✅ Loaded {len(report)} sales records for {date}")

# Run for yesterday
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
generate_daily_sales_report(yesterday)
```

### Customer 360 View

```python
def build_customer_360():
    """Build comprehensive customer view"""

    # Extract from multiple sources
    customers = ETL.from_db(db, "SELECT * FROM customers")
    orders = ETL.from_db(db, "SELECT * FROM orders")
    support_tickets = ETL.from_db(db, "SELECT * FROM support_tickets")

    # Aggregate orders
    order_summary = orders.transform(
        lambda df: df.groupby('customer_id').agg({
            'order_id': 'count',
            'total_amount': 'sum',
            'order_date': 'max'
        }).rename(columns={
            'order_id': 'total_orders',
            'total_amount': 'lifetime_value',
            'order_date': 'last_order_date'
        }).reset_index()
    )

    # Aggregate support tickets
    ticket_summary = support_tickets.transform(
        lambda df: df.groupby('customer_id').agg({
            'ticket_id': 'count',
            'priority': lambda x: (x == 'high').sum()
        }).rename(columns={
            'ticket_id': 'total_tickets',
            'priority': 'high_priority_tickets'
        }).reset_index()
    )

    # Merge everything
    customer_360 = (
        customers
        .merge(order_summary, how="left", on="customer_id")
        .merge(ticket_summary, how="left", on="customer_id")
        .transform(
            lambda df: df.fillna({
                'total_orders': 0,
                'lifetime_value': 0,
                'total_tickets': 0,
                'high_priority_tickets': 0
            })
        )
    )

    # Load
    customer_360.set_index('customer_id', inplace=True).to_db(
        db_handler=db,
        table_name="customer_360_view",
        method="replace"
    )

build_customer_360()
```

### Data Quality Pipeline

```python
def data_quality_check_and_load(source_file: str):
    """Extract, validate, clean, and load data"""

    # Extract
    raw_data = ETL.from_csv(source_file)

    # Quality checks
    def quality_checks(df):
        # Log issues
        null_emails = df['email'].isnull().sum()
        invalid_ages = ((df['age'] < 0) | (df['age'] > 120)).sum()

        print(f"Quality Report:")
        print(f"  - Null emails: {null_emails}")
        print(f"  - Invalid ages: {invalid_ages}")

        # Clean data
        df = df[df['email'].notnull()]  # Remove null emails
        df = df[(df['age'] >= 0) & (df['age'] <= 120)]  # Valid ages only
        df['email'] = df['email'].str.lower().str.strip()  # Normalize emails

        return df

    # Transform and clean
    clean_data = (
        raw_data
        .sanitize_names()
        .transform(quality_checks, context="Data quality checks")
        .set_index('customer_id', inplace=True)
    )

    # Load to production
    clean_data.to_db(
        db_handler=db,
        table_name="customers",
        method="upsert",
        synchronize_schema=True  # Auto-add new columns
    )

    # Archive raw data
    raw_data.to_db(
        db_handler=db,
        table_name="customers_raw_archive",
        method="insert"
    )

data_quality_check_and_load("customer_imports/latest.csv")
```

---

## Advanced Patterns

### Incremental Load with Watermark

```python
def incremental_load():
    """Load only new/updated records since last run"""

    # Get last watermark
    last_run = ETL.from_db(
        db,
        "SELECT MAX(last_modified) as watermark FROM etl_metadata WHERE table_name = 'orders'"
    )
    watermark = last_run['watermark'][0] if not last_run.empty else '1900-01-01'

    # Extract incremental data
    incremental = ETL.from_db(
        db=source_db,
        query="SELECT * FROM orders WHERE last_modified > :watermark",
        params={"watermark": watermark}
    )

    if not incremental.empty:
        # Load incremental data
        incremental.set_index('order_id', inplace=True).to_db(
            db_handler=warehouse_db,
            table_name="orders",
            method="upsert"
        )

        # Update watermark
        new_watermark = incremental['last_modified'].max()
        ETL.from_list([{
            "table_name": "orders",
            "last_modified": new_watermark,
            "records_processed": len(incremental)
        }]).to_db(
            db_handler=db,
            table_name="etl_metadata",
            method="upsert"
        )

incremental_load()
```

### Slowly Changing Dimension (SCD Type 2)

```python
def update_scd_type2(new_data_etl: ETL, table_name: str):
    """Implement SCD Type 2 pattern"""
    from datetime import datetime

    # Get current records
    current = ETL.from_db(
        db,
        f"SELECT * FROM {table_name} WHERE is_current = 1"
    )

    # Compare and find changes
    def detect_changes(df_new, df_current):
        # Mark records that changed
        changed = df_new.merge(
            df_current,
            on='business_key',
            how='left',
            suffixes=('_new', '_current')
        )
        changed['has_changed'] = changed.apply(
            lambda row: row['value_new'] != row['value_current'],
            axis=1
        )
        return changed[changed['has_changed']]

    changed_records = detect_changes(new_data_etl.data, current.data)

    if not changed_records.empty:
        # Close old records
        ETL.from_list([{
            'business_key': row['business_key'],
            'is_current': 0,
            'end_date': datetime.now()
        } for _, row in changed_records.iterrows()]).to_db(
            db,
            table_name,
            method="update"
        )

        # Insert new records
        new_records = changed_records.copy()
        new_records['is_current'] = 1
        new_records['start_date'] = datetime.now()
        new_records['end_date'] = None

        ETL(new_records).to_db(db, table_name, method="insert")
```

### Parallel Processing Multiple Tables

```python
from scriptman.powers.tasks import TaskManager
from concurrent.futures import ThreadPoolExecutor

def parallel_table_load(tables: list[tuple[ETL, str]]):
    """Load multiple tables in parallel"""

    def load_table(etl: ETL, table_name: str):
        try:
            etl.to_db(db, table_name, method="upsert")
            return f"✅ {table_name}"
        except Exception as e:
            return f"❌ {table_name}: {e}"

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(load_table, etl, table_name)
            for etl, table_name in tables
        ]
        results = [f.result() for f in futures]

    for result in results:
        print(result)

# Prepare data
tables_to_load = [
    (ETL.from_csv("customers.csv"), "customers"),
    (ETL.from_csv("products.csv"), "products"),
    (ETL.from_csv("orders.csv"), "orders"),
]

parallel_table_load(tables_to_load)
```

---

## Performance Optimization

### Batch Size Tuning

```python
import time

def find_optimal_batch_size(etl: ETL, table_name: str):
    """Test different batch sizes"""

    batch_sizes = [100, 500, 1000, 5000, 10000]
    results = {}

    for batch_size in batch_sizes:
        # Use truncate to reset for each test
        test_etl = etl.copy()  # Create copy

        start = time.time()
        test_etl.to_db(
            db_handler=db,
            table_name=f"{table_name}_test",
            method="truncate",
            batch_size=batch_size
        )
        duration = time.time() - start

        results[batch_size] = duration
        print(f"Batch size {batch_size}: {duration:.2f}s")

    optimal = min(results, key=results.get)
    print(f"\n✅ Optimal batch size: {optimal}")
    return optimal

optimal_size = find_optimal_batch_size(large_etl, "test_table")
```

### Memory-Efficient Processing

```python
def process_large_file_in_chunks(file_path: str, chunk_size: int = 10000):
    """Process large CSV in chunks"""
    import pandas as pd

    for chunk_df in pd.read_csv(file_path, chunksize=chunk_size):
        chunk_etl = (
            ETL.from_dataframe(chunk_df)
            .to_snake_case()
            .set_index('id', inplace=True)
        )

        chunk_etl.to_db(
            db_handler=db,
            table_name="large_table",
            method="upsert",
            batch_size=1000
        )

    print("✅ Completed processing large file")

process_large_file_in_chunks("very_large_file.csv")
```

---

## Error Handling

### Comprehensive Error Handling

```python
from scriptman.powers.database._exceptions import DatabaseError
from loguru import logger

def safe_etl_pipeline(source_file: str):
    """ETL pipeline with comprehensive error handling"""

    try:
        # Extract
        logger.info(f"Extracting data from {source_file}")
        etl = ETL.from_csv(source_file)

        if etl.empty:
            logger.warning(f"No data found in {source_file}")
            return False

        # Transform
        logger.info("Transforming data")
        etl = (
            etl.sanitize_names()
            .to_snake_case()
            .set_index('id', inplace=True)
        )

        # Load
        logger.info("Loading to database")
        etl.to_db(
            db_handler=db,
            table_name="target_table",
            method="upsert",
            allow_fallback=True  # Fallback to insert/update on error
        )

        logger.success(f"Successfully processed {len(etl)} records")
        return True

    except FileNotFoundError:
        logger.error(f"File not found: {source_file}")
        return False

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        # Could implement retry logic or alerting here
        return False

    except ValueError as e:
        logger.error(f"Data validation error: {e}")
        return False

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return False

success = safe_etl_pipeline("data.csv")
```

### Retry with Custom Logic

```python
from time import sleep

def load_with_retry(etl: ETL, max_retries: int = 3):
    """Load data with custom retry logic"""

    for attempt in range(max_retries):
        try:
            etl.to_db(db, "target_table", method="upsert")
            print(f"✅ Success on attempt {attempt + 1}")
            return True

        except DatabaseError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"❌ Attempt {attempt + 1} failed: {e}")
                print(f"⏳ Retrying in {wait_time} seconds...")
                sleep(wait_time)
            else:
                print(f"❌ Failed after {max_retries} attempts")
                raise

    return False
```

---

## See Also

- [ETL README](./ETL_README.md) - Overview and quick start
- [ETL API Reference](./ETL_API_Reference.md) - Complete API documentation
- [ETL Architecture](./ETL_Architecture.md) - Internal architecture
- [Main Documentation](../../README.md) - Back to documentation index

