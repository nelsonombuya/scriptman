# ETL API Reference

Complete API reference for the Scriptman ETL module.

---

## Table of Contents

1. [ETL Class](#etl-class)
2. [ETLDatabase Class](#etldatabase-class)
3. [Extraction Methods](#extraction-methods)
4. [Transformation Methods](#transformation-methods)
5. [Loading Methods](#loading-methods)
6. [Utility Methods](#utility-methods)

---

## ETL Class

### Constructor

```python
ETL(data: Optional[ETL_TYPES] = None)
```

Initialize an ETL object with optional data.

**Parameters:**
- `data` (Optional): Initial data as DataFrame, list of dicts, or list of tuples

**Returns:** ETL instance

**Example:**
```python
# From list of dictionaries
etl = ETL([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])

# Empty ETL object
etl = ETL()

# From DataFrame
import pandas as pd
etl = ETL(pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}))
```

---

## Extraction Methods

### `from_db()`

```python
@classmethod
ETL.from_db(
    db: DatabaseHandler,
    query: str,
    params: dict[str, Any] = {}
) -> ETL
```

Extract data from a database using a SQL query.

**Parameters:**
- `db` (DatabaseHandler): Database handler instance
- `query` (str): SQL query to execute
- `params` (dict): Query parameters for parameterized queries

**Returns:** ETL instance with query results

**Example:**
```python
etl = ETL.from_db(
    db=db_handler,
    query="SELECT * FROM customers WHERE region = :region",
    params={"region": "North"}
)
```

**Features:**
- ✅ Automatic table-level queueing
- ✅ Automatic retry on transient failures
- ✅ Thread-safe

---

### `from_csv()`

```python
@classmethod
ETL.from_csv(file_path: str | Path) -> ETL
```

Extract data from a CSV file.

**Parameters:**
- `file_path` (str | Path): Path to CSV file

**Returns:** ETL instance with CSV data

**Raises:**
- `FileNotFoundError`: If file doesn't exist

**Example:**
```python
etl = ETL.from_csv("data/customers.csv")
etl = ETL.from_csv(Path("data/customers.csv"))
```

---

### `from_json()`

```python
@classmethod
ETL.from_json(file_path: str | Path) -> ETL
```

Extract data from a JSON file.

**Parameters:**
- `file_path` (str | Path): Path to JSON file

**Returns:** ETL instance with JSON data

**Raises:**
- `FileNotFoundError`: If file doesn't exist

**Example:**
```python
etl = ETL.from_json("data/customers.json")
```

---

### `from_dataframe()`

```python
@classmethod
ETL.from_dataframe(data: DataFrame | list[DataFrame]) -> ETL
```

Create ETL object from pandas DataFrame(s).

**Parameters:**
- `data` (DataFrame | list[DataFrame]): Single or multiple DataFrames to concatenate

**Returns:** ETL instance

**Example:**
```python
import pandas as pd

df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
etl = ETL.from_dataframe(df)

# Multiple DataFrames
etl = ETL.from_dataframe([df1, df2, df3])
```

---

### `from_list()`

```python
@classmethod
ETL.from_list(data: list[dict[str, Any]]) -> ETL
```

Create ETL object from a list of dictionaries.

**Parameters:**
- `data` (list[dict]): List of dictionaries

**Returns:** ETL instance

**Example:**
```python
data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
]
etl = ETL.from_list(data)
```

---

### `from_extractor()`

```python
@classmethod
ETL.from_extractor(
    extractor: Callable[P, ETL_TYPES],
    *args: Any,
    **kwargs: Any
) -> ETL
```

Extract data using a custom extractor function.

**Parameters:**
- `extractor` (Callable): Function that returns DataFrame, list of dicts, or tuples
- `*args`: Arguments to pass to extractor
- `**kwargs`: Keyword arguments to pass to extractor

**Returns:** ETL instance

**Example:**
```python
def custom_extractor(api_url: str, api_key: str) -> list[dict]:
    response = requests.get(api_url, headers={"Authorization": api_key})
    return response.json()

etl = ETL.from_extractor(
    custom_extractor,
    "https://api.example.com/data",
    api_key="secret"
)
```

---

## Transformation Methods

### `transform()`

```python
ETL.transform(
    transformer: Callable[[DataFrame], ETL_TYPES],
    context: str = "Transformation Code Block"
) -> ETL
```

Apply a custom transformation function to the data.

**Parameters:**
- `transformer` (Callable): Function that takes a DataFrame and returns transformed data
- `context` (str): Description for logging

**Returns:** New ETL instance with transformed data

**Example:**
```python
def add_calculated_field(df: pd.DataFrame) -> pd.DataFrame:
    df['total_with_tax'] = df['total'] * 1.15
    return df

etl = etl.transform(add_calculated_field, context="Adding tax calculation")
```

---

### `merge()`

```python
ETL.merge(
    right: ETL | DataFrame,
    how: Literal["inner", "left", "right", "outer", "cross"] = "inner",
    on: Optional[str | list[str]] = None,
    left_on: Optional[str | list[str]] = None,
    right_on: Optional[str | list[str]] = None,
    left_index: bool = False,
    right_index: bool = False,
    suffixes: tuple[str, str] = ("_x", "_y"),
    **kwargs: Any
) -> ETL
```

Merge with another ETL object or DataFrame.

**Parameters:**
- `right` (ETL | DataFrame): Right dataset to merge with
- `how` (str): Type of merge ("inner", "left", "right", "outer", "cross")
- `on` (str | list): Column(s) to join on
- `left_on` (str | list): Left dataset join column(s)
- `right_on` (str | list): Right dataset join column(s)
- `left_index` (bool): Use left index as join key
- `right_index` (bool): Use right index as join key
- `suffixes` (tuple): Suffixes for overlapping columns

**Returns:** New ETL instance with merged data

**Example:**
```python
customers = ETL.from_db(db, "SELECT * FROM customers")
orders = ETL.from_db(db, "SELECT * FROM orders")

result = customers.merge(
    orders,
    how="left",
    left_on="customer_id",
    right_on="customer_id"
)
```

---

### `concat()`

```python
ETL.concat(
    other: ETL | DataFrame | list[ETL | DataFrame],
    **kwargs: Any
) -> ETL
```

Concatenate with other ETL objects or DataFrames.

**Parameters:**
- `other` (ETL | DataFrame | list): Dataset(s) to concatenate
- `**kwargs`: Additional arguments for pandas.concat (axis, ignore_index, etc.)

**Returns:** New ETL instance with concatenated data

**Example:**
```python
jan_data = ETL.from_csv("january.csv")
feb_data = ETL.from_csv("february.csv")
mar_data = ETL.from_csv("march.csv")

q1_data = jan_data.concat([feb_data, mar_data], ignore_index=True)
```

---

### `filter()`

```python
ETL.filter(
    condition: Any,
    context: str = "Filtering Code Block"
) -> ETL
```

Filter rows based on a condition.

**Parameters:**
- `condition`: Boolean condition to filter rows
- `context` (str): Description for logging

**Returns:** New ETL instance with filtered data

**Example:**
```python
# Filter by column value
etl = etl.filter(etl['age'] > 18)

# Multiple conditions
etl = etl.filter((etl['age'] > 18) & (etl['status'] == 'active'))
```

---

### `set_index()`

```python
ETL.set_index(*args: Any, **kwargs: Any) -> ETL
```

Set the DataFrame index.

**Parameters:**
- `*args`, `**kwargs`: Arguments passed to pandas DataFrame.set_index()

**Returns:** ETL instance (self if inplace=True, new instance otherwise)

**Example:**
```python
# Set single column as index
etl = etl.set_index('customer_id', inplace=True)

# Set multiple columns as index
etl = etl.set_index(['customer_id', 'order_date'], inplace=True)
```

---

### `to_snake_case()`

```python
ETL.to_snake_case() -> ETL
```

Convert all column and index names to snake_case.

**Returns:** New ETL instance with snake_case names

**Example:**
```python
# Before: "FirstName", "LastName", "EmailAddress"
etl = etl.to_snake_case()
# After: "first_name", "last_name", "email_address"
```

---

### `to_camel_case()`

```python
ETL.to_camel_case() -> ETL
```

Convert all column and index names to camelCase.

**Returns:** New ETL instance with camelCase names

**Example:**
```python
# Before: "first_name", "last_name", "email_address"
etl = etl.to_camel_case()
# After: "firstName", "lastName", "emailAddress"
```

---

### `sanitize_names()`

```python
ETL.sanitize_names() -> ETL
```

Sanitize column and index names to be SQL-safe.

**Returns:** New ETL instance with sanitized names

**Features:**
- Removes quotes, semicolons, backslashes
- Replaces special characters with underscores
- Ensures names don't start with numbers
- Limits length to 120 characters

**Example:**
```python
# Before: "User's Name!", "Email; Address", "123_column"
etl = etl.sanitize_names()
# After: "user_s_name", "email_address", "col_123_column"
```

---

### `flatten()`

```python
ETL.flatten(
    column: str,
    case: Literal["snake", "camel"] = "snake"
) -> ETL
```

Flatten a nested dictionary column into separate columns.

**Parameters:**
- `column` (str): Column containing nested dictionaries
- `case` (str): Case style for new column names ("snake" or "camel")

**Returns:** New ETL instance with flattened columns

**Example:**
```python
# Before: metadata column = {"user": {"id": 123, "name": "John"}, "status": "active"}
etl = etl.flatten('metadata', case="snake")
# After: metadata_user_id, metadata_user_name, metadata_status columns
```

---

### `flatten_all_nested_columns()`

```python
ETL.flatten_all_nested_columns(
    case: Literal["snake", "camel"] = "snake"
) -> ETL
```

Flatten all columns containing nested dictionaries.

**Parameters:**
- `case` (str): Case style for new column names

**Returns:** New ETL instance with all nested columns flattened

**Example:**
```python
etl = etl.flatten_all_nested_columns(case="snake")
```

---

### `pop_nested_column()`

```python
ETL.pop_nested_column(
    column: str,
    drop: bool = True
) -> ETL
```

Extract a nested list column into a new ETL object.

**Parameters:**
- `column` (str): Column containing nested lists
- `drop` (bool): Whether to drop the column from original DataFrame

**Returns:** New ETL instance with expanded nested data

**Example:**
```python
# Before: skills column = ["Python", "SQL", "JavaScript"]
skills_etl = etl.pop_nested_column('skills')
# Result: Each skill becomes a row with original index preserved
```

---

### `get_nested_list_columns()`

```python
ETL.get_nested_list_columns(pop: bool = False) -> dict[str, ETL]
```

Get all columns containing lists of dictionaries as separate ETL objects.

**Parameters:**
- `pop` (bool): Whether to remove columns from original DataFrame

**Returns:** Dictionary mapping column names to ETL instances

**Example:**
```python
nested_data = etl.get_nested_list_columns(pop=True)
# Returns: {"column1": ETL(...), "column2": ETL(...)}
```

---

## Loading Methods

### `to_db()`

```python
ETL.to_db(
    db_handler: DatabaseHandler,
    table_name: str,
    batch_size: int = 1000,
    batch_execute: bool = True,
    force_nvarchar: bool = False,
    allow_fallback: bool = False,
    use_logical_keys: bool = False,
    synchronize_schema: bool = True,
    sanitize_column_names: bool = True,
    method: Literal["truncate", "replace", "insert", "update", "upsert"] = "upsert"
) -> bool
```

Load data to a database table.

**Parameters:**
- `db_handler` (DatabaseHandler): Database handler instance
- `table_name` (str): Target table name
- `batch_size` (int): Rows per batch (default: 1000)
- `batch_execute` (bool): Use batch operations (default: True)
- `force_nvarchar` (bool): Force all columns to NVARCHAR(MAX) (default: False)
- `allow_fallback` (bool): Fallback to insert/update on error (default: False)
- `use_logical_keys` (bool): Use index as logical keys without DB constraints (default: False)
- `synchronize_schema` (bool): Auto-add missing columns (default: True)
- `sanitize_column_names` (bool): Auto-sanitize column names (default: True)
- `method` (str): Loading method - see below

**Methods:**
- `"truncate"`: Delete existing data, then insert
- `"replace"`: Drop and recreate table, then insert
- `"insert"`: Insert new rows only (fails on duplicates)
- `"update"`: Update existing rows (requires index)
- `"upsert"`: Insert new or update existing rows (recommended)

**Returns:** True if successful

**Raises:**
- `ValueError`: If dataset is empty or index not set for update/upsert
- `DatabaseError`: If database operation fails

**Example:**
```python
# Upsert with automatic schema sync
etl = ETL(data).set_index('customer_id', inplace=True)
etl.to_db(
    db_handler=db,
    table_name="customers",
    method="upsert",
    batch_size=5000
)

# Truncate and reload
etl.to_db(db, "staging_table", method="truncate")

# Update only existing rows
etl.to_db(db, "customers", method="update")
```

**Features:**
- ✅ Automatic table-level queueing (prevents deadlocks)
- ✅ Automatic retry on transient failures
- ✅ Automatic schema synchronization
- ✅ Batch operations for performance
- ✅ Thread-safe

---

### `to_csv()`

```python
ETL.to_csv(file_path: str | Path) -> Path
```

Save data to a CSV file.

**Parameters:**
- `file_path` (str | Path): Output file path

**Returns:** Path to saved file

**Raises:**
- `ValueError`: If dataset is empty

**Example:**
```python
output_path = etl.to_csv("output/customers.csv")
```

---

### `to_json()`

```python
ETL.to_json(
    file_path: str | Path,
    indent: int = 2
) -> Path
```

Save data to a JSON file.

**Parameters:**
- `file_path` (str | Path): Output file path
- `indent` (int): JSON indentation (default: 2)

**Returns:** Path to saved file

**Raises:**
- `ValueError`: If dataset is empty

**Example:**
```python
output_path = etl.to_json("output/customers.json", indent=4)
```

---

### `to_dataframe()`

```python
ETL.to_dataframe() -> DataFrame
```

Get the underlying pandas DataFrame.

**Returns:** pandas DataFrame

**Example:**
```python
df = etl.to_dataframe()
```

---

### `to_list()`

```python
ETL.to_list() -> list[dict[str, Any]]
```

Convert data to a list of dictionaries.

**Returns:** List of dictionaries

**Example:**
```python
data = etl.to_list()
# [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
```

---

## Utility Methods

### `timed_context()`

```python
@classmethod
@contextmanager
ETL.timed_context(
    context: str = "Code Block",
    operation: Optional[Literal["extraction", "transformation", "loading"]] = None
) -> Generator[None, None, None]
```

Context manager for timing ETL operations.

**Parameters:**
- `context` (str): Name of the operation
- `operation` (str): Type of operation (extraction, transformation, loading)

**Example:**
```python
with ETL.timed_context("Custom Processing", "transformation"):
    # Your code here
    pass
```

---

### `search_files()`

```python
@classmethod
ETL.search_files(
    file_path: str | Path,
    pattern: str = "*"
) -> list[Path]
```

Search for files matching a pattern.

**Parameters:**
- `file_path` (str | Path): Directory to search
- `pattern` (str): Glob pattern (default: "*")

**Returns:** List of matching file paths

**Example:**
```python
csv_files = ETL.search_files("data/", "*.csv")
```

---

### `search_downloads()`

```python
@classmethod
ETL.search_downloads(pattern: str = "*") -> list[Path]
```

Search for files in the configured downloads directory.

**Parameters:**
- `pattern` (str): Glob pattern

**Returns:** List of matching file paths

**Example:**
```python
downloads = ETL.search_downloads("*.xlsx")
```

---

## ETLDatabase Class

Low-level database wrapper with queue management. Usually accessed through ETL.to_db().

### Constructor

```python
ETLDatabase(
    database_handler: DatabaseHandler,
    auto_upgrade_to_etl: bool = True
)
```

**Parameters:**
- `database_handler` (DatabaseHandler): Database handler instance
- `auto_upgrade_to_etl` (bool): Auto-upgrade connection pool for ETL workloads

---

### `execute_read_query()`

```python
ETLDatabase.execute_read_query(
    query: str,
    params: dict[str, Any] = {}
) -> list[dict[str, Any]]
```

Execute a read query with automatic retry and queueing.

**Parameters:**
- `query` (str): SQL query
- `params` (dict): Query parameters

**Returns:** List of dictionaries (query results)

---

### `execute_write_query()`

```python
ETLDatabase.execute_write_query(
    query: str,
    params: dict[str, Any] = {},
    check_affected_rows: bool = False
) -> bool
```

Execute a write query with automatic retry and queueing.

**Parameters:**
- `query` (str): SQL query
- `params` (dict): Query parameters
- `check_affected_rows` (bool): Verify rows were affected

**Returns:** True if successful

---

### `execute_write_batch_query()`

```python
ETLDatabase.execute_write_batch_query(
    query: str,
    rows: Iterator[dict[str, Any]] | list[dict[str, Any]] = [],
    batch_size: int = 1000
) -> bool
```

Execute a batch write query.

**Parameters:**
- `query` (str): SQL query
- `rows` (Iterator | list): Rows to insert
- `batch_size` (int): Batch size

**Returns:** True if successful

---

### `synchronize_table_schema()`

```python
ETLDatabase.synchronize_table_schema(
    table_name: str,
    df: DataFrame,
    force_nvarchar: bool = False
) -> bool
```

Synchronize table schema with DataFrame structure.

**Parameters:**
- `table_name` (str): Table name
- `df` (DataFrame): DataFrame with target schema
- `force_nvarchar` (bool): Force NVARCHAR for all columns

**Returns:** True if successful

**Features:**
- Creates table if it doesn't exist
- Adds missing columns to existing table
- Updates column types if needed

---

## Properties

### `ETL.data`

```python
@property
ETL.data -> DataFrame
```

Access the underlying pandas DataFrame.

**Example:**
```python
df = etl.data
```

---

### `ETL.columns`

```python
ETL.columns -> Index
```

Get DataFrame columns.

**Example:**
```python
columns = etl.columns
```

---

### `ETL.empty`

```python
ETL.empty -> bool
```

Check if DataFrame is empty.

**Example:**
```python
if not etl.empty:
    etl.to_db(db, "table")
```

---

### `ETL.index`

```python
ETL.index -> Index
```

Get DataFrame index.

**Example:**
```python
index = etl.index
```

---

## Type Hints

```python
from typing import TypeAlias
from pandas import DataFrame

ETL_TYPES: TypeAlias = DataFrame | list[dict[str, Any]] | list[tuple[Any, ...]]
```

---

## See Also

- [ETL README](./ETL_README.md) - Overview and quick start
- [ETL Examples](./ETL_Examples.md) - Usage examples
- [ETL Architecture](./ETL_Architecture.md) - Internal architecture
- [Main Documentation](../../README.md) - Back to documentation index

