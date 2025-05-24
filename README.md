# Key-Value Wide Column (KVWC) Database

[![Build Status](https://github.com/dzlab/kvwc/actions/workflows/ci.yml/badge.svg)](https://github.com/dzlab/kvwc/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/kvwc)](https://pypi.org/project/kvwc/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/kvwc)](https://pypi.org/project/kvwc/)
[![License](https://img.shields.io/github/license/dzlab/kvwc)](https://github.com/dzlab/kvwc/blob/main/LICENSE)

KVWC is a Python library that implements a key-value wide-column data store, leveraging RocksDB as its underlying storage engine. It allows you to store and retrieve data organized by row keys, column names, and timestamps, providing versioning capabilities for your data. Learn more in this article - [link](https://dzlab.github.io/database/2025/05/11/wide-column-database/).

## Features

*   **Flexible Data Model:** Store data as rows, each containing multiple columns.
*   **Versioning:** Each cell (row key, column name) can store multiple values versioned by timestamps.
*   **Timestamp-Based Retrieval:** Fetch the latest version, a specific number of versions, or versions within a time range.
*   **Datasets:** Optional namespacing for rows, allowing logical separation of data (e.g., per-tenant or per-table).
*   **CRUD Operations:**
    *   `put_row`: Add or update data for one or more columns in a row.
    *   `get_row`: Retrieve data for a row, with options to filter by columns, number of versions, and time range.
    *   `delete_row`: Delete entire rows, specific columns within a row, or specific timestamped versions of a column.
*   **Persistent Storage:** Uses RocksDB for durable, on-disk storage.
*   **UTF-8 Encoding:** Keys and values are stored as UTF-8 encoded strings.

## Core Concepts

The KVWC data model is based on a few key components:

*   **Row Key:** A unique string identifier for a row. This is the primary way to group related data.
*   **Column Name:** A string identifier for a specific piece of data within a row. A row can have many columns.
*   **Timestamp (ms):** A 64-bit integer representing milliseconds since the epoch. Each value stored for a (row key, column name) pair is associated with a timestamp. This enables versioning, with data typically sorted in reverse chronological order (newest first). If no timestamp is provided during a `put_row` operation, the current server time is used.
*   **Value:** The actual data, stored as a string.
*   **Dataset (Optional):** An optional string name that acts as a namespace. If a dataset is specified, the row key is unique within that dataset. This allows, for example, different datasets to have rows with the same `row_key` without collision.

## Installation

### Prerequisites

*   Python 3.8 or higher.
*   RocksDB library installed on your system. The `python-rocksdb` package (a dependency of KVWC) will attempt to build it if not found during its own installation, but system-level installation of RocksDB is often smoother.
    *   On Debian/Ubuntu: `sudo apt-get install librocksdb-dev`
    *   On macOS: `brew install rocksdb`
    *   For other systems, or for more detailed instructions, please refer to the `python-rocksdb` documentation.

### From Source or as a Package

The project uses `pyproject.toml` for its build and dependency management.

1.  **Clone the repository (if you're working from a git clone):**
    ```bash
    git clone git@github.com:dzlab/kvwc.git
    cd kvwc
    ```

2.  **Install:**
    You can install the package and its dependencies (like `rocksdb`) using pip from the directory containing `pyproject.toml` (e.g., `kvwc`):
    ```bash
    pip install .
    ```
    This will install the `kvwc` package, making it importable in your Python projects (e.g., `from kvwc import WideColumnDB`).

## API Usage

First, import the `WideColumnDB` class:

```python
from kvwc import WideColumnDB
import time

# Initialize the database (creates files at the specified path if they don't exist)
db = WideColumnDB("./my_kvwc_database")

# Helper for timestamps in examples
current_ts_ms = int(time.time() * 1000)
```

### Putting Data (`put_row`)

The `put_row` method is used to insert or update data. It accepts a row key and a list of items. Each item is a tuple of `(column_name, value, optional_timestamp_ms)`.

```python
# Put a single column value with a specific timestamp
db.put_row("user:123", [("email", "alice@example.com", current_ts_ms)])

# Put multiple column values for the same row
# If timestamp is omitted for an item, current time is used for that item
db.put_row("product:abc", [
    ("name", "Super Widget"), # Timestamp will be current time
    ("price", "19.99", current_ts_ms - 10000), # Older price
    ("price", "21.99", current_ts_ms)         # Current price
])

# Using a dataset
db.put_row("config:xyz", [("settingA", "value1", current_ts_ms)], dataset_name="tenant_A")
db.put_row("config:xyz", [("settingA", "value2", current_ts_ms)], dataset_name="tenant_B") # Same key, different dataset
```

### Getting Data (`get_row`)

The `get_row` method retrieves data. It returns a dictionary where keys are column names and values are lists of `(timestamp_ms, value)` tuples, sorted newest first.

```python
# Get all columns for a row (latest version of each)
user_data = db.get_row("user:123")
# Example: {'email': [(current_ts_ms, 'alice@example.com')]}
if "email" in user_data:
    print(f"User email: {user_data['email'][0][1]}")


# Get specific columns for a row (can be a list or a single string)
product_info = db.get_row("product:abc", column_names=["name", "price"])
# Example: {'name': [(<ts_for_name>, 'Super Widget')], 'price': [(current_ts_ms, '21.99')]}
if "price" in product_info:
    print(f"Latest product price: {product_info['price'][0][1]}")


# Get multiple versions of a column
price_history = db.get_row("product:abc", column_names="price", num_versions=2)
# Example: {'price': [(current_ts_ms, '21.99'), (current_ts_ms - 10000, '19.99')]}
if "price" in price_history:
    print("Price history for product:abc:")
    for ts, val in price_history["price"]:
        print(f"  - Price at {ts}: {val}")


# Get data from a specific dataset
config_tenant_A = db.get_row("config:xyz", dataset_name="tenant_A")
# Example: {'settingA': [(current_ts_ms, 'value1')]}
if "settingA" in config_tenant_A:
    print(f"Setting A for tenant_A: {config_tenant_A['settingA'][0][1]}")


# Get data within a time range
# Let's add some data points for this example:
db.put_row("log:system", [("event", "start", current_ts_ms - 20000)])
db.put_row("log:system", [("event", "process", current_ts_ms - 15000)])
db.put_row("log:system", [("event", "checkpoint", current_ts_ms - 10000)])
db.put_row("log:system", [("event", "stop", current_ts_ms - 5000)])

events_in_range = db.get_row(
    "log:system",
    column_names="event",
    start_ts_ms=current_ts_ms - 16000, # Includes 'process' (ts: current_ts_ms - 15000)
    end_ts_ms=current_ts_ms - 9000,   # Includes 'checkpoint' (ts: current_ts_ms - 10000)
    num_versions=10 # Get all versions within the range
)
# Example output (newest first):
# {'event': [(<ts_for_checkpoint>, 'checkpoint'), (<ts_for_process>, 'process')]}
if "event" in events_in_range:
    print("System log events in specified time range:")
    for ts, val in events_in_range["event"]:
        print(f"  - {val} at {ts}")
```

### Deleting Data (`delete_row`)

```python
# Delete specific columns from a row (all versions of these columns)
db.delete_row("product:abc", column_names=["description"]) # Assuming 'description' existed

# Delete an entire row (all columns, all versions)
db.delete_row("user:temp_user_to_delete")

# Delete from a specific dataset
db.delete_row("config:xyz", dataset_name="tenant_B") # Deletes row "config:xyz" only from "tenant_B"

# Delete specific timestamped versions of a column
# First, put some versions
ts1, ts2, ts3 = current_ts_ms - 200, current_ts_ms - 100, current_ts_ms
db.put_row("sensor:t1", [("reading", "20C", ts1)])
db.put_row("sensor:t1", [("reading", "21C", ts2)])
db.put_row("sensor:t1", [("reading", "22C", ts3)])

# Delete the middle version (ts2)
db.delete_row("sensor:t1", column_names="reading", specific_timestamps_ms=[ts2])

data = db.get_row("sensor:t1", column_names="reading", num_versions=3)
# Example: {'reading': [(ts3, '22C'), (ts1, '20C')]}
print("Sensor t1 readings after deleting middle version:")
if "reading" in data:
    for ts, val in data["reading"]:
        print(f"  - {val} at {ts}")
```

### Closing the Database

```python
# It's good practice to close the database when done.
# This releases resources held by RocksDB.
db.close()

# After closing, db.db will be None, and further operations on this instance will fail.
# try:
#     db.get_row("user:123")
# except AttributeError as e:
#     print(f"Error after close: {e}") # Example: 'NoneType' object has no attribute 'iterkeys'
```

## Internal Key Structure

KVWC constructs internal keys for RocksDB in a way that enables efficient prefix scans and ordered retrieval by timestamp. The general format is:

`[dataset_name_bytes <SEP>] row_key_bytes <SEP> column_name_bytes <SEP> inverted_timestamp_bytes`

Where:
*   `<SEP>` is a null byte (`\x00`).
*   `dataset_name_bytes` is present only if a dataset is used for the operation.
*   `row_key_bytes`, `column_name_bytes` are UTF-8 encoded strings.
*   `inverted_timestamp_bytes` is a big-endian 8-byte representation of `(2^64 - 1) - timestamp_ms`. Inverting the timestamp allows RocksDB's default lexicographical sorting (byte-wise) to naturally order keys from newest to oldest timestamp.

This structure is an internal detail, but understanding it can be helpful for advanced use cases or debugging.

## Running Tests

The project includes a suite of unit tests in `kvwc/tests/test_wide_column_db.py`. To run the tests:

1.  Ensure you have installed the `kvwc` package and its dependencies (as described in the Installation section).
2.  Navigate to the root directory of the project.
3.  Run the tests using Python's `unittest` module:

    ```bash
    python -m unittest kvwc.tests.test_wide_column_db
    ```
    Alternatively, if your current working directory is the project root:
    ```bash
    python kvwc/tests/test_wide_column_db.py
    ```
    The tests create and remove temporary database files in a `test_db_temp_wide_column_main` directory in the location where the test script is run.
