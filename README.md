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
*   **Value Serializability:** Values can be any Python object that can be serialized into bytes. A default UTF-8 string serializer is provided, but you can plug in other serializers (e.g., Pickle, JSON, MsgPack).

## Core Concepts

The KVWC data model is based on a few key components:

*   **Row Key:** A unique string identifier for a row. This is the primary way to group related data.
*   **Column Name:** A string identifier for a specific piece of data within a row. A row can have many columns.
*   **Timestamp (ms):** A 64-bit integer representing milliseconds since the epoch. Each value stored for a (row key, column name) pair is associated with a timestamp. This enables versioning, with data typically sorted in reverse chronological order (newest first). If no timestamp is provided during a `put_row` operation, the current server time is used.
*   **Value:** The actual data, stored as bytes after serialization from a Python object. The library supports various serializers.
*   **Dataset (Optional):** An optional string name that maps directly to a RocksDB Column Family (CF). This provides strong isolation between datasets. Data within one dataset (CF) is separate from data in others, even if row keys are the same. If no dataset name is provided during operations, the `default` Column Family is used. Datasets (Column Families) must be specified during the `WideColumnDB` initialization.

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
from kvwc import WideColumnDB, PickleSerializer, JsonSerializer, MsgPackSerializer
import time
import os # For cleaning up test databases

# Define database path
DB_PATH = "./my_kvwc_database"

# Clean up previous test runs if necessary
if os.path.exists(DB_PATH):
    # This is a simplified cleanup, in a real app manage lifecycle carefully
    import shutil
    shutil.rmtree(DB_PATH)
    print(f"Cleaned up existing database at {DB_PATH}")


# Initialize the database
# You must specify the column_families (datasets) you plan to use.
# The 'default' CF is always available even if not listed.
db = WideColumnDB(
    DB_PATH,
    column_families=["users", "products", "configs", "logs", "sensors", "tenant_A", "tenant_B"],
    # Optionally specify RocksDB options (e.g., increase number of open files)
    # rocksdb_options={"max_open_files": 1000}
)

# Helper for timestamps in examples
current_ts_ms = int(time.time() * 1000)

print(f"Initialized database at {DB_PATH}")
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
db.put_row("config:xyz", [("settingA", "value2", current_ts_ms)], dataset_name="tenant_B") # Same key, different dataset in different CFs
```

### Getting Data (`get_row`)

The `get_row` method retrieves data. It returns a dictionary where keys are column names and values are lists of `(timestamp_ms, value)` tuples, sorted newest first.

```python
# Get all columns for a row (latest version of each) from the default dataset
user_data = db.get_row("user:123")
# Example: {'email': [(current_ts_ms, 'alice@example.com')]}
if "email" in user_data and user_data["email"]: # Check if data exists
    print(f"User email (default dataset): {user_data['email'][0][1]}")



# Get specific columns for a row (can be a list or a single string)
product_info = db.get_row("product:abc", column_names=["name", "price"])
# Example: {'name': [(<ts_for_name>, 'Super Widget')], 'price': [(current_ts_ms, '21.99')]}
if "price" in product_info and product_info["price"]: # Check if data exists
    print(f"Latest product price (default dataset): {product_info['price'][0][1]}")


# Get multiple versions of a column from the default dataset
price_history = db.get_row("product:abc", column_names="price", num_versions=2)

# Example: {'price': [(current_ts_ms, '21.99'), (current_ts_ms - 10000, '19.99')]}
if "price" in price_history:
    print("Price history for product:abc:")
    for ts, val in price_history["price"]:
        print(f"  - Price at {ts}: {val}")


# Get data from a specific dataset
config_tenant_A = db.get_row("config:xyz", dataset_name="tenant_A")
# Example: {'settingA': [(current_ts_ms, 'value1')]}
if "settingA" in config_tenant_A and config_tenant_A["settingA"]: # Check if data exists
    print(f"Setting A for tenant_A dataset: {config_tenant_A['settingA'][0][1]}")
```

### Time-Travel with `get_row`

The timestamping and versioning allow you to retrieve data as it existed at previous points in time or within specific time windows. The `get_row` method's `start_ts_ms`, `end_ts_ms`, and `num_versions` parameters enable this "time-travel" capability.

*   `num_versions`: Retrieve the N most recent versions. `num_versions=1` gets only the latest.
*   `start_ts_ms` / `end_ts_ms`: Retrieve versions whose timestamps fall within the specified range (inclusive). If both are used, versions must be within the range. If only `start_ts_ms` is used, it gets versions newer than or equal to that time. If only `end_ts_ms` is used, it gets versions older than or equal to that time.

When using `start_ts_ms` or `end_ts_ms`, `num_versions` acts as an *additional* limit on the number of results returned *within* that time window. If `num_versions` is larger than the actual number of versions in the range, all versions in the range are returned. To get *all* versions in a time range, set `num_versions` to a large value (e.g., 1000 or more).

Here's an example demonstrating time range retrieval:

```python
# Get data within a time range
# Let's add some data points for this example in the 'logs' dataset:
db.put_row("log:system", [("event", "start", current_ts_ms - 20000)], dataset_name="logs")
db.put_row("log:system", [("event", "process", current_ts_ms - 15000)], dataset_name="logs")
db.put_row("log:system", [("event", "checkpoint", current_ts_ms - 10000)], dataset_name="logs")
db.put_row("log:system", [("event", "stop", current_ts_ms - 5000)], dataset_name="logs")

events_in_range = db.get_row(
    "log:system",
    column_names="event",
    dataset_name="logs", # Specify the dataset
    start_ts_ms=current_ts_ms - 16000, # Includes 'process' (ts: current_ts_ms - 15000)
    end_ts_ms=current_ts_ms - 9000,   # Includes 'checkpoint' (ts: current_ts_ms - 10000)
    num_versions=10 # Get all versions within the range (assuming less than 10)
)
# Example output (newest first):
# {'event': [(<ts_for_checkpoint>, 'checkpoint'), (<ts_for_process>, 'process')]}
if "event" in events_in_range and events_in_range["event"]: # Check if data exists
    print("System log events in specified time range (logs dataset):")
    for ts, val in events_in_range["event"]:
        print(f"  - {val} at {ts}")

# Example: Get the value of 'event' column in 'log:system' row *at* the time just after 'process'
event_at_time = db.get_row(
    "log:system",
    column_names="event",
    dataset_name="logs",
    start_ts_ms=current_ts_ms - 15000,
    num_versions=1 # Get the single most recent version at or after this time
)
if "event" in event_at_time and event_at_time["event"]:
     # This will return the 'process' event because it's the newest at or after the start_ts_ms
     print(f"Event at or after {current_ts_ms - 15000}: {event_at_time['event'][0][1]} (ts: {event_at_time['event'][0][0]})")

# Example: Get the value of 'event' column in 'log:system' row *as it was* at the time just before 'checkpoint'
# By setting end_ts_ms and num_versions=1, we get the newest version whose timestamp is <= end_ts_ms
event_as_of_time = db.get_row(
    "log:system",
    column_names="event",
    dataset_name="logs",
    end_ts_ms=current_ts_ms - 10001, # Just before checkpoint timestamp
    num_versions=1
)
if "event" in event_as_of_time and event_as_of_time["event"]:
     # This will return the 'process' event because it's the newest version whose timestamp is <= 10001ms before now
     print(f"Event as of {current_ts_ms - 10001}: {event_as_of_time['event'][0][1]} (ts: {event_as_of_time['event'][0][0]})")

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
if "reading" in data and data["reading"]: # Check if data exists
    for ts, val in data["reading"]:
        print(f"  - {val} at {ts}")
```

### Closing the Database

```python
# It's good practice to close the database when done.
# This releases resources held by RocksDB.
db.close()

print(f"Closed database at {DB_PATH}")

# After closing, db.db will be None, and further operations on this instance will fail.
try:
    db.get_row("user:123")
except RuntimeError as e: # Expect RuntimeError now from DBManager access
    print(f"Error after close: {e}") # Example: Database is not initialized. Cannot get CF handle.
```

### Trying different Serializers

```python
# --- Example using a different Serializer ---
# If you need to store non-string data (like numbers, lists, dicts, custom objects),
# you can initialize WideColumnDB with a different serializer.

# Clean up the previous database
if os.path.exists(DB_PATH):
    import shutil
    shutil.rmtree(DB_PATH)
    print(f"Cleaned up existing database at {DB_PATH}")

# Initialize with PickleSerializer (can serialize most Python objects)
# Remember to specify all CFs you intend to use
db_pickle = WideColumnDB(
    DB_PATH,
    column_families=["data_pickle"],
    serializer=PickleSerializer()
)

# Store a dictionary
complex_data = {"value": 123, "status": True, "tags": ["a", "b"]}
db_pickle.put_row("complex:data", [("details", complex_data)], dataset_name="data_pickle")

# Retrieve and deserialize it
retrieved_data = db_pickle.get_row("complex:data", dataset_name="data_pickle")

if "details" in retrieved_data and retrieved_data["details"]:
    timestamp, value = retrieved_data["details"][0]
    print(f"Retrieved complex data (PickleSerializer): {value} (type: {type(value)})")
    # Output: Retrieved complex data (PickleSerializer): {'value': 123, 'status': True, 'tags': ['a', 'b']} (type: <class 'dict'>)

# Close the database instance
db_pickle.close()
print(f"Closed database at {DB_PATH}")


# --- Example using JsonSerializer ---
# Note: JsonSerializer only works with JSON-serializable types.

# Clean up the previous database
if os.path.exists(DB_PATH):
    import shutil
    shutil.rmtree(DB_PATH)
    print(f"Cleaned up existing database at {DB_PATH}")

# Initialize with JsonSerializer
db_json = WideColumnDB(
    DB_PATH,
    column_families=["data_json"],
    serializer=JsonSerializer()
)

# Store a list
json_serializable_data = [1, "two", {"three": 4}]
db_json.put_row("json:data", [("list_data", json_serializable_data)], dataset_name="data_json")

# Retrieve and deserialize it
retrieved_json_data = db_json.get_row("json:data", dataset_name="data_json")

if "list_data" in retrieved_json_data and retrieved_json_data["list_data"]:
    timestamp, value = retrieved_json_data["list_data"][0]
    print(f"Retrieved JSON data (JsonSerializer): {value} (type: {type(value)})")
    # Output: Retrieved JSON data (JsonSerializer): [1, 'two', {'three': 4}] (type: <class 'list'>)

# Close the database instance
db_json.close()
print(f"Closed database at {DB_PATH}")


# MsgPackSerializer is another efficient option, especially for binary data or performance-sensitive cases.
```

## Internal Key Structure

KVWC leverages RocksDB's Column Family feature to handle datasets. The internal keys constructed by the configured `key_codec` within a specific Column Family (dataset) are structured to enable efficient prefix scans and ordered retrieval by timestamp *within that CF*.

The general format of the key **within a Column Family** is:

`row_key_bytes <SEP> column_name_bytes <SEP> inverted_timestamp_bytes`

Or, for the `LengthPrefixedKeyCodec`:

`[len_row][row_key][len_column][column_name][inverted_timestamp_ms]`

Where:
*   `<SEP>` is a null byte (`\x00`) used by the default `KeyCodec`.
*   `row_key_bytes`, `column_name_bytes` are the byte representations of the row and column names (e.g., UTF-8 encoded for the default codec, or length-prefixed bytes for `LengthPrefixedKeyCodec`).
*   `inverted_timestamp_bytes` is a big-endian 8-byte representation of `(2^64 - 1) - timestamp_ms`. Inverting the timestamp allows RocksDB's default lexicographical sorting (byte-wise) to naturally order keys from newest to oldest timestamp.

**Note:** The dataset name is implicitly handled by using the correct RocksDB Column Family handle for each operation; it is *not* part of the key byte string stored within that CF.

This structure is an internal detail, but understanding it can be helpful for advanced use cases or debugging. The choice of `key_codec` (e.g., `KeyCodec` or `LengthPrefixedKeyCodec`) affects the exact byte format.

This structure is an internal detail, but understanding it can be helpful for advanced use cases or debugging.

## Running Tests

The project includes a suite of unit tests under the `tests/` directory. To run the tests:

1.  Ensure you have installed the `kvwc` package and its dependencies (as described in the Installation section).
2.  Navigate to the root directory of the project.
3.  Activate the python virtual enviroment
    ```bash
    source .venv/bin/activate
    ```
4.  Run the tests using Python's `unittest` module:

    ```bash
    python -m unittest discover tests
    ```
    or running a specific test file
    ```bash
    python -m unittest tests.test_wide_column_db
    ```

    The tests create and remove temporary database files in a `test_db_temp_wide_column_main` directory within the current working directory where the test script is run.
