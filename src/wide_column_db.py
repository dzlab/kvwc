import logging
import rocksdict as rocksdb
import time
from .key_codec import KeyCodec

# Set the logging level
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# Create a formatter and attach it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
# Add the handlers to the logger
logger.addHandler(console_handler)

class WideColumnDB:
    def __init__(self, db_path):
        opts = rocksdb.Options()
        opts.create_if_missing(True)
        # For more advanced usage, you might explore Column Families here
        # to represent different datasets/tables within the same DB.
        self.db = rocksdb.Rdict(db_path, opts)

    def _current_timestamp_ms(self):
        return int(time.time() * 1000)


    # --- Public API Methods ---

    def put_row(self, row_key, items, dataset_name=None):
        """
        Puts items into a row.
        items: list of tuples (column_name, value, optional_timestamp_ms)
               If timestamp_ms is None, current server time is used.
        """
        batch = rocksdb.WriteBatch()
        for item in items:
            column_name, value = item[0], item[1]
            timestamp_ms = item[2] if len(item) > 2 and item[2] is not None else self._current_timestamp_ms()

            rdb_key = KeyCodec.encode(dataset_name=dataset_name, row_key=row_key, column_name=column_name, timestamp_ms=timestamp_ms)
            rdb_value = str(value).encode('utf-8') # Assuming value can be stringified
            batch.put(rdb_key, rdb_value)
        self.db.write(batch)

    def get_row(self, row_key, column_names=None, num_versions=1, dataset_name=None, start_ts_ms=None, end_ts_ms=None):
        """
        Gets data for a row.
        Returns a dictionary: {column_name: [(timestamp_ms, value), ...]}
        """
        results = {}

        # Determine if we expect a dataset name in the key structure
        # This is a simplification; in a real system, you'd know based on how datasets are managed.
        # For this example, we assume if dataset_name is provided, keys were stored with it.
        has_dataset_name_in_key = dataset_name is not None

        # Build the prefix for scanning
        # If specific column_names are given, we iterate and build prefixes for each.
        # If not, we build a prefix for the entire row.

        target_columns = []
        if column_names:
            if isinstance(column_names, str): # Single column name
                 target_columns = [column_names]
            else: # List of column names
                 target_columns = column_names

        scan_prefixes = []
        if target_columns:
            for col_name in target_columns:
                # Prefix for a specific column: dataset? + row_key + col_name
                scan_prefixes.append(KeyCodec.encode(dataset_name=dataset_name, row_key=row_key, column_name=col_name))
        else:
            # Prefix for all columns in a row: dataset? + row_key
            scan_prefixes.append(KeyCodec.encode(dataset_name=dataset_name, row_key=row_key))
        logger.info(f'Prefixes to look for {scan_prefixes}')
        for rdb_key, _ in self.db.items():
            for prefix_bytes in scan_prefixes:
                if not rdb_key.startswith(prefix_bytes):
                    continue # Moved past our prefix

                decoded = KeyCodec.decode_key(rdb_key, has_dataset_name_in_key)
                if not decoded:
                    continue

                _, _, current_col_name, current_ts_ms = decoded

                # Time range filtering
                if start_ts_ms is not None and current_ts_ms < start_ts_ms:
                    continue
                if end_ts_ms is not None and current_ts_ms > end_ts_ms:
                    # Since keys are sorted reverse-chronologically for timestamp,
                    # if we pass end_ts_ms, subsequent keys for this column will also be too old.
                    # This logic might need refinement if scanning multiple columns with one prefix.
                    # For simplicity, we continue scanning, but a more optimized seek might be possible.
                    continue

                if current_col_name not in results:
                    results[current_col_name] = []

                if len(results[current_col_name]) < num_versions:
                    rdb_value_bytes = self.db.get(rdb_key)
                    if rdb_value_bytes is not None:
                         results[current_col_name].append((current_ts_ms, rdb_value_bytes.decode('utf-8')))
                # else: if we are scanning for specific columns, we might break early once versions are met for that column

        return results


    def delete_row(self, row_key, column_names=None, dataset_name=None, specific_timestamps_ms=None):
        """
        Deletes data for a row.
        If column_names is None, deletes all data for the row_key.
        If column_names is provided, deletes only those columns.
        If specific_timestamps_ms (list) is provided with a single column_name, deletes only those specific versions.
        """
        batch, count = rocksdb.WriteBatch(), 0
        has_dataset_name_in_key = dataset_name is not None

        # Case 1: Delete specific timestamps for a single column
        if column_names and isinstance(column_names, str) and specific_timestamps_ms:
            logger.info(f'Deleting a single column {column_names} with timestamps {specific_timestamps_ms}')
            single_col_name = column_names
            for ts_ms in specific_timestamps_ms:
                rdb_key = KeyCodec.encode(dataset_name, row_key, single_col_name, ts_ms)
                batch.delete(rdb_key)
                count += 1
            self.db.write(batch)
            return

        # Case 2 & 3: Delete columns or entire row (prefix-based deletion)
        target_cols_to_scan = []
        if column_names:
            if isinstance(column_names, str): target_cols_to_scan = [column_names]
            else: target_cols_to_scan = column_names

        scan_prefixes_for_delete = []
        if target_cols_to_scan: # Delete specific columns
            for col_name in target_cols_to_scan:
                scan_prefixes_for_delete.append(KeyCodec.encode(dataset_name=dataset_name, row_key=row_key, column_name=col_name))
        else: # Delete all columns for the row_key
            scan_prefixes_for_delete.append(KeyCodec.encode(dataset_name=dataset_name, row_key=row_key))

        logger.info(f'Scan prefixes to delete {scan_prefixes_for_delete}')
        for rdb_key, _ in self.db.items():
            for prefix_bytes in scan_prefixes_for_delete:
                if not rdb_key.startswith(prefix_bytes):
                    continue
                batch.delete(rdb_key)
                count += 1
        if count > 0: # Check if batch has operations
             self.db.write(batch)

    def close(self):
        if self.db:
            # In a real application, you might need to ensure all column family handles are closed if used.
            # For a simple DB instance, this might be sufficient or just allow it to be GC'd.
            # python-rocksdb's DB object doesn't have an explicit close method.
            # Deleting the object or letting it go out of scope handles cleanup.
            del self.db
            self.db = None
