import rocksdbpy as rocksdb
import struct
import time

# Define a separator that's unlikely to appear in keys/column names
KEY_SEPARATOR = b'\x00'
# Assuming timestamps are uint64 (e.g., nanoseconds or milliseconds)
MAX_UINT64 = 2**64 - 1

class WideColumnDB:
    def __init__(self, db_path):
        opts = rocksdb.Option()
        opts.create_if_missing(True)
        # For more advanced usage, you might explore Column Families here
        # to represent different datasets/tables within the same DB.
        self.db = rocksdb.open(db_path, opts)

    def _current_timestamp_ms(self):
        return int(time.time() * 1000)

    def _encode_key(self, row_key, column_name, timestamp_ms, dataset_name=None):
        # Ensure inputs are strings if they are not already
        row_key_bytes = str(row_key).encode('utf-8')
        column_name_bytes = str(column_name).encode('utf-8')

        # Invert timestamp for descending order (latest first)
        inverted_ts = MAX_UINT64 - timestamp_ms
        timestamp_bytes = struct.pack('>Q', inverted_ts) # Big-endian 8-byte unsigned int

        if dataset_name:
            dataset_name_bytes = str(dataset_name).encode('utf-8')
            return dataset_name_bytes + KEY_SEPARATOR + row_key_bytes + KEY_SEPARATOR + column_name_bytes + KEY_SEPARATOR + timestamp_bytes
        else:
            return row_key_bytes + KEY_SEPARATOR + column_name_bytes + KEY_SEPARATOR + timestamp_bytes

    def _decode_key(self, rdb_key_bytes, has_dataset_name=False):
        parts = rdb_key_bytes.split(KEY_SEPARATOR)
        offset = 0
        dataset_name = None
        if has_dataset_name:
            if len(parts) < 4: return None # Malformed key
            dataset_name = parts[0].decode('utf-8')
            offset = 1

        if len(parts) < (3 + offset): return None # Malformed key

        row_key = parts[0 + offset].decode('utf-8')
        column_name = parts[1 + offset].decode('utf-8')

        try:
            timestamp_bytes = parts[2 + offset]
            inverted_ts = struct.unpack('>Q', timestamp_bytes)[0]
            original_timestamp_ms = MAX_UINT64 - inverted_ts
            return dataset_name, row_key, column_name, original_timestamp_ms
        except (struct.error, IndexError):
            return None # Malformed key

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

            rdb_key = self._encode_key(row_key, column_name, timestamp_ms, dataset_name)
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
                prefix_parts = []
                if dataset_name:
                    prefix_parts.append(str(dataset_name).encode('utf-8'))
                prefix_parts.append(str(row_key).encode('utf-8'))
                prefix_parts.append(str(col_name).encode('utf-8'))
                scan_prefixes.append(KEY_SEPARATOR.join(prefix_parts) + KEY_SEPARATOR)
        else:
            # Prefix for all columns in a row: dataset? + row_key
            prefix_parts = []
            if dataset_name:
                prefix_parts.append(str(dataset_name).encode('utf-8'))
            prefix_parts.append(str(row_key).encode('utf-8'))
            scan_prefixes.append(KEY_SEPARATOR.join(prefix_parts) + KEY_SEPARATOR)

        for prefix_bytes in scan_prefixes:
            it = self.db.iterator() # Iterate over values
            it.seek(prefix_bytes) # Seek to the start of the prefix

            # Need to iterate keys as well to decode them
            kv_iterator = self.db.iterkeys()
            kv_iterator.seek(prefix_bytes)

            for rdb_key in kv_iterator:
                if not rdb_key.startswith(prefix_bytes):
                    break # Moved past our prefix

                decoded = self._decode_key(rdb_key, has_dataset_name_in_key)
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
        batch = rocksdb.WriteBatch()
        has_dataset_name_in_key = dataset_name is not None

        # Case 1: Delete specific timestamps for a single column
        if column_names and isinstance(column_names, str) and specific_timestamps_ms:
            single_col_name = column_names
            for ts_ms in specific_timestamps_ms:
                rdb_key = self._encode_key(row_key, single_col_name, ts_ms, dataset_name)
                batch.delete(rdb_key)
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
                prefix_parts = []
                if dataset_name: prefix_parts.append(str(dataset_name).encode('utf-8'))
                prefix_parts.append(str(row_key).encode('utf-8'))
                prefix_parts.append(str(col_name).encode('utf-8'))
                scan_prefixes_for_delete.append(KEY_SEPARATOR.join(prefix_parts) + KEY_SEPARATOR)
        else: # Delete all columns for the row_key
            prefix_parts = []
            if dataset_name: prefix_parts.append(str(dataset_name).encode('utf-8'))
            prefix_parts.append(str(row_key).encode('utf-8'))
            scan_prefixes_for_delete.append(KEY_SEPARATOR.join(prefix_parts) + KEY_SEPARATOR)

        for prefix_bytes in scan_prefixes_for_delete:
            it = self.db.iterkeys()
            it.seek(prefix_bytes)
            keys_to_delete = []
            for rdb_key in it:
                if not rdb_key.startswith(prefix_bytes):
                    break
                keys_to_delete.append(rdb_key)

            for key_del in keys_to_delete:
                batch.delete(key_del)

        if batch.count() > 0: # Check if batch has operations
             self.db.write(batch)

    def close(self):
        if self.db:
            # In a real application, you might need to ensure all column family handles are closed if used.
            # For a simple DB instance, this might be sufficient or just allow it to be GC'd.
            # python-rocksdb's DB object doesn't have an explicit close method.
            # Deleting the object or letting it go out of scope handles cleanup.
            del self.db
            self.db = None
