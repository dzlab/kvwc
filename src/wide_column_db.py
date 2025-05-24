import logging
import rocksdict as rocksdb
import time
from .key_codec import KeyCodec
from .serializer import Serializer, StrSerializer # Import Serializer classes

# Set the logging level
logger = logging.getLogger(__name__)


class WideColumnDB:
    def __init__(self, db_path, key_codec=None, serializer=None):
        """
        Initializes the WideColumnDB.

        Args:
            db_path (str): The path to the RocksDB database.
            key_codec (KeyCodec, optional): The key codec to use. Defaults to KeyCodec().
            serializer (Serializer, optional): The value serializer to use.
                                              Defaults to StrSerializer().
        """
        opts = rocksdb.Options()
        opts.create_if_missing(True)
        # For more advanced usage, you might explore Column Families here
        # to represent different datasets/tables within the same DB.
        self.db = rocksdb.Rdict(db_path, opts)
        if key_codec is None:
            self.key_codec = KeyCodec()
        else:
            self.key_codec = key_codec

        if serializer is None:
            self.serializer = StrSerializer() # Default to StrSerializer
        elif not isinstance(serializer, Serializer):
             logger.warning("Provided serializer is not an instance of Serializer. Defaulting to StrSerializer.")
             self.serializer = StrSerializer()
        else:
            self.serializer = serializer
        logger.info(f"Using serializer: {type(self.serializer).__name__}")


    def _current_timestamp_ms(self):
        return int(time.time() * 1000)

    # Removed _serialize_value and _deserialize_value as they are now handled by the Serializer class

    # --- Public API Methods ---

    def put_row(self, row_key, items, dataset_name=None):
        """
        Puts items into a row.
        items: list of tuples, where each tuple is either:
               (column_name, value) - current server time is used as timestamp.
               (column_name, value, timestamp_ms) - the provided timestamp is used.
        """
        batch, count  = rocksdb.WriteBatch(), 0
        for item in items:
            column_name, value = item[0], item[1]
            timestamp_ms = item[2] if len(item) > 2 and item[2] is not None else self._current_timestamp_ms()

            rdb_key = self.key_codec.encode(dataset_name=dataset_name, row_key=row_key, column_name=column_name, timestamp_ms=timestamp_ms)
            rdb_value = self.serializer.serialize(value) # Use the serializer instance
            # Assuming serializer returns bytes or None on failure (or raises exception)
            if rdb_value is not None:
                 batch.put(rdb_key, rdb_value)
                 count += 1
            else:
                 logger.warning(f"Serialization failed for value of type {type(value).__name__} for row '{row_key}', column '{column_name}'. Skipping item.")

        if count > 0: # Only write if batch is not empty
             self.db.write(batch)

    def get_row(self, row_key, column_names=None, num_versions=1, dataset_name=None, start_ts_ms=None, end_ts_ms=None):
        """
        Gets data for a row.
        Returns a dictionary: {column_name: [(timestamp_ms, value), ...]}
        """
        results = {}

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
                scan_prefixes.append(self.key_codec.encode(dataset_name=dataset_name, row_key=row_key, column_name=col_name))
        else:
            # Prefix for all columns in a row: dataset? + row_key
            scan_prefixes.append(self.key_codec.encode(dataset_name=dataset_name, row_key=row_key))

        logger.info(f'Prefixes to look for {scan_prefixes}')

        # Use RocksDB iterator with seek for efficient scanning
        # Iterate through each prefix and use from_key to seek
        for prefix_bytes in scan_prefixes:
            # Use items(from_key=prefix_bytes) to seek to the start of the prefix range
            for rdb_key, rdb_value_bytes in self.db.items(from_key=prefix_bytes):
                # Stop when the key no longer starts with the current prefix
                if not rdb_key.startswith(prefix_bytes):
                    break # Exit the inner loop for this prefix

                # Decode the key to get components
                decoded = self.key_codec.decode(rdb_key)

                if not decoded:
                    logger.warning(f"Skipping malformed key during scan starting from {prefix_bytes.hex()}: {rdb_key.hex()}")
                    continue

                # Assuming decode returns (dataset_name, row_key, column_name, original_timestamp_ms)
                try:
                    _, _, current_col_name, current_ts_ms = decoded
                except ValueError:
                     logger.warning(f"Unexpected number of decoded parts for key {rdb_key.hex()}")
                     continue


                # Apply time range filtering
                # Keys within a column prefix are sorted reverse-chronologically by timestamp (newest first).
                if start_ts_ms is not None and current_ts_ms < start_ts_ms:
                    # This version is too old. Since we iterate newest-to-oldest,
                    # all subsequent versions will also be too old.
                    break # Optimization: Exit inner loop for this prefix/column
                if end_ts_ms is not None and current_ts_ms > end_ts_ms:
                    # This version is too new. Skip it and continue to the next (older) version.
                    continue

                # Collect results, respecting num_versions
                if current_col_name not in results:
                    results[current_col_name] = []

                # Add the version if we haven't reached the version limit for this column
                if len(results[current_col_name]) < num_versions:
                     if rdb_value_bytes is not None:
                         results[current_col_name].append((current_ts_ms, self.serializer.deserialize(rdb_value_bytes)))
                # else: num_versions reached for this column, skip adding this version, but continue scanning
                # the prefix in case there are other columns covered by this prefix (if it's a row prefix scan).


        return results


    def delete_row(self, row_key, column_names=None, dataset_name=None, specific_timestamps_ms=None):
        """
        Deletes data for a row.
        If column_names is None, deletes all data for the row_key.
        If column_names is provided, deletes only those columns.
        If specific_timestamps_ms (list) is provided with a single column_name, deletes only those specific versions.
        """
        batch, count = rocksdb.WriteBatch(), 0

        # Case 1: Delete specific timestamps for a single column
        if column_names and isinstance(column_names, str) and specific_timestamps_ms:
            logger.info(f'Deleting a single column {column_names} with timestamps {specific_timestamps_ms}')
            single_col_name = column_names
            for ts_ms in specific_timestamps_ms:
                rdb_key = self.key_codec.encode(dataset_name, row_key, single_col_name, ts_ms)
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
                scan_prefixes_for_delete.append(self.key_codec.encode(dataset_name=dataset_name, row_key=row_key, column_name=col_name))
        else: # Delete all columns for the row_key
            scan_prefixes_for_delete.append(self.key_codec.encode(dataset_name=dataset_name, row_key=row_key))

        logger.info(f'Scan prefixes to delete {scan_prefixes_for_delete}')
        # Iterate through each prefix and use from_key to seek
        for prefix_bytes in scan_prefixes_for_delete:
            # Use items(from_key=prefix_bytes) to seek to the start of the prefix range
            for rdb_key, _ in self.db.items(from_key=prefix_bytes):
                # Stop when the key no longer starts with the current prefix
                if not rdb_key.startswith(prefix_bytes):
                    break # Exit the inner loop for this prefix

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
