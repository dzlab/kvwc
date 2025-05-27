import logging
import rocksdict as rocksdb
import time
from .key_codec import KeyCodec # Keep KeyCodec for now, will modify
from .serializer import Serializer, StrSerializer # Import Serializer classes
from .db_manager import RocksDBManager # Import the new DB manager

# Set the logging level
logger = logging.getLogger(__name__)


class WideColumnDB:
    def __init__(self, db_path, key_codec=None, serializer=None, rocksdb_options=None, column_families=None):
        """
        Initializes the WideColumnDB.

        Args:
            db_path (str): The path to the RocksDB database.
            key_codec (KeyCodec, optional): The key codec to use. Defaults to KeyCodec().
                                            Note: Codecs will now encode keys *without* dataset_name.
            serializer (Serializer, optional): The value serializer to use.
                                              Defaults to StrSerializer().
            rocksdb_options (dict, optional): A dictionary of RocksDB options to apply.
                                              Keys should correspond to settable options. Defaults to None.
            column_families (list, optional): A list of Column Family names (strings) to open/create.
                                             These will correspond to dataset_names. Defaults to None.
                                             The 'default' CF is always opened.
        """
        # Initialize the DB manager with provided column families
        self._db_manager = RocksDBManager(db_path, rocksdb_options=rocksdb_options, column_families=column_families)
        # Open the database via the manager
        self._db_manager.open_db() # This will raise an exception if it fails

        # Store the list of known column families for validation/reference
        # The DB manager ensures 'default' is always included
        self._known_column_families = ['default'] + (column_families if column_families is not None else [])
        # Remove duplicates just in case 'default' was explicitly passed
        self._known_column_families = list(set(self._known_column_families))


        if key_codec is None:
            # Initialize default codec (will be modified later to exclude dataset_name)
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

    def _get_cf_handle(self, dataset_name):
        """
        Gets the rocksdict handle for the specified dataset_name (Column Family).
        Uses 'default' CF if dataset_name is None or not provided.
        Raises an error if the dataset_name corresponds to a CF that wasn't opened.
        """
        cf_name = dataset_name if dataset_name is not None else 'default'
        if cf_name not in self._known_column_families:
             # This check prevents using CFs that weren't specified during init.
             # RocksDB might implicitly create them anyway depending on version/options,
             # but requiring them to be listed in init makes the CFs explicit.
             # Alternatively, we could allow implicit creation here, but being explicit
             # is generally better for managing CFs.
             logger.warning(f"Attempted to access unknown Column Family: {cf_name}. Was it included in column_families during initialization?")
             # Decide whether to raise error or default to 'default'/'None' CF.
             # For now, let's raise an error to enforce explicit CF listing.
             raise ValueError(f"Column Family '{cf_name}' is not known. Please include it in the 'column_families' list when initializing WideColumnDB.")

        db_instance = self._db_manager.db
        if db_instance is None:
            raise RuntimeError("Database is not initialized. Cannot get CF handle.")

        # Access the CF handle using dictionary-like access
        try:
            return db_instance[cf_name]
        except KeyError:
             # This should ideally not happen if _known_column_families is correct and DB opened successfully
             logger.error(f"Failed to get handle for Column Family '{cf_name}'. It might not have been opened correctly.")
             raise

    # --- Public API Methods ---

    def put_row(self, row_key, items, dataset_name=None):
        """
        Puts items into a row within a specific dataset (Column Family).
        items: list of tuples, where each tuple is either:
               (column_name, value) - current server time is used as timestamp.
               (column_name, value, timestamp_ms) - the provided timestamp is used.
        dataset_name: The name of the dataset (Column Family) to use. Defaults to 'default'.
        """
        # Get the correct CF handle
        cf_handle = self._get_cf_handle(dataset_name)

        batch, count  = rocksdb.WriteBatch(), 0
        for item in items:
            column_name, value = item[0], item[1]
            timestamp_ms = item[2] if len(item) > 2 and item[2] is not None else self._current_timestamp_ms()

            # Encode key *without* dataset_name
            rdb_key = self.key_codec.encode(row_key=row_key, column_name=column_name, timestamp_ms=timestamp_ms)

            # Check if key encoding failed (e.g., part too long for length prefix codec)
            if rdb_key is None:
                 logger.warning(f"Key encoding failed for row '{row_key}', column '{column_name}'. Skipping item.")
                 continue

            rdb_value = self.serializer.serialize(value) # Use the serializer instance
            # Assuming serializer returns bytes or None on failure (or raises exception)
            if rdb_value is not None:
                 # Add to batch specifying the CF handle
                 batch.put(rdb_key, rdb_value, column_family=cf_handle)
                 count += 1
            else:
                 logger.warning(f"Serialization failed for value of type {type(value).__name__} for row '{row_key}', column '{column_name}'. Skipping item.")

        if count > 0: # Only write if batch is not empty
            try:
                 # Write batch to the database instance (the Rdict object itself)
                 # The operations within the batch already specify the target CF
                 self._db_manager.db.write(batch)
            except Exception as e:
                 logger.error(f"Error writing batch to database for row '{row_key}' in CF '{dataset_name or 'default'}': {e}")
                 # Re-raise the exception to indicate failure
                 raise e

    def get_row(self, row_key, column_names=None, num_versions=1, dataset_name=None, start_ts_ms=None, end_ts_ms=None):
        """
        Gets data for a row within a specific dataset (Column Family).
        Returns a dictionary: {column_name: [(timestamp_ms, value), ...]}
        dataset_name: The name of the dataset (Column Family) to use. Defaults to 'default'.
        """
        # Get the correct CF handle
        cf_handle = self._get_cf_handle(dataset_name)

        results = {}

        # Build the prefix for scanning *without* dataset_name
        target_columns = []
        if column_names:
            if isinstance(column_names, str): # Single column name
                 target_columns = [column_names]
            else: # List of column names
                 target_columns = column_names

        scan_prefixes = []
        if target_columns:
            for col_name in target_columns:
                # Prefix for a specific column: row_key + col_name (no dataset)
                scan_prefixes.append(self.key_codec.encode(row_key=row_key, column_name=col_name))
        else:
            # Prefix for all columns in a row: row_key (no dataset)
            scan_prefixes.append(self.key_codec.encode(row_key=row_key))


        # Filter out any potential None results from encoding (e.g., due to length prefix limits)
        valid_scan_prefixes = [p for p in scan_prefixes if p is not None]

        if not valid_scan_prefixes:
            logger.warning(f"No valid scan prefixes could be generated for row '{row_key}' in CF '{dataset_name or 'default'}'. Returning empty results.")
            return results

        logger.info(f'Prefixes to look for in CF {dataset_name or "default"}: {valid_scan_prefixes}')

        # Use RocksDB iterator on the specific CF handle
        # Iterate through each prefix and use from_key to seek
        for prefix_bytes in valid_scan_prefixes:
            # Use items(from_key=prefix_bytes) on the CF handle to seek within that CF
            for rdb_key, rdb_value_bytes in cf_handle.items(from_key=prefix_bytes):
                # Stop when the key no longer starts with the current prefix
                if not rdb_key.startswith(prefix_bytes):
                    break # Exit the inner loop for this prefix

                # Decode the key to get components (now without dataset_name)
                # KeyCodec.decode will need to be updated to expect key format without dataset_name
                decoded = self.key_codec.decode(rdb_key)

                if not decoded:
                    logger.warning(f"Skipping malformed key during scan in CF {dataset_name or 'default'} starting from {prefix_bytes.hex()}: {rdb_key.hex()}")
                    continue

                # Assuming decode returns (row_key, column_name, original_timestamp_ms) after codec update
                try:
                    current_row_key, current_col_name, current_ts_ms = decoded
                except ValueError:
                     logger.warning(f"Unexpected number of decoded parts for key {rdb_key.hex()} in CF {dataset_name or 'default'}")
                     continue

                # Double-check that the decoded row_key matches the requested row_key,
                # although the prefix scan should handle this, it's a good sanity check.
                if current_row_key != row_key:
                    logger.warning(f"Skipping key with mismatched row_key '{current_row_key}' during scan for '{row_key}' in CF {dataset_name or 'default'}: {rdb_key.hex()}")
                    continue


                # Optimization: If we've already collected num_versions for this column,
                # skip processing further older versions for this column within this CF.
                # Note: This optimization works because keys for a column prefix are sorted by timestamp descending.
                if current_col_name in results and len(results[current_col_name]) >= num_versions:
                    continue # Move to the next key from the iterator


                # Apply time range filtering
                # Keys within a column prefix are sorted reverse-chronologically by timestamp (newest first).
                if start_ts_ms is not None and current_ts_ms < start_ts_ms:
                    # This version is too old. Since we iterate newest-to-oldest,
                    # all subsequent versions will also be too old within this column prefix.
                    # Note: This break only applies *within the current column prefix*.
                    # The outer loop continues to the next scan_prefix (potentially a different column).
                    # For row-level prefix scans, this break would stop scanning the entire row.
                    # Need to be careful with this optimization depending on prefix type.
                    # For a row-level prefix (col_names=None), breaking here is correct as it stops
                    # processing all older versions in the row.
                    # For column-level prefixes (col_names list), breaking here only stops for that specific column.
                    # The current structure handles this correctly because we iterate by prefix.
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
        Deletes data for a row within a specific dataset (Column Family).
        If column_names is None, deletes all data for the row_key in the specified dataset.
        If column_names is provided, deletes only those columns in the specified dataset.
        If specific_timestamps_ms (list) is provided with a single column_name, deletes only those specific versions in the specified dataset.
        dataset_name: The name of the dataset (Column Family) to use. Defaults to 'default'.
        """
        # Get the correct CF handle
        cf_handle = self._get_cf_handle(dataset_name)

        batch, count = rocksdb.WriteBatch(), 0

        # Case 1: Delete specific timestamps for a single column
        if column_names and isinstance(column_names, str) and specific_timestamps_ms:
            logger.info(f'Deleting a single column {column_names} with timestamps {specific_timestamps_ms} in CF {dataset_name or "default"}')
            single_col_name = column_names
            for ts_ms in specific_timestamps_ms:
                # Encode key *without* dataset_name
                rdb_key = self.key_codec.encode(None, row_key, single_col_name, ts_ms) # Pass None for dataset_name

                if rdb_key is None:
                     logger.warning(f"Key encoding failed for deletion key row '{row_key}', column '{single_col_name}', timestamp {ts_ms}. Skipping deletion for this key.")
                     continue

                # Add to batch specifying the CF handle
                batch.delete(rdb_key, column_family=cf_handle)
                count += 1
            if count > 0:
                try:
                    # Write batch to the database instance
                    self._db_manager.db.write(batch)
                except Exception as e:
                    logger.error(f"Error writing batch for specific timestamp deletion for row '{row_key}', column '{single_col_name}' in CF '{dataset_name or 'default'}': {e}")
                    # Re-raise the exception to indicate failure
                    raise e
            return

        # Case 2 & 3: Delete columns or entire row (prefix-based deletion) within a CF
        target_cols_to_scan = []
        if column_names:
            if isinstance(column_names, str): target_cols_to_scan = [column_names]
            else: target_cols_to_scan = column_names

        scan_prefixes_for_delete = []
        if target_cols_to_scan: # Delete specific columns
            for col_name in target_cols_to_scan:
                # Encode prefix *without* dataset_name
                scan_prefixes_for_delete.append(self.key_codec.encode(None, row_key, col_name)) # Pass None for dataset_name
        else: # Delete all columns for the row_key in the specified CF
            # Encode prefix *without* dataset_name
            scan_prefixes_for_delete.append(self.key_codec.encode(None, row_key)) # Pass None for dataset_name

        # Filter out any potential None results from encoding (e.g., due to length prefix limits)
        valid_scan_prefixes = [p for p in scan_prefixes_for_delete if p is not None]

        if not valid_scan_prefixes:
            logger.warning(f"No valid scan prefixes could be generated for deletion for row '{row_key}' in CF '{dataset_name or 'default'}'. No data deleted.")
            return

        logger.info(f'Scan prefixes to delete in CF {dataset_name or "default"}: {valid_scan_prefixes}')

        # Iterate through each prefix on the specific CF handle and use from_key to seek
        for prefix_bytes in valid_scan_prefixes:
            # Use items(from_key=prefix_bytes) on the CF handle
            for rdb_key, _ in cf_handle.items(from_key=prefix_bytes):
                # Stop when the key no longer starts with the current prefix
                if not rdb_key.startswith(prefix_bytes):
                    break # Exit the inner loop for this prefix

                # Add to batch specifying the CF handle
                batch.delete(rdb_key, column_family=cf_handle)
                count += 1

        if count > 0: # Check if batch has operations
             # Write batch to the database instance
             self._db_manager.db.write(batch)


    def close(self):
        """
        Closes the database using the DB manager.
        """
        if self._db_manager:
            self._db_manager.close_db()
