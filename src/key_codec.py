import struct
import logging

class KeyCodec:
    """
    A class for encoding and decoding keys used in the WideColumnDB *within a Column Family*.
    Keys are structured to support efficient retrieval by row, column, and time range.
    Key format (full key): [row_key]\x00[column_name]\x00[inverted_timestamp_ms]
    Key format (prefix): [row_key]\x00[column_name]\x00
    Timestamp is inverted to ensure descending order (latest first).
    Note: Dataset name is handled by Column Families, not part of the key here.
    """

    KEY_SEPARATOR = b'\x00'
    MAX_UINT64 = 2**64 - 1

    def encode(self, dataset_name=None, row_key=None, column_name=None, timestamp_ms=None):
        """
        Encodes components into a RocksDB key byte string or a prefix byte string
        *without* the dataset_name, as that is handled by the Column Family.
        If timestamp_ms is provided, encodes a full key.
        If timestamp_ms is None, encodes a prefix.
        """
        # Ignore dataset_name as it's handled by the Column Family
        _ = dataset_name

        parts = []

        # Row key is mandatory for valid keys/prefixes here
        if row_key is not None:
            parts.append(str(row_key).encode('utf-8'))
        else:
            # Invalid key state without a row key
            logging.error("Cannot encode key or prefix without a row_key.")
            return None


        # Column name is mandatory for column-specific keys/prefixes
        if column_name is not None:
            parts.append(str(column_name).encode('utf-8'))
        elif timestamp_ms is not None:
             # Column name is required for a full key
             logging.error("Cannot encode full key without a column_name.")
             return None
        # If column_name is None and timestamp_ms is None, this is a row prefix scan, which is okay.


        if timestamp_ms is not None:
            # This is a full key
            if column_name is None:
                 # Should have been caught above, but double check
                 logging.error("Cannot encode full key without a column_name (timestamp provided but column_name is None).")
                 return None

            # Invert timestamp for descending order (latest first)
            inverted_ts = KeyCodec.MAX_UINT64 - timestamp_ms
            timestamp_bytes = struct.pack('>Q', inverted_ts) # Big-endian 8-byte unsigned int
            parts.append(timestamp_bytes)
            return KeyCodec.KEY_SEPARATOR.join(parts)
        else:
            # This is a prefix for scanning/deletion
            # Append the separator to make it a valid prefix that matches keys
            # starting with these components followed by a separator.
            # If only row_key is present, the prefix is row_key\x00
            # If row_key and column_name are present, the prefix is row_key\x00column_name\x00
            return KeyCodec.KEY_SEPARATOR.join(parts) + KeyCodec.KEY_SEPARATOR


    def decode(self, rdb_key_bytes):
        """
        Decodes a RocksDB key byte string back into its components *without*
        expecting a dataset_name prefix.
        Assumes the key is a full key ending with a timestamp.
        Returns (row_key, column_name, original_timestamp_ms) or None if malformed.
        """
        parts = rdb_key_bytes.split(KeyCodec.KEY_SEPARATOR)

        row_key_bytes = None
        column_name_bytes = None
        timestamp_bytes = None

        # Determine the structure based on the number of parts
        # Expected format now: [row_key]\x00[column_name]\x00[timestamp] (3 parts)
        if len(parts) == 3:
            row_key_bytes = parts[0]
            column_name_bytes = parts[1]
            timestamp_bytes = parts[2]
        else:
            logging.warning(f"Malformed key during decode (unexpected parts count {len(parts)}), expected 3: {rdb_key_bytes.hex()}")
            return None # Malformed key

        # Now decode the parts that are expected to be strings
        row_key = None
        column_name = None

        try:
            if row_key_bytes is not None:
                row_key = row_key_bytes.decode('utf-8')
            if column_name_bytes is not None:
                 column_name = column_name_bytes.decode('utf-8')
        except UnicodeDecodeError:
             logging.warning(f"Malformed key during decode (unicode error): {rdb_key_bytes.hex()}")
             return None

        # Decode the timestamp
        if len(timestamp_bytes) != struct.calcsize('>Q'):
             logging.warning(f"Malformed timestamp bytes during decode (wrong size): {rdb_key_bytes.hex()}")
             return None

        try:
            inverted_ts = struct.unpack('>Q', timestamp_bytes)[0]
            original_timestamp_ms = KeyCodec.MAX_UINT64 - inverted_ts
            # Return tuple without dataset_name
            return row_key, column_name, original_timestamp_ms
        except struct.error as e:
            logging.warning(f"Error unpacking timestamp: {e} for key {rdb_key_bytes.hex()}")
            return None
