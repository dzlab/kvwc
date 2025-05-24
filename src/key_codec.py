import struct
import logging

class KeyCodec:
    """
    A class for encoding and decoding keys used in the WideColumnDB.
    Keys are structured to support efficient retrieval by row, column, and time range.
    Key format (full key): [dataset_name]\x00[row_key]\x00[column_name]\x00[inverted_timestamp_ms]
    Key format (prefix): [dataset_name]\x00[row_key]\x00[column_name]\x00
    Timestamp is inverted to ensure descending order (latest first).
    """

    KEY_SEPARATOR = b'\x00'
    MAX_UINT64 = 2**64 - 1

    def encode(self, dataset_name=None, row_key=None, column_name=None, timestamp_ms=None):
        """
        Encodes components into a RocksDB key byte string or a prefix byte string.
        If timestamp_ms is provided, encodes a full key.
        If timestamp_ms is None, encodes a prefix.
        """
        parts = []

        if dataset_name is not None:
            parts.append(str(dataset_name).encode('utf-8'))

        if row_key is not None:
            parts.append(str(row_key).encode('utf-8'))

        if column_name is not None:
            parts.append(str(column_name).encode('utf-8'))

        if timestamp_ms is not None:
            # This is a full key
            # Invert timestamp for descending order (latest first)
            inverted_ts = KeyCodec.MAX_UINT64 - timestamp_ms
            timestamp_bytes = struct.pack('>Q', inverted_ts) # Big-endian 8-byte unsigned int
            parts.append(timestamp_bytes)
            return KeyCodec.KEY_SEPARATOR.join(parts)
        else:
            # This is a prefix for scanning/deletion
            # Append the separator to make it a valid prefix that matches keys
            # starting with these components followed by a separator.
            return KeyCodec.KEY_SEPARATOR.join(parts) + KeyCodec.KEY_SEPARATOR


    def decode(self, rdb_key_bytes):
        """
        Decodes a RocksDB key byte string back into its components.
        Assumes the key is a full key ending with a timestamp.
        Returns (dataset_name, row_key, column_name, original_timestamp_ms) or None if malformed.
        """
        parts = rdb_key_bytes.split(KeyCodec.KEY_SEPARATOR)

        dataset_name_bytes = None
        row_key_bytes = None
        column_name_bytes = None
        timestamp_bytes = None

        # Determine the structure based on the number of parts
        if len(parts) == 4:
            # Format: [dataset_name]\x00[row_key]\x00[column_name]\x00[timestamp]
            dataset_name_bytes = parts[0]
            row_key_bytes = parts[1]
            column_name_bytes = parts[2]
            timestamp_bytes = parts[3]
        elif len(parts) == 3:
            # Format: [row_key]\x00[column_name]\x00[timestamp]
            # dataset_name remains None
            row_key_bytes = parts[0]
            column_name_bytes = parts[1]
            timestamp_bytes = parts[2]
        else:
            logging.warning(f"Malformed key during decode (unexpected parts count {len(parts)}): {rdb_key_bytes.hex()}")
            return None # Malformed key

        # Now decode the parts that are expected to be strings
        dataset_name = None
        row_key = None
        column_name = None

        try:
            if dataset_name_bytes is not None:
                 dataset_name = dataset_name_bytes.decode('utf-8')
            row_key = row_key_bytes.decode('utf-8')
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
            return dataset_name, row_key, column_name, original_timestamp_ms
        except struct.error as e:
            logging.warning(f"Error unpacking timestamp: {e} for key {rdb_key_bytes.hex()}")
            return None
