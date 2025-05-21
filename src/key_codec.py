import struct
import logging

# Define a separator that's unlikely to appear in keys/column names
KEY_SEPARATOR = b'\x00'
# Assuming timestamps are uint64 (e.g., nanoseconds or milliseconds)
MAX_UINT64 = 2**64 - 1

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

    @staticmethod
    def encode(dataset_name=None, row_key=None, column_name=None, timestamp_ms=None):
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


    @staticmethod
    def decode_key(rdb_key_bytes, has_dataset_name=False):
        """
        Decodes a RocksDB key byte string back into its components.
        Returns (dataset_name, row_key, column_name, original_timestamp_ms) or None if malformed.
        """
        parts = rdb_key_bytes.split(KeyCodec.KEY_SEPARATOR)
        offset = 0
        dataset_name = None
        if has_dataset_name:
            if len(parts) < 4:
                 logging.warning(f"Malformed key during decode (dataset expected): {rdb_key_bytes}")
                 return None # Malformed key
            dataset_name = parts[0].decode('utf-8')
            offset = 1

        if len(parts) < (3 + offset):
            logging.warning(f"Malformed key during decode: {rdb_key_bytes}")
            return None # Malformed key

        try:
            row_key = parts[0 + offset].decode('utf-8')
            column_name = parts[1 + offset].decode('utf-8')
            timestamp_bytes = parts[2 + offset]

            # Ensure timestamp_bytes has the correct length for unpacking
            if len(timestamp_bytes) != struct.calcsize('>Q'):
                 logging.warning(f"Malformed timestamp bytes during decode: {rdb_key_bytes}")
                 return None

            inverted_ts = struct.unpack('>Q', timestamp_bytes)[0]
            original_timestamp_ms = KeyCodec.MAX_UINT64 - inverted_ts
            return dataset_name, row_key, column_name, original_timestamp_ms
        except (struct.error, IndexError, UnicodeDecodeError) as e:
            logging.warning(f"Error decoding key parts: {e} for key {rdb_key_bytes}")
            return None # Malformed key or decoding error
