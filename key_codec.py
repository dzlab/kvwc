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
    Key format: [dataset_name]\x00[row_key]\x00[column_name]\x00[inverted_timestamp_ms]
    Timestamp is inverted to ensure descending order (latest first).
    """

    KEY_SEPARATOR = b'\x00'
    MAX_UINT64 = 2**64 - 1

    @staticmethod
    def encode_key(row_key, column_name, timestamp_ms, dataset_name=None):
        """
        Encodes components into a RocksDB key byte string.
        Key format: [dataset_name]\x00[row_key]\x00[column_name]\x00[inverted_timestamp_ms]
        Timestamp is inverted to ensure descending order (latest first).
        """
        # Ensure inputs are strings if they are not already
        row_key_bytes = str(row_key).encode('utf-8')
        column_name_bytes = str(column_name).encode('utf-8')

        # Invert timestamp for descending order (latest first)
        inverted_ts = KeyCodec.MAX_UINT64 - timestamp_ms
        timestamp_bytes = struct.pack('>Q', inverted_ts) # Big-endian 8-byte unsigned int

        parts = []
        if dataset_name:
            parts.append(str(dataset_name).encode('utf-8'))
        parts.append(row_key_bytes)
        parts.append(column_name_bytes)
        parts.append(timestamp_bytes)

        return KeyCodec.KEY_SEPARATOR.join(parts)

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

            inverted_ts = struct.unpack('>Q', timestamp_bytes)[0]
            original_timestamp_ms = KeyCodec.MAX_UINT64 - inverted_ts
            return dataset_name, row_key, column_name, original_timestamp_ms
        except (struct.error, IndexError, UnicodeDecodeError) as e:
            logging.warning(f"Error decoding key parts: {e} for key {rdb_key_bytes}")
            return None # Malformed key or decoding error
