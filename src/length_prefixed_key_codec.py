import struct
import logging

# Assuming timestamps are uint64 (e.g., nanoseconds or milliseconds)
MAX_UINT64 = 2**64 - 1

class LengthPrefixedKeyCodec:
    """
    A class for encoding and decoding keys used in the WideColumnDB
    using a length-prefixed format.
    Keys are structured to support efficient retrieval by row, column, and time range.
    Key format (full key): [len_dataset][dataset_name][len_row][row_key][len_column][column_name][inverted_timestamp_ms]
    Key format (prefix): [len_dataset][dataset_name][len_row][row_key][len_column][column_name]
    Timestamp is inverted to ensure descending order (latest first).
    Length prefix is a single byte (max length 255).
    """

    MAX_UINT64 = 2**64 - 1

    def encode(self, dataset_name=None, row_key=None, column_name=None, timestamp_ms=None):
        """
        Encodes components into a RocksDB key byte string or a prefix byte string
        using the length-prefixed format.
        If timestamp_ms is provided, encodes a full key.
        If timestamp_ms is None, encodes a prefix.
        String components are length-prefixed with a single byte (max length 255).
        Returns bytes or None if a component is too long.
        """
        encoded_parts = []

        def _encode_part(part):
            if part is None:
                # Represent None or empty string with length prefix 0
                part_bytes = b''
            else:
                 part_bytes = str(part).encode('utf-8')

            if len(part_bytes) > 255:
                # Handle error: part too long for single-byte length prefix
                logging.error(f"Key component too long ({len(part_bytes)} bytes) for length-prefix encoding: {part}")
                return None # Indicate failure

            return bytes([len(part_bytes)]) + part_bytes

        # Encode string parts
        encoded_dataset = _encode_part(dataset_name)
        encoded_row = _encode_part(row_key)
        encoded_column = _encode_part(column_name)

        # Check if any part encoding failed
        if encoded_dataset is None or encoded_row is None or encoded_column is None:
             return None

        encoded_parts.extend([encoded_dataset, encoded_row, encoded_column])

        if timestamp_ms is not None:
            # This is a full key
            # Invert timestamp for descending order (latest first)
            inverted_ts = LengthPrefixedKeyCodec.MAX_UINT64 - timestamp_ms
            timestamp_bytes = struct.pack('>Q', inverted_ts) # Big-endian 8-byte unsigned int
            encoded_parts.append(timestamp_bytes)
            return b''.join(encoded_parts)
        else:
            # This is a prefix for scanning/deletion
            return b''.join(encoded_parts)

    def decode(self, rdb_key_bytes):
        """
        Decodes a RocksDB key byte string encoded with the length-prefixed format
        back into its components.
        Assumes the key is a full key ending with a timestamp.
        Returns (dataset_name, row_key, column_name, original_timestamp_ms) or None if malformed.
        """
        offset = 0

        def _decode_part(data, current_offset):
            if current_offset >= len(data):
                logging.warning(f"Malformed key during length-prefix decode: not enough data for length byte at offset {current_offset}")
                return None, current_offset # Not enough data for length byte

            length = data[current_offset]
            current_offset += 1

            if current_offset + length > len(data):
                 logging.warning(f"Malformed key during length-prefix decode: not enough data for part (expected {length} bytes) at offset {current_offset - 1}")
                 return None, current_offset # Not enough data for the part

            part_bytes = data[current_offset : current_offset + length]
            current_offset += length

            try:
                part_str = part_bytes.decode('utf-8') if length > 0 else None
                return part_str, current_offset
            except UnicodeDecodeError:
                 logging.warning(f"Malformed key during length-prefix decode (unicode error) decoding {length} bytes at offset {current_offset - length}: {data.hex()}")
                 return None, current_offset # Return offset to try and continue parsing

        # Decode parts: dataset_name, row_key, column_name
        dataset_name, offset = _decode_part(rdb_key_bytes, offset)
        if dataset_name is None and offset < len(rdb_key_bytes) and rdb_key_bytes[offset-1] != 0:
             # Failed to decode a non-empty part
             logging.warning(f"Malformed key during length-prefix decode (failed to decode dataset_name): {rdb_key_bytes.hex()}")
             return None

        row_key, offset = _decode_part(rdb_key_bytes, offset)
        if row_key is None and offset < len(rdb_key_bytes) and rdb_key_bytes[offset-1] != 0:
             logging.warning(f"Malformed key during length-prefix decode (failed to decode row_key): {rdb_key_bytes.hex()}")
             return None

        column_name, offset = _decode_part(rdb_key_bytes, offset)
        if column_name is None and offset < len(rdb_key_bytes) and rdb_key_bytes[offset-1] != 0:
             logging.warning(f"Malformed key during length-prefix decode (failed to decode column_name): {rdb_key_bytes.hex()}")
             return None

        # Decode timestamp if remaining bytes match timestamp size
        timestamp_bytes_size = struct.calcsize('>Q')
        if len(rdb_key_bytes) - offset == timestamp_bytes_size:
            try:
                timestamp_bytes = rdb_key_bytes[offset : offset + timestamp_bytes_size]
                inverted_ts = struct.unpack('>Q', timestamp_bytes)[0]
                original_timestamp_ms = LengthPrefixedKeyCodec.MAX_UINT64 - inverted_ts
                # offset += timestamp_bytes_size # No need to update offset further
                return dataset_name, row_key, column_name, original_timestamp_ms
            except struct.error as e:
                logging.warning(f"Error unpacking timestamp during length-prefix decode: {e} for key {rdb_key_bytes.hex()}")
                return None
        elif len(rdb_key_bytes) - offset == 0:
             # This might be a prefix, but decode is for full keys. Treat as malformed for now.
             logging.warning(f"Malformed key during length-prefix decode (no timestamp found where expected): {rdb_key_bytes.hex()}")
             return None
        else:
            # Remaining bytes are not the size of a timestamp
            logging.warning(f"Malformed key during length-prefix decode (unexpected remaining bytes size {len(rdb_key_bytes) - offset}): {rdb_key_bytes.hex()}")
            return None
