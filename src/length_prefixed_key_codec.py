import struct
import logging

# Assuming timestamps are uint64 (e.g., nanoseconds or milliseconds)
MAX_UINT64 = 2**64 - 1

class LengthPrefixedKeyCodec:
    """
    A class for encoding and decoding keys used in the WideColumnDB *within a Column Family*
    using a length-prefixed format.
    Keys are structured to support efficient retrieval by row, column, and time range.
    Key format (full key): [len_row][row_key][len_column][column_name][inverted_timestamp_ms]
    Key format (prefix): [len_row][row_key][len_column][column_name] or [len_row][row_key]
    Timestamp is inverted to ensure descending order (latest first).
    Length prefix is a single byte (max length 255).
    Note: Dataset name is handled by Column Families, not part of the key here.
    """

    MAX_UINT64 = 2**64 - 1

    def encode(self, dataset_name=None, row_key=None, column_name=None, timestamp_ms=None):
        """
        Encodes components into a RocksDB key byte string or a prefix byte string
        using the length-prefixed format, *without* the dataset_name.
        If timestamp_ms is provided, encodes a full key.
        If timestamp_ms is None, encodes a prefix.
        String components are length-prefixed with a single byte (max length 255).
        Returns bytes or None if a component is too long or mandatory parts are missing.
        """
        # Ignore dataset_name as it's handled by the Column Family
        _ = dataset_name

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

        # Row key is mandatory
        if row_key is not None:
             encoded_row = _encode_part(row_key)
             if encoded_row is None: return None # Failed to encode row key
             encoded_parts.append(encoded_row)
        else:
            logging.error("Cannot encode key or prefix without a row_key.")
            return None


        # Column name is required for full keys or column prefixes
        if timestamp_ms is not None or column_name is not None:
             encoded_column = _encode_part(column_name)
             if encoded_column is None: return None # Failed to encode column name
             encoded_parts.append(encoded_column)


        if timestamp_ms is not None:
            # This is a full key
            if column_name is None:
                 # Should have been caught above, but double check
                 logging.error("Cannot encode full key without a column_name (timestamp provided but column_name is None).")
                 return None

            # Invert timestamp for descending order (latest first)
            inverted_ts = LengthPrefixedKeyCodec.MAX_UINT64 - timestamp_ms
            timestamp_bytes = struct.pack('>Q', inverted_ts) # Big-endian 8-byte unsigned int
            encoded_parts.append(timestamp_bytes)
            return b''.join(encoded_parts)
        else:
            # This is a prefix for scanning/deletion
            # The prefix is [len_row][row_key][len_column][column_name]
            # or [len_row][row_key] if column_name was None.
            return b''.join(encoded_parts)

    def decode(self, rdb_key_bytes):
        """
        Decodes a RocksDB key byte string encoded with the length-prefixed format
        back into its components, *without* expecting a dataset_name prefix.
        Assumes the key is a full key ending with a timestamp.
        Returns (row_key, column_name, original_timestamp_ms) or None if malformed.
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
                # Decode to string if length > 0, otherwise return None or empty string?
                # Let's return None for length 0, matching the encoding logic's handling of None.
                part_str = part_bytes.decode('utf-8') if length > 0 else None
                return part_str, current_offset
            except UnicodeDecodeError:
                 logging.warning(f"Malformed key during length-prefix decode (unicode error) decoding {length} bytes at offset {current_offset - length}: {data.hex()}")
                 return None, current_offset # Return offset to try and continue parsing

        # Decode parts: row_key, column_name
        row_key, offset = _decode_part(rdb_key_bytes, offset)
        if row_key is None and (offset == 0 or (offset < len(rdb_key_bytes) and rdb_key_bytes[offset-1] != 0)):
             # Failed to decode a non-empty row_key, or row_key was expected but not found
             logging.warning(f"Malformed key during length-prefix decode (failed to decode row_key): {rdb_key_bytes.hex()}")
             return None

        column_name, offset = _decode_part(rdb_key_bytes, offset)
        if column_name is None and (offset < len(rdb_key_bytes) and rdb_key_bytes[offset-1] != 0):
             # Failed to decode a non-empty column_name, or column_name was expected but not found before timestamp
             logging.warning(f"Malformed key during length-prefix decode (failed to decode column_name): {rdb_key_bytes.hex()}")
             # Even if column_name is legitimately None (encoded as length 0),
             # if there are subsequent bytes, it implies something is wrong unless
             # those bytes are exactly the timestamp. So continue decoding timestamp.


        # Decode timestamp if remaining bytes match timestamp size
        timestamp_bytes_size = struct.calcsize('>Q')
        if len(rdb_key_bytes) - offset == timestamp_bytes_size:
            try:
                timestamp_bytes = rdb_key_bytes[offset : offset + timestamp_bytes_size]
                inverted_ts = struct.unpack('>Q', timestamp_bytes)[0]
                original_timestamp_ms = LengthPrefixedKeyCodec.MAX_UINT64 - inverted_ts
                # offset += timestamp_bytes_size # No need to update offset further
                # Return tuple without dataset_name
                return row_key, column_name, original_timestamp_ms
            except struct.error as e:
                logging.warning(f"Error unpacking timestamp during length-prefix decode: {e} for key {rdb_key_bytes.hex()}")
                return None
        # Removed the check for len(rdb_key_bytes) - offset == 0 as decode is for full keys
        else:
            # Remaining bytes are not the size of a timestamp, or there was no column name
            logging.warning(f"Malformed key during length-prefix decode (unexpected remaining bytes size {len(rdb_key_bytes) - offset} or missing column name): {rdb_key_bytes.hex()}")
            return None
