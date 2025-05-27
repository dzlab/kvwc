import unittest
import time
import struct
from src.key_codec import KeyCodec # Import from src
from src.length_prefixed_key_codec import LengthPrefixedKeyCodec # Import from src
import logging

# Configure basic logging for tests
# Avoid reconfiguring if already configured by root logging in test_wide_column_db
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


# Moved MAX_UINT64 definition here or import from codecs if they expose it publicly
# For now, let's use the definition from KeyCodec as it's the same.
MAX_UINT64 = KeyCodec.MAX_UINT64


class TestKeyCodecs(unittest.TestCase):

    def setUp(self):
        """Set up KeyCodec instances for each test."""
        # Codecs are initialized without dataset_name consideration
        self.separator_codec = KeyCodec()
        self.length_prefixed_codec = LengthPrefixedKeyCodec()
        self.test_ts_ms = int(time.time() * 1000)
        self.inverted_test_ts_bytes = struct.pack('>Q', MAX_UINT64 - self.test_ts_ms)


    # --- Tests for Separator-Based KeyCodec (without dataset_name) ---

    def test_separator_encode_full_key(self):
        key_bytes = self.separator_codec.encode(
            row_key="row1",
            column_name="colA",
            timestamp_ms=self.test_ts_ms
            # dataset_name is ignored by the codec
        )
        # Expected format: [row_key]\x00[column_name]\x00[timestamp]
        expected_bytes = b"row1\x00colA\x00" + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

    def test_separator_encode_full_key_missing_mandatory_parts(self):
         # row_key is mandatory
         self.assertIsNone(self.separator_codec.encode(column_name="colA", timestamp_ms=self.test_ts_ms))
         # column_name is mandatory for a full key
         self.assertIsNone(self.separator_codec.encode(row_key="row1", timestamp_ms=self.test_ts_ms))


    def test_separator_encode_prefix_column(self):
        prefix_bytes = self.separator_codec.encode(
            row_key="row1",
            column_name="colA"
            # dataset_name is ignored
        )
        # Expected format: [row_key]\x00[column_name]\x00
        expected_bytes = b"row1\x00colA\x00"
        self.assertEqual(prefix_bytes, expected_bytes)

    def test_separator_encode_prefix_row(self):
        prefix_bytes = self.separator_codec.encode(
            row_key="row1"
            # dataset_name and column_name are None
        )
        # Expected format: [row_key]\x00
        expected_bytes = b"row1\x00"
        self.assertEqual(prefix_bytes, expected_bytes)


    def test_separator_decode_full_key(self):
        # Encoded key without dataset_name
        encoded_key = b"row1\x00colA\x00" + self.inverted_test_ts_bytes
        decoded = self.separator_codec.decode(encoded_key)
        # Expected decoded format: (row_key, column_name, original_timestamp_ms)
        self.assertEqual(decoded, ("row1", "colA", self.test_ts_ms))


    def test_separator_decode_malformed(self):
        # Missing expected parts (should be 3 parts: row, col, ts)
        malformed_key_too_few_parts = b"row1\x00colA"
        self.assertIsNone(self.separator_codec.decode(malformed_key_too_few_parts))

        malformed_key_too_many_parts = b"part1\x00part2\x00part3\x00part4" # 4 parts
        self.assertIsNone(self.separator_codec.decode(malformed_key_too_many_parts))


        # Wrong timestamp size
        malformed_key_wrong_ts_size = b"row1\x00colA\x00" + b"\x00" * 7 # 7 bytes instead of 8
        self.assertIsNone(self.separator_codec.decode(malformed_key_wrong_ts_size))

        # Unicode error in string part
        # Malformed UTF-8 in row_key
        malformed_key_unicode = b"\xff\xffrow1\x00colA\x00" + self.inverted_test_ts_bytes
        self.assertIsNone(self.separator_codec.decode(malformed_key_unicode))
        # Malformed UTF-8 in column_name
        malformed_key_unicode = b"row1\x00colA\xff\xff\x00" + self.inverted_test_ts_bytes
        self.assertIsNone(self.separator_codec.decode(malformed_key_unicode))


    def test_separator_timestamp_inversion(self):
        ts1 = 1678886400000 # March 15, 2023
        ts2 = 1678886460000 # March 15, 2023 + 1 min

        # Keys encoded without dataset_name
        key1 = self.separator_codec.encode(row_key="r", column_name="c", timestamp_ms=ts1)
        key2 = self.separator_codec.encode(row_key="r", column_name="c", timestamp_ms=ts2)

        # Verify that the key with the later timestamp is smaller (comes first in RocksDB iteration)
        self.assertLess(key2, key1)

        decoded1 = self.separator_codec.decode(key1)
        decoded2 = self.separator_codec.decode(key2)

        # Timestamp is the last element in the decoded tuple
        self.assertEqual(decoded1[-1], ts1)
        self.assertEqual(decoded2[-1], ts2)


    # --- Tests for Length-Prefixed KeyCodec (without dataset_name) ---

    def test_length_prefixed_encode_full_key(self):
        key_bytes = self.length_prefixed_codec.encode(
            row_key="row1",
            column_name="colA",
            timestamp_ms=self.test_ts_ms
            # dataset_name is ignored
        )
        # Expected format: [len_row][row_key][len_column][column_name][timestamp]
        expected_bytes = b'\x04row1\x04colA' + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

    def test_length_prefixed_encode_full_key_missing_mandatory_parts(self):
         # row_key is mandatory
         self.assertIsNone(self.length_prefixed_codec.encode(column_name="colA", timestamp_ms=self.test_ts_ms))
         # column_name is mandatory for a full key
         self.assertIsNone(self.length_prefixed_codec.encode(row_key="row1", timestamp_ms=self.test_ts_ms))


    def test_length_prefixed_encode_prefix_column(self):
        prefix_bytes = self.length_prefixed_codec.encode(
            row_key="row1",
            column_name="colA"
            # dataset_name is ignored
        )
         # Expected format: [len_row][row_key][len_column][column_name]
        expected_bytes = b'\x04row1\x04colA'
        self.assertEqual(prefix_bytes, expected_bytes)

    def test_length_prefixed_encode_prefix_row(self):
        prefix_bytes = self.length_prefixed_codec.encode(
            row_key="row1"
            # dataset_name and column_name are None
        )
        # Expected format: [len_row][row_key][len_column=0]
        expected_bytes = b'\x04row1\x00' # Length prefix for column_name (0) should be included
        self.assertEqual(prefix_bytes, expected_bytes)


    def test_length_prefixed_decode_full_key(self):
        # Encoded key without dataset_name
        encoded_key = b'\x04row1\x04colA' + self.inverted_test_ts_bytes
        decoded = self.length_prefixed_codec.decode(encoded_key)
        # Expected decoded format: (row_key, column_name, original_timestamp_ms)
        self.assertEqual(decoded, ("row1", "colA", self.test_ts_ms))

    def test_length_prefixed_decode_malformed(self):
        # Not enough data for length byte
        malformed_key_len_byte = b'\x04row1\x04colA'[:-1] # Cut off last byte of colA len prefix
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key_len_byte))

        # Not enough data for part based on length byte
        malformed_key_part_data = b'\x04row1\x05colA' # len_column is 5, but only 4 bytes follow
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key_part_data))

        # Wrong timestamp size
        malformed_key_wrong_ts_size = b'\x04row1\x04colA' + b'\x00' * 7 # 7 bytes instead of 8
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key_wrong_ts_size))

         # Unicode error within a part
        # Malformed UTF-8 in row_key
        malformed_key_unicode_row = b'\x04ro\xff\xff\x04colA' + self.inverted_test_ts_bytes
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key_unicode_row))
        # Malformed UTF-8 in column_name
        malformed_key_unicode_col = b'\x04row1\x04col\xff\xff' + self.inverted_test_ts_bytes
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key_unicode_col))


    def test_length_prefixed_timestamp_inversion(self):
        ts1 = 1678886400000 # March 15, 2023
        ts2 = 1678886460000 # March 15, 2023 + 1 min

        # Keys encoded without dataset_name
        key1 = self.length_prefixed_codec.encode(row_key="r", column_name="c", timestamp_ms=ts1)
        key2 = self.length_prefixed_codec.encode(row_key="r", column_name="c", timestamp_ms=ts2)

        # Verify that the key with the later timestamp is smaller (comes first in RocksDB iteration)
        self.assertLess(key2, key1)

        decoded1 = self.length_prefixed_codec.decode(key1)
        decoded2 = self.length_prefixed_codec.decode(key2)

        # Timestamp is the last element in the decoded tuple
        self.assertEqual(decoded1[-1], ts1)
        self.assertEqual(decoded2[-1], ts2)


    def test_length_prefixed_encode_long_part(self):
        # A string > 255 bytes
        long_string = "a" * 256
        # Encoding row_key should fail if it's too long
        self.assertIsNone(self.length_prefixed_codec.encode(row_key=long_string, column_name="colA", timestamp_ms=self.test_ts_ms))
         # Encoding column_name should fail if it's too long
        self.assertIsNone(self.length_prefixed_codec.encode(row_key="row1", column_name=long_string, timestamp_ms=self.test_ts_ms))


    def test_length_prefixed_encode_empty_strings(self):
        key_bytes = self.length_prefixed_codec.encode(
            row_key="",
            column_name="",
            timestamp_ms=self.test_ts_ms
        )
        # Expected: [len=0][len=0][timestamp]
        expected_bytes = b'\x00\x00' + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

        decoded = self.length_prefixed_codec.decode(key_bytes)
        # Empty strings encoded as length 0 should decode to None by the _decode_part logic.
        self.assertEqual(decoded, (None, None, self.test_ts_ms))


if __name__ == '__main__':
    unittest.main()
