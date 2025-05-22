import unittest
import time
import struct
from src import KeyCodec, LengthPrefixedKeyCodec

class TestKeyCodecs(unittest.TestCase):

    def setUp(self):
        """Set up a KeyCodec instance for each test."""
        self.separator_codec = KeyCodec()
        self.length_prefixed_codec = LengthPrefixedKeyCodec()
        self.test_ts_ms = int(time.time() * 1000)
        self.inverted_test_ts_bytes = struct.pack('>Q', KeyCodec.MAX_UINT64 - self.test_ts_ms)


    # --- Tests for Separator-Based KeyCodec ---

    def test_separator_encode_full_key(self):
        key_bytes = self.separator_codec.encode(
            dataset_name="my_dataset",
            row_key="row1",
            column_name="colA",
            timestamp_ms=self.test_ts_ms
        )
        expected_bytes = b"my_dataset\x00row1\x00colA\x00" + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

    def test_separator_encode_full_key_no_dataset(self):
        key_bytes = self.separator_codec.encode(
            row_key="row1",
            column_name="colA",
            timestamp_ms=self.test_ts_ms
        )
        expected_bytes = b"row1\x00colA\x00" + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

    def test_separator_encode_prefix_column(self):
        prefix_bytes = self.separator_codec.encode(
            dataset_name="my_dataset",
            row_key="row1",
            column_name="colA"
        )
        expected_bytes = b"my_dataset\x00row1\x00colA\x00"
        self.assertEqual(prefix_bytes, expected_bytes)

    def test_separator_encode_prefix_row(self):
        prefix_bytes = self.separator_codec.encode(
            dataset_name="my_dataset",
            row_key="row1"
        )
        expected_bytes = b"my_dataset\x00row1\x00"
        self.assertEqual(prefix_bytes, expected_bytes)

    def test_separator_decode_full_key(self):
        encoded_key = b"my_dataset\x00row1\x00colA\x00" + self.inverted_test_ts_bytes
        decoded = self.separator_codec.decode(encoded_key)
        self.assertEqual(decoded, ("my_dataset", "row1", "colA", self.test_ts_ms))

    def test_separator_decode_full_key_no_dataset(self):
        encoded_key = b"row1\x00colA\x00" + self.inverted_test_ts_bytes
        decoded = self.separator_codec.decode(encoded_key)
        self.assertEqual(decoded, (None, "row1", "colA", self.test_ts_ms))

    def test_separator_decode_malformed(self):
        # Missing parts
        malformed_key = b"row1\x00colA"
        self.assertIsNone(self.separator_codec.decode(malformed_key))

        # Wrong timestamp size
        malformed_key = b"row1\x00colA\x00" + b"\x00" * 7 # 7 bytes instead of 8
        self.assertIsNone(self.separator_codec.decode(malformed_key))

        # Unicode error
        malformed_key = b"row1\x00colA\x00\xff\xff" + self.inverted_test_ts_bytes # Invalid UTF-8
        self.assertIsNone(self.separator_codec.decode(malformed_key))

    def test_separator_timestamp_inversion(self):
        ts1 = 1678886400000 # March 15, 2023
        ts2 = 1678886460000 # March 15, 2023 + 1 min

        key1 = self.separator_codec.encode(row_key="r", column_name="c", timestamp_ms=ts1)
        key2 = self.separator_codec.encode(row_key="r", column_name="c", timestamp_ms=ts2)

        # Verify that the key with the later timestamp is smaller (comes first in RocksDB iteration)
        self.assertLess(key2, key1)

        decoded1 = self.separator_codec.decode(key1)
        decoded2 = self.separator_codec.decode(key2)

        self.assertEqual(decoded1[-1], ts1)
        self.assertEqual(decoded2[-1], ts2)


    # --- Tests for Length-Prefixed KeyCodec ---

    def test_length_prefixed_encode_full_key(self):
        key_bytes = self.length_prefixed_codec.encode(
            dataset_name="my_dataset",
            row_key="row1",
            column_name="colA",
            timestamp_ms=self.test_ts_ms
        )
        # Expected: [len_dataset][dataset_name][len_row][row_key][len_column][column_name][timestamp]
        expected_bytes = b'\x0amy_dataset\x04row1\x04colA' + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

    def test_length_prefixed_encode_full_key_no_dataset(self):
        key_bytes = self.length_prefixed_codec.encode(
            dataset_name=None, # Explicitly None
            row_key="row1",
            column_name="colA",
            timestamp_ms=self.test_ts_ms
        )
         # Expected: [len_dataset=0][len_row][row_key][len_column][column_name][timestamp]
        expected_bytes = b'\x00\x04row1\x04colA' + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

    def test_length_prefixed_encode_prefix_column(self):
        prefix_bytes = self.length_prefixed_codec.encode(
            dataset_name="my_dataset",
            row_key="row1",
            column_name="colA"
        )
         # Expected: [len_dataset][dataset_name][len_row][row_key][len_column][column_name]
        expected_bytes = b'\x0amy_dataset\x04row1\x04colA'
        self.assertEqual(prefix_bytes, expected_bytes)

    def test_length_prefixed_encode_prefix_row(self):
        prefix_bytes = self.length_prefixed_codec.encode(
            dataset_name="my_dataset",
            row_key="row1"
        )
        # Expected: [len_dataset][dataset_name][len_row][row_key][len_column=0]
        expected_bytes = b'\x0amy_dataset\x04row1\x00'
        self.assertEqual(prefix_bytes, expected_bytes)

    def test_length_prefixed_decode_full_key(self):
        encoded_key = b'\x0amy_dataset\x04row1\x04colA' + self.inverted_test_ts_bytes
        decoded = self.length_prefixed_codec.decode(encoded_key)
        self.assertEqual(decoded, ("my_dataset", "row1", "colA", self.test_ts_ms))

    def test_length_prefixed_decode_full_key_no_dataset(self):
        encoded_key = b'\x00\x04row1\x04colA' + self.inverted_test_ts_bytes
        decoded = self.length_prefixed_codec.decode(encoded_key)
        self.assertEqual(decoded, (None, "row1", "colA", self.test_ts_ms))

    def test_length_prefixed_decode_malformed(self):
        # Not enough data for length byte
        malformed_key = b'\x0amy_dataset\x04row1\x04colA'[:-1] # Cut off last byte
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key))

        # Not enough data for part based on length byte
        malformed_key = b'\x0amy_dataset\x04row1\x04colA' + b'\x05' + b'abc' # Declares 5 bytes, provides 3
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key))

        # Wrong timestamp size
        malformed_key = b'\x0amy_dataset\x04row1\x04colA' + b'\x00' * 7 # 7 bytes instead of 8
        self.assertIsNone(self.length_prefixed_codec.decode(malformed_key))

         # Unicode error within a part
        malformed_key = b'\x0amy_dataset\x04row1\x04colA' + b'\x02\xff\xff' + self.inverted_test_ts_bytes # Invalid UTF-8 in dataset part
        self.assertIsNone(self.length_prefixed_codec.decode(b'\x0amy_datas\xff\xff\x04row1\x04colA' + self.inverted_test_ts_bytes))

    def test_length_prefixed_timestamp_inversion(self):
        ts1 = 1678886400000 # March 15, 2023
        ts2 = 1678886460000 # March 15, 2023 + 1 min

        key1 = self.length_prefixed_codec.encode(row_key="r", column_name="c", timestamp_ms=ts1)
        key2 = self.length_prefixed_codec.encode(row_key="r", column_name="c", timestamp_ms=ts2)

        # Verify that the key with the later timestamp is smaller (comes first in RocksDB iteration)
        self.assertLess(key2, key1)

        decoded1 = self.length_prefixed_codec.decode(key1)
        decoded2 = self.length_prefixed_codec.decode(key2)

        self.assertEqual(decoded1[-1], ts1)
        self.assertEqual(decoded2[-1], ts2)

    def test_length_prefixed_encode_long_part(self):
        # A string > 255 bytes
        long_string = "a" * 256
        self.assertIsNone(self.length_prefixed_codec.encode(row_key=long_string, column_name="colA"))

    def test_length_prefixed_encode_empty_strings(self):
        key_bytes = self.length_prefixed_codec.encode(
            dataset_name="",
            row_key="",
            column_name="",
            timestamp_ms=self.test_ts_ms
        )
        # Expected: [len=0][len=0][len=0][timestamp]
        expected_bytes = b'\x00\x00\x00' + self.inverted_test_ts_bytes
        self.assertEqual(key_bytes, expected_bytes)

        decoded = self.length_prefixed_codec.decode(key_bytes)
        # Empty strings should decode to None by the _decode_part logic, which seems reasonable.
        self.assertEqual(decoded, (None, None, None, self.test_ts_ms))

if __name__ == '__main__':
    unittest.main()
