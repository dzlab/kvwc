import unittest
import time
import shutil
import os
import sys

# Ensure the 'vibecoding' directory (which contains the 'kvwc' package) is in sys.path.
# This allows 'from kvwc import ...' to work correctly when running this test script directly.
# Python will look for 'kvwc' inside the directories listed in sys.path.
#
# Project Structure relevant to this path modification:
# vibecoding/            (This directory needs to be in sys.path)
# ├── kvwc/              (This is the 'kvwc' package)
# │   ├── __init__.py
# │   └── wide_column_db.py
# │   └── pyproject.toml (Defines 'kvwc' as the project, located in this dir)
# └── kvwc/tests/        (The directory containing this test script)
#     └── test_wide_column_db.py
#
# Path calculation:
# __file__ is .../kvwc/tests/test_wide_column_db.py
# SCRIPT_DIR is .../kvwc/tests/
# KVWC_DIR (directory of the 'kvwc' package) is .../kvwc/
# PROJECT_ROOT_DIR (directory containing the 'kvwc' package) is .../
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KVWC_DIR = os.path.dirname(SCRIPT_DIR) # This is kvwc
PROJECT_ROOT_DIR = os.path.dirname(KVWC_DIR) # This is /

if PROJECT_ROOT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_DIR)

from src import WideColumnDB, MAX_UINT64, KEY_SEPARATOR

TEST_DB_PATH_BASE = "test_db_temp_wide_column_main"

class TestWideColumnDB(unittest.TestCase):
    db_path = None
    db = None

    @classmethod
    def setUpClass(cls):
        if os.path.exists(TEST_DB_PATH_BASE):
            shutil.rmtree(TEST_DB_PATH_BASE)
        os.makedirs(TEST_DB_PATH_BASE)

    def setUp(self):
        # Generate a unique DB path for each test method to ensure isolation
        test_method_name = self.id().split('.')[-1]
        self.db_path = os.path.join(TEST_DB_PATH_BASE, test_method_name)

        # Clean up any remnants from a previous failed run for this specific test
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

        self.db = WideColumnDB(self.db_path)
        self.current_time = int(time.time() * 1000)

    def tearDown(self):
        if self.db:
            self.db.close() # self.db.db is set to None here
            self.db = None # Ensure the TestWideColumnDB's own db attribute is None

        if self.db_path and os.path.exists(self.db_path):
            try:
                shutil.rmtree(self.db_path)
            except OSError as e:
                # Handle cases where db might not be fully released, common on Windows
                print(f"Warning: Could not remove {self.db_path} during teardown: {e}")
        self.db_path = None

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TEST_DB_PATH_BASE):
            try:
                shutil.rmtree(TEST_DB_PATH_BASE)
            except OSError as e:
                 print(f"Warning: Could not remove {TEST_DB_PATH_BASE} during class teardown: {e}")

    def test_put_get_single_item(self):
        row_key = "user1"
        col_name = "email"
        value = "user1@example.com"
        ts = self.current_time

        self.db.put_row(row_key, [(col_name, value, ts)])

        result = self.db.get_row(row_key, column_names=[col_name])
        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)
        self.assertEqual(result[col_name][0], (ts, value))

    def test_put_get_multiple_items(self):
        row_key = "product1"
        items = [
            ("name", "Laptop", self.current_time - 100),
            ("price", "1200.00", self.current_time - 50),
            ("category", "Electronics", self.current_time)
        ]
        self.db.put_row(row_key, items)

        result_all = self.db.get_row(row_key)
        self.assertEqual(len(result_all), 3)
        self.assertEqual(result_all["name"][0], (self.current_time - 100, "Laptop"))
        self.assertEqual(result_all["price"][0], (self.current_time - 50, "1200.00"))
        self.assertEqual(result_all["category"][0], (self.current_time, "Electronics"))

        result_specific_cols = self.db.get_row(row_key, column_names=["name", "category"])
        self.assertEqual(len(result_specific_cols), 2)
        self.assertIn("name", result_specific_cols)
        self.assertIn("category", result_specific_cols)
        self.assertNotIn("price", result_specific_cols)

        result_single_col_str = self.db.get_row(row_key, column_names="price")
        self.assertEqual(len(result_single_col_str), 1)
        self.assertIn("price", result_single_col_str)

    def test_put_get_no_timestamp_uses_current(self):
        row_key = "event1"
        col_name = "type"
        value = "login"

        time_before_put = int(time.time() * 1000)
        self.db.put_row(row_key, [(col_name, value)]) # No timestamp provided
        # Add a small sleep to ensure the write is visible before reading
        time.sleep(0.1)
        time_after_put = int(time.time() * 1000)

        result = self.db.get_row(row_key, column_names=[col_name])
        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)

        retrieved_ts, retrieved_value = result[col_name][0]
        self.assertEqual(retrieved_value, value)
        self.assertTrue(time_before_put <= retrieved_ts <= time_after_put + 5, f"Timestamp {retrieved_ts} out of range ({time_before_put}, {time_after_put+5})") # Allow a small delta

    def test_get_num_versions(self):
        row_key = "config_setting"
        col_name = "timeout"
        ts1 = self.current_time - 200
        ts2 = self.current_time - 100
        ts3 = self.current_time

        self.db.put_row(row_key, [(col_name, "10s", ts1)])
        self.db.put_row(row_key, [(col_name, "20s", ts2)])
        self.db.put_row(row_key, [(col_name, "30s", ts3)])

        # Get 1 version (latest)
        result1 = self.db.get_row(row_key, [col_name], num_versions=1)
        self.assertEqual(len(result1[col_name]), 1)
        self.assertEqual(result1[col_name][0], (ts3, "30s"))

        # Get 2 versions
        result2 = self.db.get_row(row_key, [col_name], num_versions=2)
        self.assertEqual(len(result2[col_name]), 2)
        self.assertEqual(result2[col_name][0], (ts3, "30s")) # Newest
        self.assertEqual(result2[col_name][1], (ts2, "20s")) # Second newest

        # Get all 3 versions
        result3 = self.db.get_row(row_key, [col_name], num_versions=3)
        self.assertEqual(len(result3[col_name]), 3)
        self.assertEqual(result3[col_name][0], (ts3, "30s"))
        self.assertEqual(result3[col_name][1], (ts2, "20s"))
        self.assertEqual(result3[col_name][2], (ts1, "10s"))

        # Request more versions than exist
        result_more = self.db.get_row(row_key, [col_name], num_versions=5)
        self.assertEqual(len(result_more[col_name]), 3) # Should return all available

    def test_put_get_with_dataset(self):
        dataset1 = "dset_alpha"
        dataset2 = "dset_beta"
        row_key = "shared_key"
        col_name = "data_val"
        ts = self.current_time

        self.db.put_row(row_key, [(col_name, "alpha_value", ts)], dataset_name=dataset1)
        self.db.put_row(row_key, [(col_name, "beta_value", ts + 1)], dataset_name=dataset2)
        self.db.put_row(row_key, [(col_name, "no_dataset_value", ts + 2)])

        # Get from dataset1
        res_alpha = self.db.get_row(row_key, [col_name], dataset_name=dataset1)
        self.assertEqual(res_alpha[col_name][0], (ts, "alpha_value"))

        # Get from dataset2
        res_beta = self.db.get_row(row_key, [col_name], dataset_name=dataset2)
        self.assertEqual(res_beta[col_name][0], (ts + 1, "beta_value"))

        # Get without dataset (should only get the non-dataset entry)
        res_none = self.db.get_row(row_key, [col_name])
        self.assertEqual(res_none[col_name][0], (ts + 2, "no_dataset_value"))

        # Get ALL columns for row_key in dataset1 (should only be one column)
        res_alpha_all_cols = self.db.get_row(row_key, dataset_name=dataset1)
        self.assertIn(col_name, res_alpha_all_cols)
        self.assertEqual(len(res_alpha_all_cols), 1)

    def test_get_time_range(self):
        row_key = "timeseries_data"
        col_name = "value"
        ts_base = self.current_time
        timestamps = [ts_base - 300, ts_base - 200, ts_base - 100, ts_base]
        values = ["v_oldest", "v_mid_old", "v_mid_new", "v_newest"]

        for i, ts in enumerate(timestamps):
            self.db.put_row(row_key, [(col_name, values[i], ts)])

        # Range including mid_old and mid_new
        result = self.db.get_row(row_key, [col_name],
                                 start_ts_ms=timestamps[1], # ts_base - 200
                                 end_ts_ms=timestamps[2],   # ts_base - 100
                                 num_versions=10)
        self.assertEqual(len(result[col_name]), 2)
        self.assertIn((timestamps[2], values[2]), result[col_name]) # v_mid_new (newer)
        self.assertIn((timestamps[1], values[1]), result[col_name]) # v_mid_old



        # Range: only newest
        result = self.db.get_row(row_key, [col_name], start_ts_ms=timestamps[3], num_versions=10)
        self.assertEqual(len(result[col_name]), 1)
        self.assertEqual(result[col_name][0], (timestamps[3], values[3]))

        # Range: only oldest
        result = self.db.get_row(row_key, [col_name], end_ts_ms=timestamps[0], num_versions=10)
        self.assertEqual(len(result[col_name]), 1)
        self.assertEqual(result[col_name][0], (timestamps[0], values[0]))

        # Range: between oldest and mid_old (exclusive of actual points)
        result = self.db.get_row(row_key, [col_name],
                                 start_ts_ms=timestamps[0] + 1,
                                 end_ts_ms=timestamps[1] - 1,
                                 num_versions=10)
        self.assertNotIn(col_name, result) # Should be empty

        # Range: up to mid_new (inclusive)
        result = self.db.get_row(row_key, [col_name], end_ts_ms=timestamps[2], num_versions=10)
        self.assertEqual(len(result[col_name]), 3) # oldest, mid_old, mid_new
        self.assertIn((timestamps[2], values[2]), result[col_name])
        self.assertIn((timestamps[1], values[1]), result[col_name])
        self.assertIn((timestamps[0], values[0]), result[col_name])

        # Range: from mid_old (inclusive)
        result = self.db.get_row(row_key, [col_name], start_ts_ms=timestamps[1], num_versions=10)
        self.assertEqual(len(result[col_name]), 3) # mid_old, mid_new, newest
        self.assertIn((timestamps[3], values[3]), result[col_name])
        self.assertIn((timestamps[2], values[2]), result[col_name])
        self.assertIn((timestamps[1], values[1]), result[col_name])

    def test_get_non_existent(self):
        result = self.db.get_row("non_existent_row_key")
        self.assertEqual(len(result), 0) # Should be an empty dict

        self.db.put_row("existing_row", [("colA", "valA", self.current_time)])
        result = self.db.get_row("existing_row", ["non_existent_col"])
        self.assertEqual(len(result), 0) # Empty dict as column not found

    def test_delete_specific_columns(self):
        row_key = "doc1"
        ts = self.current_time
        self.db.put_row(row_key, [
            ("title", "My Doc", ts),
            ("author", "Me", ts),
            ("status", "draft", ts)
        ])

        self.db.delete_row(row_key, column_names=["author", "status"])

        result = self.db.get_row(row_key)
        self.assertIn("title", result)
        self.assertNotIn("author", result)
        self.assertNotIn("status", result)
        self.assertEqual(result["title"][0], (ts, "My Doc"))

        # Delete remaining column using single string
        self.db.delete_row(row_key, column_names="title")
        result_final = self.db.get_row(row_key)
        self.assertEqual(len(result_final), 0)

    def test_delete_entire_row(self):
        row_key = "session_abc"
        ts = self.current_time
        self.db.put_row(row_key, [("user_id", "u1", ts), ("ip", "1.2.3.4", ts)])
        self.db.put_row(row_key, [("user_id", "u1_new", ts + 10)]) # Add another version

        self.db.delete_row(row_key) # No columns specified, delete all
        result = self.db.get_row(row_key)
        self.assertEqual(len(result), 0)

    def test_delete_with_dataset(self):
        dataset = "logs"
        row_key = "req123"
        ts = self.current_time
        self.db.put_row(row_key, [("url", "/home", ts)], dataset_name=dataset)
        self.db.put_row(row_key, [("user_agent", "BrowserX", ts)]) # No dataset

        # Delete from dataset "logs"
        self.db.delete_row(row_key, dataset_name=dataset)

        res_dataset = self.db.get_row(row_key, dataset_name=dataset)
        self.assertEqual(len(res_dataset), 0)

        res_no_dataset = self.db.get_row(row_key) # Should still have user_agent
        self.assertIn("user_agent", res_no_dataset)
        self.assertEqual(res_no_dataset["user_agent"][0], (ts, "BrowserX"))

        # Delete the non-dataset entry as well
        self.db.delete_row(row_key, column_names=["user_agent"])
        res_no_dataset_after = self.db.get_row(row_key)
        self.assertEqual(len(res_no_dataset_after), 0)


    def test_delete_specific_timestamps(self):
        row_key = "sensor_temp"
        col_name = "reading"
        ts1, ts2, ts3 = self.current_time - 20, self.current_time - 10, self.current_time

        self.db.put_row(row_key, [(col_name, "20C", ts1)])
        self.db.put_row(row_key, [(col_name, "21C", ts2)])
        self.db.put_row(row_key, [(col_name, "22C", ts3)])

        # Delete the middle version (ts2)
        self.db.delete_row(row_key, column_names=col_name, specific_timestamps_ms=[ts2])

        result = self.db.get_row(row_key, [col_name], num_versions=3)
        self.assertEqual(len(result[col_name]), 2)
        self.assertEqual(result[col_name][0], (ts3, "22C")) # newest
        self.assertEqual(result[col_name][1], (ts1, "20C")) # oldest
        self.assertNotIn((ts2, "21C"), result[col_name])

        # Delete remaining versions
        self.db.delete_row(row_key, column_names=col_name, specific_timestamps_ms=[ts1, ts3])
        result_final = self.db.get_row(row_key, [col_name], num_versions=3)
        self.assertNotIn(col_name, result_final) # Column should be gone

    def test_delete_non_existent_data(self):
        # Try deleting things that aren't there, should not error
        self.db.delete_row("non_row_key")
        self.db.delete_row("non_row_key", column_names=["non_col"])
        self.db.delete_row("non_row_key", column_names="non_col", specific_timestamps_ms=[self.current_time])

        row_key_exists = "real_row_for_delete"
        self.db.put_row(row_key_exists, [("actual_col", "data", self.current_time)])

        # Delete non-existent column in existing row
        self.db.delete_row(row_key_exists, column_names=["fake_col"])
        result = self.db.get_row(row_key_exists)
        self.assertIn("actual_col", result) # Original data should persist

        # Delete non-existent timestamp for existing column
        self.db.delete_row(row_key_exists, column_names="actual_col", specific_timestamps_ms=[self.current_time + 1000])
        result = self.db.get_row(row_key_exists) # Original data should persist
        self.assertEqual(result["actual_col"][0], (self.current_time, "data"))

    def test_key_encoding_order_implicit(self):
        # This is implicitly tested by test_get_num_versions,
        # where results are expected in reverse chronological order.
        # If encoding (inverted timestamp) was wrong, order would be wrong.
        row_key = "implicit_order_test"
        col_name = "val"
        ts_older = self.current_time - 100
        ts_newer = self.current_time
        self.db.put_row(row_key, [(col_name, "older", ts_older)])
        self.db.put_row(row_key, [(col_name, "newer", ts_newer)])

        result = self.db.get_row(row_key, [col_name], num_versions=2)
        self.assertEqual(result[col_name][0], (ts_newer, "newer"))
        self.assertEqual(result[col_name][1], (ts_older, "older"))

    def test_close_method(self):
        db_path_close = os.path.join(TEST_DB_PATH_BASE, "close_test_db_instance")
        if os.path.exists(db_path_close): shutil.rmtree(db_path_close)

        temp_db = WideColumnDB(db_path_close)
        temp_db.put_row("r1", [("c1", "v1", self.current_time)])

        temp_db.close()
        self.assertIsNone(temp_db.db) # Internal RocksDB instance should be None

        with self.assertRaises(AttributeError): # 'NoneType' object has no attribute 'write'
            temp_db.put_row("r2", [("c2", "v2", self.current_time + 1)])

        # Ensure the directory is cleaned up if not handled by instance tearDown
        if os.path.exists(db_path_close):
            shutil.rmtree(db_path_close)

    def test_empty_strings_as_keys_values(self):
        ts = self.current_time
        # Empty column name
        self.db.put_row("row_empty_col", [("", "val_for_empty_col", ts)])
        res1 = self.db.get_row("row_empty_col", [""])
        self.assertIn("", res1)
        self.assertEqual(res1[""][0], (ts, "val_for_empty_col"))

        # Empty value
        self.db.put_row("row_empty_val", [("col_for_empty_val", "", ts + 1)])
        res2 = self.db.get_row("row_empty_val", ["col_for_empty_val"])
        self.assertIn("col_for_empty_val", res2)
        self.assertEqual(res2["col_for_empty_val"][0], (ts + 1, ""))

        # Empty row key
        self.db.put_row("", [("col_in_empty_row", "val_in_empty_row", ts + 2)])
        res3 = self.db.get_row("", ["col_in_empty_row"])
        self.assertIn("col_in_empty_row", res3)
        self.assertEqual(res3["col_in_empty_row"][0], (ts + 2, "val_in_empty_row"))

    def test_get_row_on_completely_empty_db(self):
        # Create a db instance but don't call setUp which might add data for other tests
        empty_db_path = os.path.join(TEST_DB_PATH_BASE, "totally_empty_db")
        if os.path.exists(empty_db_path): shutil.rmtree(empty_db_path)

        empty_db_instance = WideColumnDB(empty_db_path)
        try:
            result = empty_db_instance.get_row("any_key_whatsoever")
            self.assertEqual(result, {})
        finally:
            empty_db_instance.close()
            if os.path.exists(empty_db_path):
                shutil.rmtree(empty_db_path)

    def test_delete_row_triggers_no_write_if_nothing_deleted(self):
        # This test checks the `if len(list(batch.iterate())) > 0:` condition
        # by attempting to delete data that doesn't exist.
        # No actual write to RocksDB should occur.
        # We can't directly check if db.write was called, but we can ensure no errors.
        self.db.delete_row("no_such_row_for_batch_check") # Should not error

        # Check DB is still usable
        self.db.put_row("after_empty_delete", [("c","v", self.current_time)])
        res = self.db.get_row("after_empty_delete")
        self.assertIn("c", res)
        self.assertEqual(res["c"][0], (self.current_time, "v"))

    def test_get_row_multiple_cols_varied_versions(self):
        row_key = "multi_col_versions"
        ts = self.current_time
        self.db.put_row(row_key, [("colA", "A_v1", ts - 20)])
        self.db.put_row(row_key, [("colA", "A_v2", ts - 10)])
        self.db.put_row(row_key, [("colA", "A_v3", ts)])

        self.db.put_row(row_key, [("colB", "B_v1", ts - 5)])

        self.db.put_row(row_key, [("colC", "C_v1", ts - 15)])
        self.db.put_row(row_key, [("colC", "C_v2", ts - 3)])

        # Request 2 versions for colA and colB
        result = self.db.get_row(row_key, column_names=["colA", "colB"], num_versions=2)

        self.assertIn("colA", result)
        self.assertEqual(len(result["colA"]), 2)
        self.assertEqual(result["colA"][0], (ts, "A_v3"))
        self.assertEqual(result["colA"][1], (ts - 10, "A_v2"))

        self.assertIn("colB", result)
        self.assertEqual(len(result["colB"]), 1) # Only 1 version exists for colB
        self.assertEqual(result["colB"][0], (ts - 5, "B_v1"))

        self.assertNotIn("colC", result) # colC was not requested

        # Request all columns, default num_versions=1
        result_all_latest = self.db.get_row(row_key)
        self.assertEqual(len(result_all_latest["colA"]), 1)
        self.assertEqual(result_all_latest["colA"][0], (ts, "A_v3"))
        self.assertEqual(len(result_all_latest["colB"]), 1)
        self.assertEqual(result_all_latest["colB"][0], (ts - 5, "B_v1"))
        self.assertEqual(len(result_all_latest["colC"]), 1)
        self.assertEqual(result_all_latest["colC"][0], (ts - 3, "C_v2"))

    def test_put_row_empty_items_list(self):
        row_key = "row_for_empty_put"
        self.db.put_row(row_key, []) # Put with an empty list of items

        result = self.db.get_row(row_key)
        self.assertEqual(result, {}) # Expect no data for this row

        # Ensure subsequent operations are fine
        self.db.put_row(row_key, [("col1", "val1", self.current_time)])
        result_after = self.db.get_row(row_key)
        self.assertIn("col1", result_after)
        self.assertEqual(result_after["col1"][0], (self.current_time, "val1"))

    def test_malformed_keys_during_decode(self):
        # This test is a bit more internal, trying to simulate malformed keys
        # that _decode_key might encounter. It's hard to *perfectly* simulate without
        # direct DB manipulation, but we can try via _encode_key.

        # A key that might be too short if split by KEY_SEPARATOR
        # Manually craft a key that would lead to insufficient parts
        raw_key_too_short = b"rowkey" + KEY_SEPARATOR + b"colname"
        # This key is missing the timestamp part
        self.db.db.put(raw_key_too_short, b"value")

        # When get_row scans, it should ideally skip this malformed key.
        # We put a valid key nearby to ensure scan continues.
        self.db.put_row("rowkey", [("colname_valid", "v", self.current_time)])

        # Attempt to get data for "rowkey". If _decode_key handles errors gracefully,
        # it should skip the malformed one and return the valid one.
        # Note: RocksDB orders keys lexicographically. "rowkey\x00colname" comes before
        # "rowkey\x00colname_valid\x00<timestamp>" if colname < colname_valid.
        # If colname is "z_malformed", it might come after. Let's use a name that comes first.

        # Better approach: directly test _decode_key if it were public, or trust current coverage.
        # The current test is more of an integration check for robustness.

        # The `_decode_key` returns None for malformed keys, and get_row has `if not decoded: continue`
        # So, the main test is that it doesn't crash and valid data is still retrieved.
        res = self.db.get_row("rowkey", ["colname_valid"])
        self.assertIn("colname_valid", res)
        self.assertEqual(res["colname_valid"][0], (self.current_time, "v"))

        # Test with dataset name expected but key is too short
        raw_key_dataset_too_short = b"dataset" + KEY_SEPARATOR + b"row" + KEY_SEPARATOR + b"col"
        self.db.db.put(raw_key_dataset_too_short, b"value_ds_short")
        self.db.put_row("row", [("col_valid_ds", "val_ds", self.current_time)], dataset_name="dataset")

        res_ds = self.db.get_row("row", ["col_valid_ds"], dataset_name="dataset")
        self.assertIn("col_valid_ds", res_ds)
        self.assertEqual(res_ds["col_valid_ds"][0], (self.current_time, "val_ds"))

        # Test with key that has incorrect timestamp bytes (not 8 bytes)
        # struct.error in _decode_key
        invalid_ts_bytes = b"short"
        key_bad_ts = b"row_bad_ts" + KEY_SEPARATOR + b"col_bad_ts" + KEY_SEPARATOR + invalid_ts_bytes
        self.db.db.put(key_bad_ts, b"value_bad_ts")
        self.db.put_row("row_bad_ts", [("col_good_ts", "val_good_ts", self.current_time)])

        res_bad_ts = self.db.get_row("row_bad_ts", ["col_good_ts"])
        self.assertIn("col_good_ts", res_bad_ts)
        self.assertEqual(res_bad_ts["col_good_ts"][0], (self.current_time, "val_good_ts"))


if __name__ == '__main__':
    unittest.main()
