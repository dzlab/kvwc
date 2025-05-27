import unittest
import rocksdict as rocksdb
import os
import shutil
import time
from src.wide_column_db import WideColumnDB
from src.key_codec import KeyCodec
from src.length_prefixed_key_codec import LengthPrefixedKeyCodec
from src.serializer import StrSerializer, PickleSerializer, JsonSerializer, MsgPackSerializer # Import all serializers
import logging

# Configure basic logging for tests
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestWideColumnDB(unittest.TestCase):
    TEST_DB_PATH = "test_rocksdb_cf"
    # Define specific column families for testing
    TEST_CFS = ["dataset1", "dataset2", "another_cf"]

    @classmethod
    def setUpClass(cls):
        """Set up the test database directory once for all tests."""
        if os.path.exists(cls.TEST_DB_PATH):
            shutil.rmtree(cls.TEST_DB_PATH)
        # Database creation is now handled by RocksDBManager within WideColumnDB

    @classmethod
    def tearDownClass(cls):
        """Clean up the test database directory once after all tests."""
        if os.path.exists(cls.TEST_DB_PATH):
            shutil.rmtree(cls.TEST_DB_PATH)

    def setUp(self):
        """Create a new WideColumnDB instance for each test with specific CFs."""
        # Ensure the directory is clean before each test
        if os.path.exists(self.TEST_DB_PATH):
            shutil.rmtree(self.TEST_DB_PATH)
        # Initialize WideColumnDB with the test Column Families
        self.db = WideColumnDB(self.TEST_DB_PATH, column_families=self.TEST_CFS)
        # Use default codecs and serializers unless a test specifies otherwise

    def tearDown(self):
        """Close the database after each test."""
        if self.db:
            self.db.close()

    def test_put_get_single_item_default_cf(self):
        """Test putting and getting a single item in the default CF."""
        row_key = "row1"
        col_name = "colA"
        value = "value1"
        ts = int(time.time() * 1000)

        # Put without dataset_name (uses default CF)
        self.db.put_row(row_key, [(col_name, value, ts)])

        # Get without dataset_name (reads from default CF)
        result = self.db.get_row(row_key, column_names=col_name)

        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)
        self.assertEqual(result[col_name][0], (ts, value))

    def test_put_get_single_item_specific_cf(self):
        """Test putting and getting a single item in a specific CF."""
        row_key = "row1"
        col_name = "colA"
        value = "value1"
        ts = int(time.time() * 1000)
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        # Put with dataset_name
        self.db.put_row(row_key, [(col_name, value, ts)], dataset_name=dataset_name)

        # Get from the specific dataset_name
        result = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name)

        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)
        self.assertEqual(result[col_name][0], (ts, value))

        # Verify that getting from a *different* CF or default doesn't return the data
        result_default = self.db.get_row(row_key, column_names=col_name, dataset_name=None)
        self.assertNotIn(col_name, result_default)

        result_other_cf = self.db.get_row(row_key, column_names=col_name, dataset_name=self.TEST_CFS[1])
        self.assertNotIn(col_name, result_other_cf)


    def test_put_get_multiple_items_same_row_diff_cols_diff_cfs(self):
        """Test putting multiple items in the same row across different columns and CFs."""
        row_key = "row1"
        ts = int(time.time() * 1000)

        # Put items in default CF
        self.db.put_row(row_key, [("colA", "valA_default", ts)], dataset_name=None)
        self.db.put_row(row_key, [("colB", "valB_default", ts)], dataset_name=None)

        # Put items in dataset1 CF
        self.db.put_row(row_key, [("colA", "valA_dataset1", ts)], dataset_name=self.TEST_CFS[0])
        self.db.put_row(row_key, [("colC", "valC_dataset1", ts)], dataset_name=self.TEST_CFS[0])

        # Get from default CF - should only get colA and colB from default
        result_default = self.db.get_row(row_key)
        self.assertIn("colA", result_default)
        self.assertIn("colB", result_default)
        self.assertNotIn("colC", result_default)
        self.assertEqual(result_default["colA"][0], (ts, "valA_default"))
        self.assertEqual(result_default["colB"][0], (ts, "valB_default"))


        # Get from dataset1 CF - should only get colA and colC from dataset1
        result_dataset1 = self.db.get_row(row_key, dataset_name=self.TEST_CFS[0])
        self.assertIn("colA", result_dataset1)
        self.assertIn("colC", result_dataset1)
        self.assertNotIn("colB", result_dataset1)
        self.assertEqual(result_dataset1["colA"][0], (ts, "valA_dataset1"))
        self.assertEqual(result_dataset1["colC"][0], (ts, "valC_dataset1"))

        # Get from another_cf - should be empty
        result_another = self.db.get_row(row_key, dataset_name=self.TEST_CFS[2])
        self.assertEqual(result_another, {})


    def test_put_get_no_timestamp_uses_current_default_cf(self):
        """Test put with no timestamp uses current time in default CF."""
        row_key = "row_ts"
        col_name = "col_ts"
        value = "value_ts"

        # Capture start time
        start_ts = int(time.time() * 1000)
        # Put without timestamp (uses current time)
        self.db.put_row(row_key, [(col_name, value)])
        # Capture end time
        end_ts = int(time.time() * 1000)

        result = self.db.get_row(row_key, column_names=col_name)

        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)
        retrieved_ts, retrieved_value = result[col_name][0]

        self.assertEqual(retrieved_value, value)
        # Check if the timestamp is within the expected range
        self.assertTrue(start_ts <= retrieved_ts <= end_ts, f"Timestamp {retrieved_ts} not between {start_ts} and {end_ts}")

    def test_get_num_versions_specific_cf(self):
        """Test retrieving multiple versions from a specific CF."""
        row_key = "row_versions"
        col_name = "col_v"
        dataset_name = self.TEST_CFS[1] # Use 'dataset2'

        ts1 = int(time.time() * 1000) - 10000 # Older
        ts2 = int(time.time() * 1000) - 5000  # Newer
        ts3 = int(time.time() * 1000)          # Newest

        # Put multiple versions with different timestamps in dataset2
        self.db.put_row(row_key, [(col_name, "value1", ts1)], dataset_name=dataset_name)
        self.db.put_row(row_key, [(col_name, "value2", ts2)], dataset_name=dataset_name)
        self.db.put_row(row_key, [(col_name, "value3", ts3)], dataset_name=dataset_name)

        # Put some data in default CF for the same row/column to ensure isolation
        self.db.put_row(row_key, [(col_name, "value_default", ts3)], dataset_name=None)


        # Get 2 versions from dataset2
        result_dataset2_v2 = self.db.get_row(row_key, column_names=col_name, num_versions=2, dataset_name=dataset_name)
        self.assertIn(col_name, result_dataset2_v2)
        self.assertEqual(len(result_dataset2_v2[col_name]), 2)
        # Results should be sorted by timestamp descending (newest first)
        self.assertEqual(result_dataset2_v2[col_name][0], (ts3, "value3"))
        self.assertEqual(result_dataset2_v2[col_name][1], (ts2, "value2"))

        # Get 3 versions from dataset2
        result_dataset2_v3 = self.db.get_row(row_key, column_names=col_name, num_versions=3, dataset_name=dataset_name)
        self.assertIn(col_name, result_dataset2_v3)
        self.assertEqual(len(result_dataset2_v3[col_name]), 3)
        self.assertEqual(result_dataset2_v3[col_name][0], (ts3, "value3"))
        self.assertEqual(result_dataset2_v3[col_name][1], (ts2, "value2"))
        self.assertEqual(result_dataset2_v3[col_name][2], (ts1, "value1"))

         # Get 1 version from default CF - should only get the default value
        result_default_v1 = self.db.get_row(row_key, column_names=col_name, num_versions=1, dataset_name=None)
        self.assertIn(col_name, result_default_v1)
        self.assertEqual(len(result_default_v1[col_name]), 1)
        self.assertEqual(result_default_v1[col_name][0], (ts3, "value_default"))


    def test_get_time_range_specific_cf(self):
        """Test retrieving data within a time range from a specific CF."""
        row_key = "row_time_range"
        col_name = "col_tr"
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        ts_early = int(time.time() * 1000) - 20000
        ts_middle = int(time.time() * 1000) - 10000
        ts_late = int(time.time() * 1000) - 5000
        ts_latest = int(time.time() * 1000)

        # Put data in dataset1
        self.db.put_row(row_key, [(col_name, "early", ts_early)], dataset_name=dataset_name)
        self.db.put_row(row_key, [(col_name, "middle", ts_middle)], dataset_name=dataset_name)
        self.db.put_row(row_key, [(col_name, "late", ts_late)], dataset_name=dataset_name)
        self.db.put_row(row_key, [(col_name, "latest", ts_latest)], dataset_name=dataset_name)

        # Put data in default CF with same keys/timestamps to ensure isolation
        self.db.put_row(row_key, [(col_name, "early_def", ts_early)], dataset_name=None)
        self.db.put_row(row_key, [(col_name, "latest_def", ts_latest)], dataset_name=None)


        # Get from dataset1 within a time range (inclusive)
        # Range: ts_middle to ts_latest
        result = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name,
                                start_ts_ms=ts_middle, end_ts_ms=ts_latest)

        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 3)
        # Results should be sorted descending by timestamp
        self.assertEqual(result[col_name][0], (ts_latest, "latest"))
        self.assertEqual(result[col_name][1], (ts_late, "late"))
        self.assertEqual(result[col_name][2], (ts_middle, "middle"))

        # Get from dataset1 with only start time
        result_start_only = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name,
                                            start_ts_ms=ts_late)
        self.assertIn(col_name, result_start_only)
        self.assertEqual(len(result_start_only[col_name]), 2)
        self.assertEqual(result_start_only[col_name][0], (ts_latest, "latest"))
        self.assertEqual(result_start_only[col_name][1], (ts_late, "late"))

        # Get from dataset1 with only end time
        result_end_only = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name,
                                          end_ts_ms=ts_middle)
        self.assertIn(col_name, result_end_only)
        self.assertEqual(len(result_end_only[col_name]), 2)
        self.assertEqual(result_end_only[col_name][0], (ts_middle, "middle")) # Newest first in range
        self.assertEqual(result_end_only[col_name][1], (ts_early, "early"))

        # Get from default CF with same time range - should only get default values
        result_default_range = self.db.get_row(row_key, column_names=col_name, dataset_name=None,
                                                start_ts_ms=ts_middle, end_ts_ms=ts_latest)
        # Default CF only had ts_latest in this range
        self.assertIn(col_name, result_default_range)
        self.assertEqual(len(result_default_range[col_name]), 1)
        self.assertEqual(result_default_range[col_name][0], (ts_latest, "latest_def"))


    def test_get_non_existent_row_or_column_specific_cf(self):
        """Test getting non-existent data from a specific CF."""
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'
        # Put some data first in another CF to ensure DB is not entirely empty
        self.db.put_row("existing_row", [("existing_col", "value", int(time.time()*1000))], dataset_name=self.TEST_CFS[1])

        # Get non-existent row from dataset1
        result_row = self.db.get_row("non_existent_row", dataset_name=dataset_name)
        self.assertEqual(result_row, {})

        # Get non-existent column from an existing row in dataset1
        self.db.put_row("row_with_data", [("colA", "valA", int(time.time()*1000))], dataset_name=dataset_name)
        result_col = self.db.get_row("row_with_data", column_names="non_existent_col", dataset_name=dataset_name)
        self.assertEqual(result_col, {})

        # Get non-existent row from default CF
        result_row_default = self.db.get_row("non_existent_row_default", dataset_name=None)
        self.assertEqual(result_row_default, {})


    def test_delete_specific_columns_specific_cf(self):
        """Test deleting specific columns in a specific CF."""
        row_key = "row_delete_cols"
        ts = int(time.time() * 1000)
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        # Put data in dataset1 for row_delete_cols
        self.db.put_row(row_key, [
            ("colA", "valA1", ts - 1000),
            ("colA", "valA2", ts),
            ("colB", "valB1", ts),
            ("colC", "valC1", ts)
        ], dataset_name=dataset_name)

         # Put data in default CF for the same row/cols to ensure isolation
        self.db.put_row(row_key, [
            ("colA", "valA_def", ts),
            ("colB", "valB_def", ts)
        ], dataset_name=None)

        # Verify initial state in dataset1
        initial_data_dataset1 = self.db.get_row(row_key, dataset_name=dataset_name, num_versions=2)
        self.assertIn("colA", initial_data_dataset1)
        self.assertEqual(len(initial_data_dataset1["colA"]), 2)
        self.assertIn("colB", initial_data_dataset1)
        self.assertEqual(len(initial_data_dataset1["colB"]), 1)
        self.assertIn("colC", initial_data_dataset1)
        self.assertEqual(len(initial_data_dataset1["colC"]), 1)

        # Verify initial state in default CF
        initial_data_default = self.db.get_row(row_key, dataset_name=None)
        self.assertIn("colA", initial_data_default)
        self.assertIn("colB", initial_data_default)
        self.assertNotIn("colC", initial_data_default)


        # Delete colA and colC from dataset1
        self.db.delete_row(row_key, column_names=["colA", "colC"], dataset_name=dataset_name)

        # Verify state in dataset1 after deletion
        after_delete_dataset1 = self.db.get_row(row_key, dataset_name=dataset_name, num_versions=2)
        self.assertNotIn("colA", after_delete_dataset1) # colA should be deleted
        self.assertIn("colB", after_delete_dataset1) # colB should remain
        self.assertNotIn("colC", after_delete_dataset1) # colC should be deleted

        # Verify state in default CF (should be unaffected)
        after_delete_default = self.db.get_row(row_key, dataset_name=None)
        self.assertIn("colA", after_delete_default)
        self.assertIn("colB", after_delete_default)


    def test_delete_entire_row_specific_cf(self):
        """Test deleting an entire row in a specific CF."""
        row_key = "row_delete_entire"
        ts = int(time.time() * 1000)
        dataset_name = self.TEST_CFS[1] # Use 'dataset2'

        # Put data in dataset2 for row_delete_entire
        self.db.put_row(row_key, [
            ("colA", "valA1", ts),
            ("colB", "valB1", ts)
        ], dataset_name=dataset_name)

        # Put data in default CF for the same row to ensure isolation
        self.db.put_row(row_key, [("colA", "valA_def", ts)], dataset_name=None)

        # Verify initial state in dataset2
        initial_data_dataset2 = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertIn("colA", initial_data_dataset2)
        self.assertIn("colB", initial_data_dataset2)

        # Verify initial state in default CF
        initial_data_default = self.db.get_row(row_key, dataset_name=None)
        self.assertIn("colA", initial_data_default)

        # Delete the entire row in dataset2
        self.db.delete_row(row_key, dataset_name=dataset_name)

        # Verify state in dataset2 after deletion
        after_delete_dataset2 = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertEqual(after_delete_dataset2, {}) # Row should be empty in dataset2

        # Verify state in default CF (should be unaffected)
        after_delete_default = self.db.get_row(row_key, dataset_name=None)
        self.assertIn("colA", after_delete_default)


    def test_delete_with_dataset_isolation(self):
        """Test deleting in one dataset does not affect another."""
        row_key = "row_delete_isolated"
        ts = int(time.time() * 1000)
        dataset1_name = self.TEST_CFS[0]
        dataset2_name = self.TEST_CFS[1]

        # Put data in both datasets for the same row/columns
        self.db.put_row(row_key, [("colA", "valA1", ts), ("colB", "valB1", ts)], dataset_name=dataset1_name)
        self.db.put_row(row_key, [("colA", "valA2", ts), ("colB", "valB2", ts)], dataset_name=dataset2_name)

        # Verify initial state
        data_dataset1_before = self.db.get_row(row_key, dataset_name=dataset1_name)
        data_dataset2_before = self.db.get_row(row_key, dataset_name=dataset2_name)
        self.assertIn("colA", data_dataset1_before)
        self.assertIn("colB", data_dataset1_before)
        self.assertIn("colA", data_dataset2_before)
        self.assertIn("colB", data_dataset2_before)

        # Delete row from dataset1
        self.db.delete_row(row_key, dataset_name=dataset1_name)

        # Verify dataset1 is empty for this row
        data_dataset1_after = self.db.get_row(row_key, dataset_name=dataset1_name)
        self.assertEqual(data_dataset1_after, {})

        # Verify dataset2 is unaffected
        data_dataset2_after = self.db.get_row(row_key, dataset_name=dataset2_name)
        self.assertIn("colA", data_dataset2_after)
        self.assertIn("colB", data_dataset2_after)


    def test_delete_specific_timestamps_specific_cf(self):
        """Test deleting specific timestamp versions for a column in a specific CF."""
        row_key = "row_delete_ts"
        col_name = "col_ts_del"
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        ts1 = int(time.time() * 1000) - 20000
        ts2 = int(time.time() * 1000) - 10000
        ts3 = int(time.time() * 1000) - 5000
        ts4 = int(time.time() * 1000) # Newest

        # Put multiple versions in dataset1
        self.db.put_row(row_key, [
            (col_name, "val1", ts1),
            (col_name, "val2", ts2),
            (col_name, "val3", ts3),
            (col_name, "val4", ts4)
        ], dataset_name=dataset_name)

        # Put similar data in default CF to ensure isolation
        self.db.put_row(row_key, [
             (col_name, "val1_def", ts1),
             (col_name, "val4_def", ts4)
        ], dataset_name=None)


        # Verify initial state in dataset1
        initial_data_dataset1 = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name, num_versions=4)
        self.assertEqual(len(initial_data_dataset1.get(col_name, [])), 4)

         # Verify initial state in default CF
        initial_data_default = self.db.get_row(row_key, column_names=col_name, dataset_name=None, num_versions=2)
        self.assertEqual(len(initial_data_default.get(col_name, [])), 2)


        # Delete specific timestamps (ts2 and ts4) from dataset1
        self.db.delete_row(row_key, column_names=col_name, specific_timestamps_ms=[ts2, ts4], dataset_name=dataset_name)

        # Verify state in dataset1 after deletion
        after_delete_dataset1 = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name, num_versions=4)
        self.assertIn(col_name, after_delete_dataset1)
        self.assertEqual(len(after_delete_dataset1[col_name]), 2)
        # The remaining versions should be ts3 and ts1 (newest first)
        self.assertEqual(after_delete_dataset1[col_name][0], (ts3, "val3"))
        self.assertEqual(after_delete_dataset1[col_name][1], (ts1, "val1"))

        # Verify state in default CF (should be unaffected)
        after_delete_default = self.db.get_row(row_key, column_names=col_name, dataset_name=None, num_versions=2)
        self.assertEqual(len(after_delete_default.get(col_name, [])), 2)
        self.assertEqual(after_delete_default[col_name][0], (ts4, "val4_def"))
        self.assertEqual(after_delete_default[col_name][1], (ts1, "val1_def"))


    def test_delete_non_existent_data_specific_cf(self):
        """Test deleting non-existent data does not raise errors in a specific CF."""
        row_key = "row_delete_non_existent"
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        # Ensure the row/dataset combination is initially empty
        initial_state = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertEqual(initial_state, {})

        # Attempt to delete non-existent column
        try:
            self.db.delete_row(row_key, column_names="non_existent_col", dataset_name=dataset_name)
            # If no exception is raised, the test passes for this part
            delete_col_success = True
        except Exception as e:
            logger.error(f"Deleting non-existent column raised exception: {e}")
            delete_col_success = False
        self.assertTrue(delete_col_success, "Deleting non-existent column should not raise an error.")

        # Attempt to delete non-existent specific timestamps
        try:
            self.db.delete_row(row_key, column_names="some_col", specific_timestamps_ms=[123, 456], dataset_name=dataset_name)
            delete_ts_success = True
        except Exception as e:
            logger.error(f"Deleting non-existent timestamps raised exception: {e}")
            delete_ts_success = False
        self.assertTrue(delete_ts_success, "Deleting non-existent timestamps should not raise an error.")


        # Attempt to delete non-existent entire row
        try:
            self.db.delete_row(row_key, dataset_name=dataset_name)
            delete_row_success = True
        except Exception as e:
            logger.error(f"Deleting non-existent row raised exception: {e}")
            delete_row_success = False
        self.assertTrue(delete_row_success, "Deleting non-existent row should not raise an error.")


    def test_key_encoding_order_implicit_specific_cf(self):
        """Test that key encoding maintains correct order within a CF."""
        row_key = "order_row"
        col_name = "order_col"
        dataset_name = self.TEST_CFS[2] # Use 'another_cf'

        # Put data with different timestamps in the same row/col in the specific CF
        ts1 = 1678886400000 # March 15, 2023
        ts2 = 1678886460000 # March 15, 2023 + 1 min

        self.db.put_row(row_key, [(col_name, "val1", ts1)], dataset_name=dataset_name)
        self.db.put_row(row_key, [(col_name, "val2", ts2)], dataset_name=dataset_name)

        # Getting the row should return versions newest first due to timestamp inversion
        result = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name, num_versions=2)

        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 2)
        self.assertEqual(result[col_name][0], (ts2, "val2")) # Newest
        self.assertEqual(result[col_name][1], (ts1, "val1")) # Older


    def test_close_method(self):
        """Test that the close method can be called multiple times."""
        db_path = "test_db_close"
        # Clean up previous test run
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
        # Initialize with a CF to ensure manager handles them on close
        db = WideColumnDB(db_path, column_families=["cf1"])
        db.put_row("r1", [("c1", "v1")], dataset_name="cf1") # Perform an operation in the CF
        db.put_row("r2", [("c2", "v2")], dataset_name=None) # Perform an operation in default
        db.close()
        logger.info("First close successful.")
        # Calling close again should not raise an error
        db.close()
        logger.info("Second close successful.")
        # Clean up the temp directory
        if os.path.exists(db_path):
            shutil.rmtree(db_path)


    def test_empty_strings_as_keys_values_specific_cf(self):
        """Test handling of empty strings for row/column keys and values in a specific CF."""
        row_key = "" # Empty row key
        col_name = "" # Empty column name
        value = "" # Empty value
        ts = int(time.time() * 1000)
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        # Put item with empty strings for row, column, and value in dataset1
        self.db.put_row(row_key, [(col_name, value, ts)], dataset_name=dataset_name)

        # Get the item from dataset1
        result = self.db.get_row(row_key, column_names=col_name, dataset_name=dataset_name)

        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)
        retrieved_ts, retrieved_value = result[col_name][0]

        self.assertEqual(retrieved_value, value) # Value should be empty string
        self.assertEqual(retrieved_ts, ts) # Timestamp should match

        # Get from default CF to ensure isolation
        result_default = self.db.get_row(row_key, column_names=col_name, dataset_name=None)
        self.assertNotIn(col_name, result_default) # Should not be in default CF


    def test_get_row_on_completely_empty_db_specific_cf(self):
        """Test get_row on an empty DB instance for a specific CF."""
        # The DB instance is created in setUp, but no data is put yet.
        # Attempt to get from a specific CF
        result_cf = self.db.get_row("any_row", dataset_name=self.TEST_CFS[0])
        self.assertEqual(result_cf, {})

        # Attempt to get from default CF
        result_default = self.db.get_row("any_row", dataset_name=None)
        self.assertEqual(result_default, {})


    def test_delete_row_triggers_no_write_if_nothing_deleted_specific_cf(self):
        """Test that delete_row doesn't trigger a RocksDB write if no keys match in a specific CF."""
        row_key = "row_no_delete_write"
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        # Ensure the row/dataset is empty
        initial_state = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertEqual(initial_state, {})

        # RocksDB does not expose a simple way to check if a write occurred without
        # potentially impacting performance or relying on internal logging/metrics.
        # The best we can do here is ensure no exceptions are raised when deleting
        # non-existent data, which we test in test_delete_non_existent_data_specific_cf.
        # This test case is left primarily as a placeholder or reminder that a
        # more robust check would require deeper RocksDB interaction.
        logger.info("Note: Test `test_delete_row_triggers_no_write_if_nothing_deleted_specific_cf` primarily checks for lack of errors.")

        try:
             # Attempt to delete from an empty row in the specific CF
            self.db.delete_row(row_key, dataset_name=dataset_name)
            delete_success = True
        except Exception as e:
            logger.error(f"Deleting from empty row raised exception: {e}")
            delete_success = False
        self.assertTrue(delete_success, "Deleting from an empty row should not raise an error.")


    def test_get_row_multiple_cols_varied_versions_specific_cf(self):
        """Test getting multiple columns with varied numbers of versions from a specific CF."""
        row_key = "row_multi_col_versions"
        dataset_name = self.TEST_CFS[1] # Use 'dataset2'

        ts1 = int(time.time() * 1000) - 30000
        ts2 = int(time.time() * 1000) - 20000
        ts3 = int(time.time() * 1000) - 10000
        ts4 = int(time.time() * 1000)

        # Put data in dataset2
        # colA: 2 versions
        self.db.put_row(row_key, [("colA", "valA_old", ts1), ("colA", "valA_new", ts3)], dataset_name=dataset_name)
        # colB: 3 versions
        self.db.put_row(row_key, [("colB", "valB_v1", ts1), ("colB", "valB_v2", ts2), ("colB", "valB_v3", ts4)], dataset_name=dataset_name)
        # colC: 1 version
        self.db.put_row(row_key, [("colC", "valC_only", ts2)], dataset_name=dataset_name)

        # Put similar data in default CF to ensure isolation
        self.db.put_row(row_key, [("colA", "valA_def", ts4), ("colB", "valB_def", ts4)], dataset_name=None)


        # Get row from dataset2 with num_versions=1 (default)
        result_v1 = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertIn("colA", result_v1)
        self.assertEqual(len(result_v1["colA"]), 1)
        self.assertEqual(result_v1["colA"][0], (ts3, "valA_new")) # Newest version of colA

        self.assertIn("colB", result_v1)
        self.assertEqual(len(result_v1["colB"]), 1)
        self.assertEqual(result_v1["colB"][0], (ts4, "valB_v3")) # Newest version of colB

        self.assertIn("colC", result_v1)
        self.assertEqual(len(result_v1["colC"]), 1)
        self.assertEqual(result_v1["colC"][0], (ts2, "valC_only")) # Newest version of colC

        # Get row from dataset2 with num_versions=3
        result_v3 = self.db.get_row(row_key, dataset_name=dataset_name, num_versions=3)
        self.assertIn("colA", result_v3)
        # colA only has 2 versions, so we should get 2, not 3
        self.assertEqual(len(result_v3["colA"]), 2)
        self.assertEqual(result_v3["colA"][0], (ts3, "valA_new"))
        self.assertEqual(result_v3["colA"][1], (ts1, "valA_old"))

        self.assertIn("colB", result_v3)
        self.assertEqual(len(result_v3["colB"]), 3)
        self.assertEqual(result_v3["colB"][0], (ts4, "valB_v3"))
        self.assertEqual(result_v3["colB"][1], (ts2, "valB_v2"))
        self.assertEqual(result_v3["colB"][2], (ts1, "valB_v1"))

        self.assertIn("colC", result_v3)
        # colC only has 1 version, so we should get 1, not 3
        self.assertEqual(len(result_v3["colC"]), 1)
        self.assertEqual(result_v3["colC"][0], (ts2, "valC_only"))

        # Get row from default CF - should only get default values
        result_default = self.db.get_row(row_key, dataset_name=None, num_versions=3)
        self.assertIn("colA", result_default)
        self.assertEqual(len(result_default["colA"]), 1)
        self.assertEqual(result_default["colA"][0], (ts4, "valA_def"))
        self.assertIn("colB", result_default)
        self.assertEqual(len(result_default["colB"]), 1)
        self.assertEqual(result_default["colB"][0], (ts4, "valB_def"))
        self.assertNotIn("colC", result_default)


    def test_get_row_num_versions_optimization_specific_cf(self):
        """Test that get_row correctly stops scanning after reaching num_versions per column in a specific CF."""
        row_key = "row_v_opt"
        col_name1 = "col_opt1"
        col_name2 = "col_opt2"
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'

        # Put many versions for two columns in dataset1
        num_versions_to_put = 10
        timestamps1 = [int(time.time() * 1000) - i * 1000 for i in range(num_versions_to_put)]
        timestamps2 = [int(time.time() * 1000) - i * 500 for i in range(num_versions_to_put)] # Different timestamps

        items1 = [(col_name1, f"val1_{ts}", ts) for ts in timestamps1]
        items2 = [(col_name2, f"val2_{ts}", ts) for ts in timestamps2]

        self.db.put_row(row_key, items1, dataset_name=dataset_name)
        self.db.put_row(row_key, items2, dataset_name=dataset_name)

        # Put some data in default CF to ensure isolation
        self.db.put_row(row_key, [(col_name1, "val_def", int(time.time()*1000))], dataset_name=None)


        # Get 3 versions for both columns from dataset1
        num_versions_to_get = 3
        result = self.db.get_row(row_key, column_names=[col_name1, col_name2],
                                 num_versions=num_versions_to_get, dataset_name=dataset_name)

        self.assertIn(col_name1, result)
        self.assertEqual(len(result[col_name1]), num_versions_to_get)
        # Verify the timestamps of the retrieved versions are the newest ones
        retrieved_ts1 = [ts for ts, _ in result[col_name1]]
        expected_ts1 = sorted(timestamps1, reverse=True)[:num_versions_to_get]
        self.assertEqual(retrieved_ts1, expected_ts1)

        self.assertIn(col_name2, result)
        self.assertEqual(len(result[col_name2]), num_versions_to_get)
        # Verify the timestamps of the retrieved versions are the newest ones
        retrieved_ts2 = [ts for ts, _ in result[col_name2]]
        expected_ts2 = sorted(timestamps2, reverse=True)[:num_versions_to_get]
        self.assertEqual(retrieved_ts2, expected_ts2)


        # Get 1 version from default CF - should only get the default value
        result_default = self.db.get_row(row_key, column_names=col_name1, dataset_name=None, num_versions=1)
        self.assertIn(col_name1, result_default)
        self.assertEqual(len(result_default[col_name1]), 1)
        self.assertEqual(result_default[col_name1][0][1], "val_def") # Check value


    def test_put_row_empty_items_list_specific_cf(self):
        """Test calling put_row with an empty items list in a specific CF."""
        row_key = "row_empty_put"
        dataset_name = self.TEST_CFS[1] # Use 'dataset2'

        # Verify row is empty initially
        initial_state = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertEqual(initial_state, {})

        # Call put_row with empty list
        try:
            self.db.put_row(row_key, [], dataset_name=dataset_name)
            put_success = True
        except Exception as e:
            logger.error(f"put_row with empty list raised exception: {e}")
            put_success = False
        self.assertTrue(put_success, "put_row with an empty items list should not raise an error.")

        # Verify row is still empty
        after_put_state = self.db.get_row(row_key, dataset_name=dataset_name)
        self.assertEqual(after_put_state, {})


    def test_malformed_keys_during_decode_specific_cf(self):
        """Test that malformed keys encountered during iteration are skipped in a specific CF."""
        row_key = "row_malformed_test"
        col_name = "col_malformed"
        dataset_name = self.TEST_CFS[0] # Use 'dataset1'
        ts = int(time.time() * 1000)

        # Put one valid key in dataset1
        valid_key = self.db.key_codec.encode(row_key=row_key, column_name=col_name, timestamp_ms=ts)
        valid_value = self.db.serializer.serialize("valid_value")

        # Get the CF handle
        cf_handle = self.db._get_cf_handle(dataset_name)

        # Manually put a malformed key into the specific CF using the underlying rocksdict handle
        # This bypasses the WideColumnDB's put_row which would use the codec.
        # We need to create a key that the codec will *fail* to decode later.
        # Example malformed key for SeparatorCodec (missing a separator/part): b"row_malformed_test\x00col_malformed"
        # Example malformed key for LengthPrefixedKeyCodec (wrong length prefix): b'\x04row_malformed_test\x05col_malformed' + struct.pack('>Q', KeyCodec.MAX_UINT64 - ts)

        codec_type = type(self.db.key_codec)

        if codec_type == KeyCodec:
             # Malformed key for SeparatorCodec: missing timestamp part
            malformed_key = b"row_malformed_test\x00col_malformed\x00"[:-1] # Cut off part of the timestamp
            logger.info(f"Testing malformed key for SeparatorCodec: {malformed_key.hex()}")
            # Check that our codec actually considers this malformed
            self.assertIsNone(self.db.key_codec.decode(malformed_key), "Codec should fail to decode this malformed key.")

        elif codec_type == LengthPrefixedKeyCodec:
             # Malformed key for LengthPrefixedKeyCodec: not enough data for part
            malformed_key = b'\x04' + b'row_malformed_test'.encode('utf-8') + b'\x05' + b'col_malformed'.encode('utf-8')[:3] # Declares 5 bytes for col, but only provides 3
            malformed_key += struct.pack('>Q', LengthPrefixedKeyCodec.MAX_UINT64 - ts) # Add a valid timestamp at the end using correct codec's MAX_UINT64
            logger.info(f"Testing malformed key for LengthPrefixedKeyCodec: {malformed_key.hex()}")
            # Check that our codec actually considers this malformed
            self.assertIsNone(self.db.key_codec.decode(malformed_key), "Codec should fail to decode this malformed key.")
        else:
            self.fail(f"Unknown codec type: {codec_type.__name__}")
            return # Stop the test if codec is unknown


        # Use a batch to put both the valid and malformed keys atomically
        batch = rocksdb.WriteBatch()
        if valid_key is not None and valid_value is not None:
            batch.put(valid_key, valid_value, column_family=cf_handle)
        batch.put(malformed_key, b"malformed_value", column_family=cf_handle) # Put the malformed key


        # Write the batch
        self.db._db_manager.db.write(batch)

        # Now, attempt to get the row. The iterator should encounter the malformed key,
        # the decode method should return None, and WideColumnDB should skip it.
        # We should only retrieve the valid key.
        result = self.db.get_row(row_key, dataset_name=dataset_name, num_versions=2) # Requesting 2 versions to be sure iterator continues past malformed key

        # Verify that only the valid key's data was returned
        self.assertIn(col_name, result)
        self.assertEqual(len(result[col_name]), 1)
        retrieved_ts, retrieved_value = result[col_name][0]
        self.assertEqual(retrieved_value, "valid_value")
        self.assertEqual(retrieved_ts, ts)

        # Verify that the malformed key did NOT prevent the valid key from being read
        # This is implicitly tested by the assertion above.


    def test_different_serializers_specific_cf(self):
        """Test different serializers with put/get in a specific CF."""
        db_path = "test_db_serializers_cf"
        dataset_name = "serializer_cf"
        # Clean up previous test run
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        # Test StrSerializer (already implicitly tested, but explicitly call it out)
        db_str = WideColumnDB(db_path, serializer=StrSerializer(), column_families=[dataset_name])
        db_str.put_row("row_str", [("col_str", "simple string")], dataset_name=dataset_name)
        result_str = db_str.get_row("row_str", dataset_name=dataset_name)
        self.assertEqual(result_str.get("col_str", [])[0][1], "simple string")
        db_str.close()


        # Test PickleSerializer
        db_pickle = WideColumnDB(db_path, serializer=PickleSerializer(), column_families=[dataset_name])
        data_pickle = {"key": "value", "number": 123, "list": [1, 2, 3]}
        db_pickle.put_row("row_pickle", [("col_pickle", data_pickle)], dataset_name=dataset_name)
        result_pickle = db_pickle.get_row("row_pickle", dataset_name=dataset_name)
        self.assertEqual(result_pickle.get("col_pickle", [])[0][1], data_pickle)
        db_pickle.close()

        # Test JsonSerializer
        db_json = WideColumnDB(db_path, serializer=JsonSerializer(), column_families=[dataset_name])
        data_json = {"key": "value", "number": 123, "list": [1, 2, 3]} # JSON friendly data
        db_json.put_row("row_json", [("col_json", data_json)], dataset_name=dataset_name)
        result_json = db_json.get_row("row_json", dataset_name=dataset_name)
        self.assertEqual(result_json.get("col_json", [])[0][1], data_json)
        db_json.close()


        # Test MsgPackSerializer
        db_msgpack = WideColumnDB(db_path, serializer=MsgPackSerializer(), column_families=[dataset_name])
        data_msgpack = {"key": b"binary_value", "number": 123, "list": [1, 2, 3]} # MsgPack handles bytes
        db_msgpack.put_row("row_msgpack", [("col_msgpack", data_msgpack)], dataset_name=dataset_name)
        result_msgpack = db_msgpack.get_row("row_msgpack", dataset_name=dataset_name)
        # Note: msgpack unpackb with raw=False decodes binary to str, adjust expected if necessary
        # Assuming raw=False gives str where possible:
        expected_msgpack_data = {"key": "binary_value", "number": 123, "list": [1, 2, 3]}
        self.assertEqual(result_msgpack.get("col_msgpack", [])[0][1], expected_msgpack_data)
        db_msgpack.close()


        # Clean up the temp directory
        if os.path.exists(db_path):
            shutil.rmtree(db_path)


    # Add a test for initializing WideColumnDB without specifying column_families (should use default)
    def test_init_without_column_families_uses_default(self):
        db_path = "test_db_no_cfs"
        # Clean up previous test run
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        # Initialize WideColumnDB without specifying column_families
        db = WideColumnDB(db_path)

        # Put data without dataset_name (should go to default CF)
        db.put_row("row1", [("colA", "val1")])

        # Get data without dataset_name (should read from default CF)
        result = db.get_row("row1")
        self.assertIn("colA", result)
        self.assertEqual(result.get("colA", [])[0][1], "val1")

        # Attempting to access a non-existent CF should raise an error
        with self.assertRaises(ValueError):
            db.put_row("row2", [("colB", "val2")], dataset_name="non_existent_cf")

        db.close()

        # Clean up
        if os.path.exists(db_path):
            shutil.rmtree(db_path)


    # Add a test for providing 'default' in the column_families list explicitly
    def test_init_with_default_in_column_families(self):
        db_path = "test_db_explicit_default"
        cfs_list = ["default", "my_cf"] # Explicitly include 'default'
        # Clean up previous test run
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        # Initialize WideColumnDB with 'default' explicitly listed
        db = WideColumnDB(db_path, column_families=cfs_list)

        # Put data without dataset_name (should go to default CF)
        db.put_row("row1", [("colA", "val1")])
        # Put data in the explicitly listed custom CF
        db.put_row("row1", [("colB", "valB")], dataset_name="my_cf")


        # Get data from default CF
        result_default = db.get_row("row1")
        self.assertIn("colA", result_default)
        self.assertNotIn("colB", result_default)
        self.assertEqual(result_default.get("colA", [])[0][1], "val1")

         # Get data from custom CF
        result_custom = db.get_row("row1", dataset_name="my_cf")
        self.assertIn("colB", result_custom)
        self.assertNotIn("colA", result_custom)
        self.assertEqual(result_custom.get("colB", [])[0][1], "valB")


        db.close()

        # Clean up
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
