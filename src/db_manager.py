import logging
import rocksdict as rocksdb

logger = logging.getLogger(__name__)

class RocksDBManager:
    """
    Manages the lifecycle of a RocksDB database instance with support for Column Families.
    Handles opening, closing, and applying RocksDB options.
    """
    def __init__(self, db_path, rocksdb_options=None, column_families=None):
        """
        Initializes the RocksDBManager.

        Args:
            db_path (str): The path to the RocksDB database directory.
            rocksdb_options (dict, optional): A dictionary of RocksDB options to apply.
                                              Keys should correspond to settable options
                                              (e.g., 'max_open_files'). Values are the
                                              desired option values. Defaults to None.
            column_families (list, optional): A list of Column Family names (strings) to open/create.
                                             Defaults to None, meaning only the 'default' CF is used.
        """
        self._db_path = db_path
        self._rocksdb_options = rocksdb_options
        self._column_families = column_families if column_families is not None else [] # Store requested CFs
        self._db = None # Holds the RocksDB instance

    def open_db(self):
        """
        Opens the RocksDB database instance with specified Column Families.

        Applies the specified options and creates the database and Column Families
        if they don't exist. Stores the opened database instance internally.

        Returns:
            rocksdict.Rdict: The opened RocksDB database instance, which can be
                             indexed by CF name (including 'default').

        Raises:
            Exception: If there is an error opening the database or creating CFs.
        """
        if self._db is not None:
            logger.warning(f"Database at {self._db_path} is already open.")
            return self._db

        opts = rocksdb.Options()
        opts.create_if_missing(True)
        opts.create_missing_column_families(True) # Enable creating missing CFs

        # Apply provided options
        if self._rocksdb_options is not None:
            if not isinstance(self._rocksdb_options, dict):
                 logger.warning("Provided rocksdb_options is not a dictionary. Ignoring.")
            else:
                for key, value in self._rocksdb_options.items():
                    # Attempt to find and call the corresponding setter method (common rocksdict pattern)
                    setter_name = f"set_{key}"
                    setter_method = getattr(opts, setter_name, None)

                    if setter_method and callable(setter_method):
                        try:
                            setter_method(value)
                            logger.debug(f"Applied RocksDB option: {key} = {value}")
                        except Exception as e:
                            logger.warning(f"Failed to apply RocksDB option '{key}' with value '{value}': {e}")
                    # Also try setting attributes directly (less common for complex options)
                    elif hasattr(opts, key):
                         try:
                             setattr(opts, key, value)
                             logger.debug(f"Applied RocksDB attribute option: {key} = {value}")
                         except Exception as e:
                             logger.warning(f"Failed to apply RocksDB attribute option '{key}' with value '{value}': {e}")
                    else:
                        logger.warning(f"Unknown or unsettable RocksDB option ignored: {key}")

        # Always include the 'default' CF when opening
        all_cfs_to_open = list(set(['default'] + self._column_families))

        try:
            # Open with multiple column families
            cols = {}
            for colname in all_cfs_to_open:
                cols[colname] = rocksdb.Options()
            self._db = rocksdb.Rdict(self._db_path, opts, column_families=cols)
            logger.info(f"Successfully opened RocksDB database at {self._db_path} with CFs: {all_cfs_to_open}")
            return self._db
        except Exception as e:
            logger.error(f"Failed to open RocksDB database at {self._db_path}: {e}")
            # Re-raise the exception to indicate failure
            raise e

    def close_db(self):
        """
        Closes the RocksDB database instance if it is open.
        """
        if self._db is not None:
            logger.info(f"Closing RocksDB database at {self._db_path}")
            # The rocksdict object does not have an explicit close method.
            # Deleting the object triggers the underlying C++ cleanup.
            # If there are CF handles, they are also managed by the Rdict instance.
            del self._db
            self._db = None
        else:
            logger.warning(f"Database at {self._db_path} is not open. Cannot close.")


    def _validate_dataset(self, dataset_name) -> rocksdb.Rdict:
        """
        Gets the rocksdict handle for the specified dataset_name (Column Family).
        Uses 'default' CF if dataset_name is None or not provided.
        Raises an error if the dataset_name corresponds to a CF that wasn't opened.
        """
        cf_name = dataset_name if dataset_name is not None else 'default'
        if cf_name not in self._column_families:
            # This check prevents using CFs that weren't specified during init.
            # RocksDB might implicitly create them anyway depending on version/options,
            # but requiring them to be listed in init makes the CFs explicit.
            # Alternatively, we could allow implicit creation here, but being explicit
            # is generally better for managing CFs.
            logger.warning(f"Attempted to access unknown Column Family: {cf_name}. Was it included in column_families during initialization?")
            # Decide whether to raise error or default to 'default'/'None' CF.
            # For now, let's raise an error to enforce explicit CF listing.
            raise ValueError(f"Column Family '{cf_name}' is not known. Please include it in the 'column_families' list when initializing WideColumnDB.")
        return cf_name

    def get_cf_handle(self, dataset_name) -> rocksdb.ColumnFamily:
        cf_name = self._validate_dataset(dataset_name)
        # Access the CF handle using dictionary-like access
        try:
            return self.db.get_column_family_handle(cf_name)
        except KeyError:
            # This should ideally not happen if _known_column_families is correct and DB opened successfully
            logger.error(f"Failed to get handle for Column Family '{cf_name}'. It might not have been opened correctly.")
            raise

    def get_cf(self, dataset_name) -> rocksdb.Rdict:
        """
        Gets the rocksdict handle for the specified dataset_name (Column Family).
        Uses 'default' CF if dataset_name is None or not provided.
        Raises an error if the dataset_name corresponds to a CF that wasn't opened.
        """
        cf_name = self._validate_dataset(dataset_name)
        # Access the CF handle using dictionary-like access
        try:
            return self.db.get_column_family(cf_name)
        except KeyError:
             # This should ideally not happen if _known_column_families is correct and DB opened successfully
             logger.error(f"Failed to get handle for Column Family '{cf_name}'. It might not have been opened correctly.")
             raise


    @property
    def db(self):
        """
        Provides access to the underlying RocksDB instance.
        Returns None if the database is not open.
        The returned object can be indexed by CF name to access specific CF handles.
        e.g., db_instance['my_cf'].put(...)
        """
        db_instance = self._db
        if db_instance is None:
            raise RuntimeError("Database is not initialized. Cannot get CF handle.")
        return db_instance

    # Context management methods (__enter__ and __exit__) remain the same
    def __enter__(self):
        """Context management protocol entry: Opens the database."""
        self.open_db()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context management protocol exit: Closes the database."""
        self.close_db()
        # Return False to propagate any exceptions, True to suppress
        return False
