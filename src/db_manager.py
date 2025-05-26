import logging
import rocksdict as rocksdb

logger = logging.getLogger(__name__)

class RocksDBManager:
    """
    Manages the lifecycle of a RocksDB database instance.
    Handles opening, closing, and applying RocksDB options.
    """
    def __init__(self, db_path, rocksdb_options=None):
        """
        Initializes the RocksDBManager.

        Args:
            db_path (str): The path to the RocksDB database directory.
            rocksdb_options (dict, optional): A dictionary of RocksDB options to apply.
                                              Keys should correspond to settable options
                                              (e.g., 'max_open_files'). Values are the
                                              desired option values. Defaults to None.
        """
        self._db_path = db_path
        self._rocksdb_options = rocksdb_options
        self._db = None # Holds the RocksDB instance

    def open_db(self):
        """
        Opens the RocksDB database instance.

        Applies the specified options and creates the database if it doesn't exist.
        Stores the opened database instance internally.

        Returns:
            rocksdict.Rdict: The opened RocksDB database instance.

        Raises:
            Exception: If there is an error opening the database.
        """
        if self._db is not None:
            logger.warning(f"Database at {self._db_path} is already open.")
            return self._db

        opts = rocksdb.Options()
        opts.create_if_missing(True) # Set default option

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

        try:
            self._db = rocksdb.Rdict(self._db_path, opts)
            logger.info(f"Successfully opened RocksDB database at {self._db_path}")
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
            del self._db
            self._db = None
        else:
            logger.warning(f"Database at {self._db_path} is not open. Cannot close.")

    @property
    def db(self):
        """
        Provides access to the underlying RocksDB instance.
        Returns None if the database is not open.
        """
        return self._db

    def __enter__(self):
        """Context management protocol entry: Opens the database."""
        self.open_db()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context management protocol exit: Closes the database."""
        self.close_db()
        # Return False to propagate any exceptions, True to suppress
        return False