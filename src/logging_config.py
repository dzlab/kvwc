import logging

def configure_logging(level=logging.INFO):
    """
    Configures the root logger for console output.

    This function should be called once at the beginning of the application's
    execution to set up the logging infrastructure.

    Args:
        level: The logging level to set for the root logger (e.g., logging.INFO, logging.DEBUG).
    """
    # Get the root logger
    root_logger = logging.getLogger()

    # Prevent adding multiple handlers if this function is called more than once
    if not root_logger.handlers:
        root_logger.setLevel(level)

        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # Create a formatter and attach it to the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        # Add the handler to the root logger
        root_logger.addHandler(console_handler)

    # Optional: Configure specific package loggers if needed, e.g.:
    # logging.getLogger('kvwc').setLevel(logging.DEBUG)

# Example usage (in your main application file):
# from kvwc.src.logging_config import configure_logging
# configure_logging()
# import logging
# logger = logging.getLogger(__name__)
# logger.info("Logging configured and working!")

configure_logging()
