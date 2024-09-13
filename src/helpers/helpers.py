# helpers.py
import logging

class CodeHelper:
    def __init__(self, log_file_path: str):
        """
        Initialize the CodeHelper class and set up logging configuration.
        Args:
            log_file_path (str): The path to the log file.
        """
        logging.basicConfig(
            filename=log_file_path,
            filemode='a',
            format='%(asctime)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO
        )
        self.logger = logging.getLogger()

    def log_info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def log_error(self, message: str):
        """Log an error message."""
        self.logger.error(message)
