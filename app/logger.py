import logging
import os

def setup_logger(logfile: str = "logs/errors.log") -> logging.Handler:
    """
    Configures logging to a file.

    Args:
        logfile (str): Path to the log file.

    Returns:
        logging.Handler: The filehandler for logging.
    """
    os.makedirs(os.path.dirname(logfile), exist_ok=True)
    handler = logging.FileHandler(logfile)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ))
    return handler
