import logging, os

def setup_logger(name=None):
    """
    Configure and return a logger.
    :param name: Optional name for the logger.
    """
    logger = logging.getLogger(name)  # Use root logger if name is None
    if not logger.handlers:  # Avoid duplicate handlers
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger