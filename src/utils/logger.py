import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

LOG_DIR = Path("logs")
MASTER_DIR = LOG_DIR / "master"
MASTER_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, module_name: str = None, level: int = logging.INFO, to_console: bool = True,
                     timestamped: bool = False):
    """
    Returns a logger that:
        1. Logs everything to master/pipeline.log
        2. Logs all module messages to module folder
        3. Separates warnings and errors
        4. Optionally console output
        5. Can create timestamped subfolders for each run
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # --- Master log ---
    master_handler = RotatingFileHandler(
        MASTER_DIR / "pipeline.log",
        maxBytes=5_000_000,
        backupCount=5
    )
    master_handler.setFormatter(formatter)
    logger.addHandler(master_handler)

    if module_name:
        # Determine module folder
        module_dir = LOG_DIR / module_name
        if timestamped:
            run_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            module_dir = module_dir / run_time
        module_dir.mkdir(parents=True, exist_ok=True)

        # Module all-level log
        module_handler = RotatingFileHandler(module_dir / f"{module_name}.log", maxBytes=5_000_000, backupCount=5)
        module_handler.setFormatter(formatter)
        logger.addHandler(module_handler)

        # Warnings log
        warn_handler = RotatingFileHandler(module_dir / f"{module_name}_warning.log", maxBytes=5_000_000, backupCount=5)
        warn_handler.setLevel(logging.WARNING)
        warn_handler.setFormatter(formatter)
        logger.addHandler(warn_handler)

        # Errors log
        error_handler = RotatingFileHandler(module_dir / f"{module_name}_error.log", maxBytes=5_000_000, backupCount=5)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

    # Console
    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
