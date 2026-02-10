from pathlib import Path

import pandas as pd

from src.utils.dates import today_str
from src.utils.logger import get_logger

logger = get_logger(__name__)
def save(df: pd.DataFrame,stage, name_prefix: str):
    RAW_DIR = Path(f"data_lake/{stage}/{name_prefix}")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    today = today_str()
    file_path = RAW_DIR / f"{name_prefix}_{today}.csv"
    df.to_csv(file_path, index=False)
    logger.info(f"Saved raw data to {file_path}")
    return file_path