import os

import pandas as pd

from src.utils.helpers import save
from src.utils.logger import get_logger

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

logger = get_logger(__name__,module_name='extract')

def extract_from_csv(file_path: str) -> pd.DataFrame:
    logger.info(f"Reading CSV: {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Read {len(df)} rows from CSV")
    return df



def run_extract(csv_paths: list[str]) -> dict[str, pd.DataFrame]:
    tables = {}

    for path in csv_paths:
        table_name = os.path.splitext(os.path.basename(path))[0]

        logger.info(f"Extracting table: {table_name}")

        df = extract_from_csv(path)

        save(df, "raw", f"{table_name}")

        tables[table_name] = df

    return tables
