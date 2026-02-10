import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='order_validation')

def check_order(df: pd.DataFrame, table_name: str, columns: list[str]) -> pd.DataFrame:
    invalid_order = df[columns].isna().any(axis=1)

    if invalid_order.any():
        logger.warning(f"Invalid payment values found in {table_name}")

        df.loc[invalid_order, "is_valid"] = False
        df.loc[invalid_order, "error_reason"] += "invalid_order;"

    return df
