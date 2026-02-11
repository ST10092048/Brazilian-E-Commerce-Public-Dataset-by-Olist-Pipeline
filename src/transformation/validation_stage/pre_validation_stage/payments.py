import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='payment_validation')


def check_payments(df: pd.DataFrame, table_name: str, columns: list[str]) -> pd.DataFrame:
    #Total payment value must be ≥ order value
    invalid_payment = (df[columns] <= 0).any(axis=1)

    if invalid_payment.any():
        logger.warning(f"Invalid payment values found in {table_name}")

        df.loc[invalid_payment, "is_valid"] = False
        df.loc[invalid_payment, "error_reason"] += "invalid_payment"

    return df