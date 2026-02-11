from src.transformation.validation_stage.pre_validation_stage.orders import check_order
from src.transformation.validation_stage.pre_validation_stage.payments import check_payments
from src.utils.helpers import save
from src.utils.logger import get_logger
import pandas as pd

logger = get_logger(__name__,module_name='pre_validation_stage')

def data_validation(tables: dict[str, pd.DataFrame], rules: dict) -> dict[str, pd.DataFrame]:

    validated_tables = {}

    logger.info(f"Running pre_validation_stage for {len(tables)} tables")

    for table_name, df in tables.items():
        logger.info(f"Validating table: {table_name}")

        if table_name not in rules:
            raise KeyError(f"No pre_validation_stage rules defined for table '{table_name}'")

        df = df.copy()
        df["is_valid"] = True
        df["error_reason"] = ""

        table_rules = rules[table_name]

        if table_rules.get("unique_cols"):
            df = check_unique(df, table_name, table_rules["unique_cols"])

        if table_rules.get("not_null"):
            df = check_null_values(df, table_name, table_rules["not_null"])

        if table_rules.get("payment_value"):
            df = check_payments(df, table_name, table_rules["payment_value"])
        if table_rules.get('order_valid'):
            df = check_order(df, table_name, table_rules["order_valid"])

        # split data
        rejected = df[df["is_valid"] == False]
        valid = df[df["is_valid"]]

        if not rejected.empty:
            save(rejected, "rejected", table_name)
            logger.warning(f"{len(rejected)} rejected rows saved for {table_name}")

        validated_tables[table_name] = valid

    logger.info("Validation step completed")
    return validated_tables



def check_unique(df: pd.DataFrame, table_name: str, columns: list[str]) -> pd.DataFrame:
    duplicates = df.duplicated(subset=columns, keep=False)

    if duplicates.any():
        logger.warning(f"Duplicates found in {table_name} for {columns}")

        df.loc[duplicates, "is_valid"] = False
        df.loc[duplicates, "error_reason"] += "duplicate"

    return df


def check_null_values(df: pd.DataFrame, table_name: str, columns: list[str]) -> pd.DataFrame:
    null_mask = df[columns].isnull().any(axis=1)

    if null_mask.any():
        logger.warning(f"Null values found in {table_name} for {columns}")

        df.loc[null_mask, "is_valid"] = False
        df.loc[null_mask, "error_reason"] += "null_value"

    return df








