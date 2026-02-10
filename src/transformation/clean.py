import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='clean')
def run_cleaning(tables: dict[str, pd.DataFrame], rules: dict) -> dict[str, pd.DataFrame]:
    cleaned = {}

    for table_name, df in tables.items():
        if table_name not in rules:
            raise KeyError(f"No cleaning rules defined for table '{table_name}'")

        table_rules = rules[table_name]
        df = df.copy()
        logger.info(f"Cleaning table '{table_name}' with {len(df)}")
        df.drop_duplicates()
        logger.info(f"Removing duplicate table '{table_name} {len(df)}'")

        # String cleaning
        df = standard_string_cleaning(df, table_rules.get("string_cols", []))

        # Numeric cleaning
        df = numeric_cleaning(df, table_rules.get("number_cols", {}))

        # Datetime cleaning
        # df = datetime_cleaning(df, table_rules.get("datetime_cols", {}))

        # Drop duplicates if subset specified
        drop_subset = table_rules.get("drop_duplicates")
        if drop_subset:
            missing = set(drop_subset) - set(df.columns)
            if missing:
                raise KeyError(f"Missing columns for duplicate removal: {missing}")
            df = df.drop_duplicates(subset=drop_subset)

        cleaned[table_name] = df
        print(table_name)
        print(df)

    return cleaned


def standard_string_cleaning(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            raise KeyError(f"Expected string column '{col}' not found")

        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
        )

    return df

def numeric_cleaning(df:pd.DataFrame,number_data):

    for col, dtype in number_data.items():
        if col not in df.columns:
            raise KeyError(f"Expected numeric column '{col}' not found")

        df[col] = pd.to_numeric(df[col], errors="coerce")

        if dtype == "int":
            df[col] = df[col].astype("Int64")
        elif dtype == "float":
            df[col] = df[col].astype("float")


    return df
