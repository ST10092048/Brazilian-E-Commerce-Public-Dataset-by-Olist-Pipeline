# src/quality/metrics.py
import pandas as pd

def table_quality_metrics(df: pd.DataFrame) -> dict:
    """
    Compute dataset-level quality metrics.
    Assumes df has 'is_valid' column from validation.
    """
    total_rows = len(df)
    if total_rows == 0:
        return {}

    valid_rows = df["is_valid"].sum()
    rejected_rows = total_rows - valid_rows

    return {
        "total_rows": total_rows,
        "valid_rows": int(valid_rows),
        "rejected_rows": int(rejected_rows),
        "valid_rate": round(valid_rows / total_rows, 4),
        "rejected_rate": round(rejected_rows / total_rows, 4),
    }


def column_quality_metrics(df: pd.DataFrame, columns: list[str]) -> dict:
    """
    Compute column-level quality metrics: null rate and distinct rate.
    """
    metrics = {}

    for col in columns:
        metrics[col] = {
            "null_rate": round(df[col].isnull().mean(), 4),
            "distinct_rate": round(df[col].nunique() / len(df), 4),
        }

    return metrics
