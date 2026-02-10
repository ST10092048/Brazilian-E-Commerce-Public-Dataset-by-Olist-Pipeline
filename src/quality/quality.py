# src/quality/run_quality.py
from src.quality.metrics import table_quality_metrics, column_quality_metrics
from src.utils.logger import get_logger
from src.utils.helpers import save
import pandas as pd

logger = get_logger(__name__, module_name="quality")


def run_quality(tables: dict[str, pd.DataFrame], rules: dict) -> dict:
    """
    Run data quality metrics on all tables.
    rules: optional dict for column-level metrics
    Returns a dict of metrics per table
    """
    all_metrics = {}

    logger.info(f"Running data quality for {len(tables)} tables")

    for table_name, df in tables.items():
        logger.info(f"Computing quality metrics for {table_name}")

        # Table-level metrics
        table_metrics = table_quality_metrics(df)

        # Column-level metrics if specified
        cols = rules.get(table_name, {}).get("quality_columns", [])
        if cols:
            table_metrics["columns"] = column_quality_metrics(df, cols)

        all_metrics[table_name] = table_metrics

        # Save metrics as CSV (one row per table)
        save(pd.DataFrame([table_metrics]), "quality", table_name)

        logger.info(f"Metrics saved for {table_name}")

    return all_metrics
