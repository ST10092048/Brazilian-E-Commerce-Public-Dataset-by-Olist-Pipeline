import pandas as pd

from src.transformation.enrichment_stage.joins.orders import join_orders_payments
from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='enrich_stage')
def run_enrichment(tables: dict[str, pd.DataFrame], rules: dict) -> dict[str, pd.DataFrame]:
    enriched_tables = {}

    for table_name, df in tables.items():
        df = df.copy()
        df = df[df['is_valid'] == True]


        enrichment_rules = rules.get(table_name, {}).get("enrichment", {})

        if 'add_time_columns' in enrichment_rules:
            for col in enrichment_rules["add_time_columns"]:
                df[col] = pd.to_datetime(df[col])
                df[f"{col}_year"] = df[col].dt.year
                df[f"{col}_month"] = df[col].dt.month
                df[f"{col}_day"] = df[col].dt.day

        if "calculate_total_values" in enrichment_rules:
            if 'total_values' in enrichment_rules["total_values"]:
                df['Total'] = df.get('price', 0) + df.get('freight_value', 0)
                
        enriched_tables[table_name] = df
        print(df)


    return enriched_tables

def run_joins(tables: dict[str, pd.DataFrame])-> pd.DataFrame:
    orders_df = tables.get("olist_orders_dataset")
    payments_df = tables.get("olist_order_payments_dataset")

    if orders_df is None or payments_df is None:
        logger.warning("Both 'orders' and 'payments' tables must be present")
        return pd.DataFrame()


    order_payments_df = join_orders_payments(orders_df, payments_df)


    return order_payments_df