import pandas as pd


def run_enrichment(tables: dict[str, pd.DataFrame], rules: dict) -> dict[str, pd.DataFrame]:
    enriched_tables = {}

    for table_name, df in tables.items():
        df = df.copy()
        df = df[df['is_valid'] == True]
        enrichment_rules = rules.get(table_name, {}).get("enrichment", {})

        # Example: add time columns
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
