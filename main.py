import pandas as pd

from src.ingestion.extraction import  run_extract
from src.quality.quality import run_quality
from src.transformation.clean import run_cleaning
from src.transformation.enrich_data import run_enrichment
from src.transformation.rules import TABLE_RULES
from src.transformation.validation import run_validation


def main():
    tables = run_extract(['Brazilian E-Commerce/olist_customers_dataset.csv',
                          'Brazilian E-Commerce/olist_geolocation_dataset.csv',
                          'Brazilian E-Commerce/olist_order_items_dataset.csv',
                          'Brazilian E-Commerce/olist_order_payments_dataset.csv',
                          'Brazilian E-Commerce/olist_order_reviews_dataset.csv',
                          'Brazilian E-Commerce/olist_orders_dataset.csv',
                          'Brazilian E-Commerce/olist_products_dataset.csv',
                          'Brazilian E-Commerce/olist_sellers_dataset.csv',
                          'Brazilian E-Commerce/product_category_name_translation.csv'])

    cleaned = run_cleaning(tables,TABLE_RULES)

    validated_tables = run_validation(cleaned, TABLE_RULES)

    run_enrichment(validated_tables, TABLE_RULES)

    metrics = run_quality(validated_tables, TABLE_RULES)
    print(metrics)
    metrics_df = pd.DataFrame.from_dict({
        table: {
            "valid_rate": metrics[table]["valid_rate"],
            "rejected_rate": metrics[table]["rejected_rate"],
            "total_rows": metrics[table]["total_rows"]
        } for table in metrics
    }, orient="index")

    # Add timestamp
    metrics_df["date"] = pd.Timestamp.today()
    print(metrics_df)


    return None


if __name__ == "__main__":
    main()