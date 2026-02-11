from src.ingestion.extraction import  run_extract
from src.transformation.clean import run_cleaning
from src.transformation.enrichment_stage.enrich_data import run_enrichment, run_joins
from src.transformation.business_rules import TABLE_RULES
from src.transformation.validation_stage.business_validation.business_validation_ import check_order_status,check_delivery_before_purchase
from src.transformation.validation_stage.data_validation.validation import run_validation


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
    neo = run_joins(tables)
    neo_2 =check_delivery_before_purchase(neo)
    wrong = neo_2[neo_2['is_valid'] == False]
    print(wrong)
    wrong_2 =check_order_status(neo)
    no =wrong_2[wrong_2['is_valid'] == False]
    print(no)




if __name__ == "__main__":
    main()