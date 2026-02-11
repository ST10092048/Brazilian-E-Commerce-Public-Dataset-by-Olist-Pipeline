from src.ingestion.extraction import  run_extract
from src.loading_to_warehouse.create_tables import create_replace, confirm_tables
from src.transformation.clean import data_run_cleaning
from src.transformation.enrichment_stage.enrich_data import run_enrichment, run_joins
from src.transformation.business_rules import TABLE_RULES
from src.transformation.validation_stage.business_validation.business_validation_ import check_order_status,check_delivery_before_purchase
from src.transformation.validation_stage.run_validation import pre_validation, run_validation


def main():
    # RAW DATA
    tables = run_extract(['Brazilian E-Commerce/olist_customers_dataset.csv',
                          'Brazilian E-Commerce/olist_geolocation_dataset.csv',
                          'Brazilian E-Commerce/olist_order_items_dataset.csv',
                          'Brazilian E-Commerce/olist_order_payments_dataset.csv',
                          'Brazilian E-Commerce/olist_order_reviews_dataset.csv',
                          'Brazilian E-Commerce/olist_orders_dataset.csv',
                          'Brazilian E-Commerce/olist_products_dataset.csv',
                          'Brazilian E-Commerce/olist_sellers_dataset.csv',
                          'Brazilian E-Commerce/product_category_name_translation.csv'])

    #DATA CLEANING AND CLEANING RULES APPLIED TO TABLES
    cleaned = data_run_cleaning(tables,TABLE_RULES)

    #PRE-VALIDATION, VALIDATING THE CLEAN DATA AND APPLYING VALIDATION RULES
    data_validated = pre_validation(cleaned,TABLE_RULES)

    # DATA ENRICHMENT , THIS IS WHERE I JOIN TABLES PREPARING THEM FOR BUSINESS LOGIC VALIDATION
    enriched_tables = run_joins(data_validated)
    # AFTER JOIN ARE RUN THE JOIN TABLE IS THEN SAVED INTO THE DATA LAKE AS A PROCESSED TABLE

    # BUSINESS RULES VALIDATION , THIS TABLE WILL THEN BE READ FROM THE DATALAKE , TO RUN THE BUSINESS VALIDATIONS

    BUSINESS_VALIDATED_TABLES = ["order_payments"]

    tables_for_loading = []

    # Load data_validated tables not in enriched
    tables_for_loading += [
        (name, df) for name, df in data_validated.items()
        if name not in enriched_tables
    ]

    # Load enriched tables not in BUSINESS_VALIDATED_TABLES
    tables_for_loading += [
        (name, df) for name, df in enriched_tables.items()
        if name not in BUSINESS_VALIDATED_TABLES
    ]

    # Load business validated tables
    for name in BUSINESS_VALIDATED_TABLES:
        tables_for_loading.append((name, run_validation(enriched_tables[name])))
    #Preparing Data For loading coming soon

    con = create_replace(tables_for_loading)
    confirm_tables(con,tables_for_loading)





if __name__ == "__main__":
    main()