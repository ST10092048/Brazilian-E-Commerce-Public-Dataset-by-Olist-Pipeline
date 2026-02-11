from src.transformation.validation_stage.business_validation.business_validation_ import check_delivery_before_purchase, \
    check_order_status, check_delivered_status_with_no_date
from src.transformation.validation_stage.data_validation.data_validation import data_validation


def pre_validation(cleaned, TABLE_RULES):
    # Data Validation
    df = data_validation(cleaned, TABLE_RULES)
    return df


def run_validation(df):

    #Business Logic
    validation_steps = [
        check_delivery_before_purchase,
        check_order_status,
        check_delivered_status_with_no_date
    ]

    for step in validation_steps:
        df = step(df)

    return df
