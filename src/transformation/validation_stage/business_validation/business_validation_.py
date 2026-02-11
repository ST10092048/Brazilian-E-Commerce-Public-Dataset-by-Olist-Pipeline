import pandas as pd

from src.utils.helpers import save


def check_delivery_before_purchase(df: pd.DataFrame) -> pd.DataFrame:
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])

    invalid_delivery = df['order_delivered_customer_date'] < df['order_purchase_timestamp']

    df['is_valid'] = df.get('is_valid', True)
    df.loc[invalid_delivery, 'is_valid'] = False

    df['error_reason'] = df.get('error_reason', '')
    df.loc[invalid_delivery, 'error_reason'] += 'delivery_before_purchase;'
    wrong = df[df['is_valid'] == False]
    # if wrong:
    #     save(wrong,'rejected','delivery_before_purchase')

    return df

def check_order_status(df: pd.DataFrame) -> pd.DataFrame:
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')

    invalid_delivery = (df['order_status'] == 'processing') & df['order_delivered_customer_date'].notna()


    df['is_valid'] = df.get('is_valid', True)
    df.loc[invalid_delivery, 'is_valid'] = False

    df['error_reason'] = df.get('error_reason', '')
    df.loc[invalid_delivery, 'error_reason'] += 'processing_has_delivery_date;'

    return df


