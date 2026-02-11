import pandas as pd

from src.utils.helpers import save
from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='business_validation')
def check_delivery_before_purchase(df: pd.DataFrame) -> pd.DataFrame:
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])

    invalid_delivery = df['order_delivered_customer_date'] < df['order_purchase_timestamp']

    df['is_valid'] = df.get('is_valid', True)
    df.loc[invalid_delivery, 'is_valid'] = False

    df['error_reason'] = df.get('error_reason', '')
    df.loc[invalid_delivery, 'error_reason'] += 'delivery_before_purchase;'
    wrong = df[df['is_valid'] == False]
    if not wrong.empty:
        save(wrong,'rejected','delivery_before_purchase')
        logger.warning(f'delivery_before_purchase rejected')

    return df

def check_order_status(df: pd.DataFrame) -> pd.DataFrame:
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')

    invalid_delivery = (df['order_status'] == 'processing') & df['order_delivered_customer_date'].notna()


    df['is_valid'] = df.get('is_valid', True)
    df.loc[invalid_delivery, 'is_valid'] = False

    df['error_reason'] = df.get('error_reason', '')
    df.loc[invalid_delivery, 'error_reason'] += 'processing_has_delivery_date;'
    wrong = df[df['is_valid'] == False]
    if not wrong.empty:
        save(wrong,'rejected','processing_has_delivery_date')
        logger.warning(f'processing_has_delivery_date rejected')

    return df

def check_delivered_status_with_no_date(df: pd.DataFrame) -> pd.DataFrame:
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')

    invalid_delivery_status = (df['order_status'] == 'delivered') & df['order_delivered_carrier_date'].isna()


    df['is_valid'] = df.get('is_valid', True)
    df.loc[invalid_delivery_status, 'is_valid'] = False

    df['error_reason'] = df.get('error_reason', '')
    df.loc[invalid_delivery_status, 'error_reason'] += 'processing_has_delivery_date;'
    wrong = df[df['is_valid'] == False]
    if not wrong.empty:
        save(wrong,'rejected','invalid_delivery_status')
        logger.warning(f'invalid_delivery_status rejected')

    return df


