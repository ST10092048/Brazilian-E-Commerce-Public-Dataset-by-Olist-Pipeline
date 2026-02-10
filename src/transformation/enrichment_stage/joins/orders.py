import pandas as pd

from src.utils.helpers import save
from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='payment_validation')
def join_orders_payments(orders: pd.DataFrame,payments:pd.DataFrame) -> pd.DataFrame:
    try:
        order_payments = orders.merge(
            payments,
            how='left',
            left_on='order_id',
            right_on='order_id'
        )
        save(order_payments, 'processed', 'order_payments_joined')
        return order_payments
    except Exception as e:
        logger.warning(f'Error on join order_payments :{e}')

    return pd.DataFrame()