import pandas as pd

from src.utils.helpers import save
from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='join_validation')

def join_orders_payments(tables: dict[str, pd.DataFrame]):

    orders_df = tables.get("olist_orders_dataset")
    payments_df = tables.get("olist_order_payments_dataset")

    if orders_df is None or payments_df is None:
        logger.warning("Missing required tables for orders-payments join")
        return "order_payments", pd.DataFrame()

    order_payments = orders_df.merge(
        payments_df,
        on="order_id",
        how="left"
    )
    save(order_payments, 'processed', 'order_payments_joined')

    return "order_payments", order_payments
