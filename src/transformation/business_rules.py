TABLE_RULES = {
    'olist_customers_dataset': {
        'string_cols': ['customer_city', 'customer_state'],
        'numeric_col': {'customer_zip_code_prefix': 'int'},
        'unique_col': ['customer_unique_id', 'customer_id'],
        'not_null': ['customer_id', 'customer_unique_id'],
        'quality_columns': ['customer_id', 'customer_unique_id'],
    },
    'olist_geolocation_dataset': {
        'string_cols': ['geolocation_city', 'geolocation_state'],
        'numeric_col': {'geolocation_zip_code_prefix': 'int', 'geolocation_lat': 'float', 'geolocation_lng': 'float'},
    },
    'olist_order_items_dataset': {
        'numeric_col': {'order_item_id': 'int', 'price': 'float', 'freight_value': 'float'},
        'unique_col': ['order_id'],
        'not_null': ['order_id', 'order_item_id', 'product_id', 'seller_id', 'price', 'freight_value',
                     'shipping_limit_date'],
        'payment_value': ['price'],
        'quality_columns': ['order_id', 'price', 'product_id', 'seller_id'],
        'enrichment': {
            "add_time_columns": ["shipping_limit_date"],
        }
    },
    'olist_order_payments_dataset': {
        'string_cols': ['payment_type'],
        'numeric_col': {'payment_sequential': 'int', 'payment_installments': 'int', 'payment_value': 'float'},
        'unique_col': ['order_id'],
        'not_null': ['order_id', 'payment_sequential', 'payment_type', 'payment_value', 'payment_installments'],
        'payment_value': ['payment_value'],
        'quality_columns': ['payment_value', 'order_id', ],
    },
    'olist_order_reviews_dataset': {
        'string_cols': ['review_comment_title', 'review_comment_message'],
        'numeric_col': {'review_score': 'int'},
        'unique_col': ['review_id', 'order_id'],
        'not_null': ['review_id', 'order_id'],
        'quality_columns': ['review_id', 'order_id'],
    },
    'olist_orders_dataset': {
        'string_cols': ['order_status'],
        'unique_col': ['order_id', 'customer_id'],
        'not_null': ['order_id', 'customer_id', 'order_status'],
        'quality_columns': ['order_id', 'customer_id', 'order_status'],
        'order_valid':['order_id','customer_id'],
    },
    'olist_products_dataset': {
        'string_cols': ['product_category_name', ],
        'numeric_col': {'product_name_lenght': 'float', 'product_description_lenght': 'float',
                        'product_photos_qty': 'float', 'product_weight_g': 'float',
                        'product_length_cm': 'float', 'product_height_cm': 'float',
                        'product_width_cm': 'float'},
        'unique_col': ['product_id']
    },
    'olist_sellers_dataset': {
        'string_cols': ['seller_city', 'seller_state'],
        'numeric_col': {'seller_zip_code_prefix': 'int'},
        'unique_col': ['seller_id'],
        'not_null': ['seller_id', 'seller_city', 'seller_state'],
        'quality_columns': ['seller_id', 'seller_city', 'seller_state'],

    },
    'product_category_name_translation': {
        'string_cols': ['product_category_name', 'product_category_name_english'],
    }
}
