import duckdb

from src.utils.logger import get_logger

logger = get_logger(__name__,module_name='warehouse_loading')
def create_replace(tables: list[tuple[str, object]]):
    con = duckdb.connect("ecommerce_analytics.duckdb")
    if con is None:
        return logger.warning("Could not connect to duckdb")

    for name, df in tables:

        clean_name =clean_table_names(name)

        logger.info(f"Loading staging table: stg_{clean_name}")
        # Register dataframe
        con.register(clean_name, df)

        # Create or replace staging table
        con.execute(f"""
            CREATE OR REPLACE TABLE stg_{clean_name} AS
            SELECT * FROM {clean_name}
        """)
        logger.info(f"Created table: {clean_name}")

        con.unregister(clean_name)


    logger.info("all staging tables refreshed.")
    return con


def confirm_tables(con, tables: list[tuple[str, object]]):
    for name, _ in tables:  # unpack the tuple
        clean_name = clean_table_names(name)
        staging_name = f"stg_{clean_name}"  # include 'stg_' prefix
        count = con.execute(f"SELECT COUNT(*) FROM {staging_name}").fetchone()[0]
        if count <= 0:
            logger.warning(f"Table is invalid {staging_name}: {count} rows")
        else:
            logger.info(f"Table is valid {staging_name}: {count} rows")



def clean_table_names(name):
    clean_name = name
    if clean_name.startswith("olist_"):
        clean_name = clean_name[len("olist_"):]
    if clean_name.endswith("_dataset"):
        clean_name = clean_name[:-len("_dataset")]
    return clean_name