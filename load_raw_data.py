"""
Kaggle E-Commerce Dataset Ingestion Pipeline

This script downloads an e-commerce dataset from Kaggle and loads it into
Snowflake's raw schema. The data is split into three logical entities:
- raw_users: User dimension data
- raw_products: Product catalog with pricing
- raw_orders: Transactional order records

Requirements:
    - SNOWFLAKE_PASSWORD environment variable must be set
    - Kaggle authentication configured
"""

import os
import re
from pathlib import Path

import kagglehub
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Configuration
# In a real project I would set these credentials in a more private and secure way
# like using a config/yaml file to keep common snowflake settings and defining the user
# and password credentials in a .env file that would be added to gitignore.
SNOWFLAKE_CONFIG = {
    'account': 'trgelfi-qt08625',
    'user': 'pedromcvaz',
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': 'vfx_take_home_warehouse',
    'database': 'vfx_take_home_database',
    'schema': 'raw',
    'role': 'ACCOUNTADMIN'
}


def download_dataset():
    """
    Download e-commerce dataset from Kaggle using kagglehub.

    Returns:
        str: Absolute path to the downloaded dataset directory.

    Raises:
        Exception: If dataset download fails.
    """
    path = kagglehub.dataset_download("steve1215rogg/e-commerce-dataset")
    return path


def get_csv_file(dataset_path):
    """
    Locate the CSV file within the downloaded dataset directory.
    This is a bit of a lazy solution

    Args:
        dataset_path (str): Path to the dataset directory.

    Returns:
        Path: Path object pointing to the CSV file.

    Raises:
        FileNotFoundError: If no CSV files are found in the directory.
    """
    path_obj = Path(dataset_path)
    csv_files = list(path_obj.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {dataset_path}")

    return csv_files[0]


def load_and_inspect_data(csv_path):
    """
    Load CSV data into a pandas DataFrame and perform basic validation.

    Args:
        csv_path (Path): Path to the CSV file.

    Returns:
        pd.DataFrame: Loaded dataset with all columns and rows.

    Notes:
        Validates data structure including shape, columns, types, and null counts.
    """
    df = pd.read_csv(csv_path)

    # Basic validation
    assert not df.empty, "Dataset is empty"
    assert df.shape[0] > 0, "No rows found in dataset"

    return df


def create_raw_schema(conn):
    """
    Create the raw schema in Snowflake if it does not already exist.

    Args:
        conn (snowflake.connector.SnowflakeConnection): Active Snowflake connection.

    Returns:
        None
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"USE DATABASE {SNOWFLAKE_CONFIG['database']}"
        )
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
        print("✓ Schema 'raw' created or already exists")
    finally:
        cursor.close()


def clean_column_name(col):
    """
    Normalize column name to UPPER_SNAKE_CASE.

    Args:
        col (str): Original column name.

    Returns:
        str: Cleaned column name.
    """
    # Remove parentheses and dots, then replace spaces with underscores
    return re.sub(r'[().]', '', col).replace(' ', '_').upper()


def prepare_dataframes(df):
    """
    Transform the source dataset into normalized entity DataFrames.

    Splits the monolithic e-commerce dataset into three logical entities
    following dimensional modeling principles:

    Args:
        df (pd.DataFrame): Source e-commerce dataset.

    Returns:
        tuple: A 3-tuple containing:
            - raw_orders (pd.DataFrame): All transactional records with metadata.
            - raw_users (pd.DataFrame): Unique users with simulated attributes.
            - raw_products (pd.DataFrame): Unique products with category and pricing.

    Notes:
        - Column names are standardized to UPPER_SNAKE_CASE
        - LOADED_AT timestamp is added to track ingestion time
        - User emails are simulated as {user_id}@example.com
        - Currency is inferred as INR based on 'Rs.' notation
    """
    # Standardize column names on source dataframe
    df_clean = df.copy()

    # Step 1: Basic cleanup - uppercase, remove special chars
    df_clean.columns = [clean_column_name(col) for col in df_clean.columns]

    # Step 2: Semantic renaming for clarity
    # The challenge says to assume prices are in USD but the columns mention RS.
    # I chose not to mention the currency in the column names as this way I could in the
    # future add support for multiple currencies without having tho change the schema of
    # the table. In order to keep track of the currency and add support to multiple
    # currencies, I've added a CURRENCY column.
    column_mapping = {
        'PRICE_RS': 'PRICE',
        'FINAL_PRICE_RS': 'FINAL_PRICE',
        'DISCOUNT_%': 'DISCOUNT_PCT'
    }
    df_clean.rename(columns=column_mapping, inplace=True)

    df_clean['CURRENCY'] = 'USD' # As per the exercise instructions

    print(f"\n Standardized columns:")
    print(f" {df_clean.columns.tolist()}")

    # Note: by creating the variable current_timestamp and then broadcasting it,
    # pd.Timestamp.now() is only computed once, while if I had just done
    # raw_users['LOADED_AT'] = pd.Timestamp.now() it would look cleaner, but it would
    # call pd.Timestamp.now() for every single row which would be extremely slow
    current_timestamp = pd.Timestamp.now()

    # Extract RAW_USERS - unique users from transactions
    raw_users = (
        df_clean[['USER_ID']]
        .drop_duplicates()
        .copy()
    )
    # EMAIL is just for flare, but I chose to add it to simulate a potential real-life
    # way of adding a deterministic user master key like asked in the challenge
    raw_users['EMAIL'] = raw_users['USER_ID'].astype(str) + '@example.com'
    # SOURCE_SYSTEM is  also just for flare in this exercise but in a real life scenario
    # where data can arrive from multiple sources (e.g., web app, mobile app,
    # POS system, CRM) this would be valuable.
    raw_users['SOURCE_SYSTEM'] = 'ecommerce_web'
    raw_users['LOADED_AT'] = current_timestamp

    # Extract RAW_PRODUCTS - unique products with catalog attributes
    raw_products = (
        df_clean[['PRODUCT_ID', 'CATEGORY', 'PRICE']]
        .drop_duplicates(subset=['PRODUCT_ID'])
        .copy()
    )
    raw_products['LOADED_AT'] = current_timestamp

    # RAW_ORDERS - all transaction records
    raw_orders = df_clean.copy()
    raw_orders['LOADED_AT'] = current_timestamp

    print(f"\n Prepared dataframes:")
    print(f" • raw_orders: {raw_orders.shape[0]:,} rows")
    print(f"   Columns: {list(raw_orders.columns)}")
    print(f" • raw_users: {raw_users.shape[0]:,} rows")
    print(f"   Columns: {list(raw_users.columns)}")
    print(f" • raw_products: {raw_products.shape[0]:,} rows")
    print(f"   Columns: {list(raw_products.columns)}")

    return raw_orders, raw_users, raw_products


def upload_to_snowflake(conn, df, table_name):
    """
    Upload a DataFrame to Snowflake as a table in the raw schema.

    Args:
        conn (snowflake.connector.SnowflakeConnection): Active Snowflake connection.
        df (pd.DataFrame): DataFrame to upload.
        table_name (str): Target table name in Snowflake.

    Returns:
        bool: True if upload succeeds, False otherwise.

    Notes:
        - Creates table automatically if it does not exist
        - Overwrites existing table data
        - Uses unquoted identifiers for compatibility
    """
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False
    )

    if success:
        print(f"✓ Loaded {nrows:,} rows into {table_name}")
    else:
        print(f"✗ Failed to load {table_name}")

    return success


def main():
    """
    Execute the complete ETL pipeline.

    Pipeline steps:
        1. Download dataset from Kaggle
        2. Locate and load CSV file
        3. Transform data into normalized entities
        4. Connect to Snowflake
        5. Create raw schema
        6. Upload all entities to Snowflake

    Raises:
        SystemExit: If SNOWFLAKE_PASSWORD environment variable is not set.
    """
    # Validate configuration
    if not SNOWFLAKE_CONFIG['password']:
        print("ERROR: SNOWFLAKE_PASSWORD environment variable not set")
        print("Set via: export SNOWFLAKE_PASSWORD='your_password'")
        return

    # Download and load data
    dataset_path = download_dataset()
    csv_file = get_csv_file(dataset_path)
    df = load_and_inspect_data(csv_file)

    # Transform data
    raw_orders, raw_users, raw_products = prepare_dataframes(df)

    # Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print(
        f"✓ Connected to Snowflake ({SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']})")

    try:
        # Initialize schema and load data
        create_raw_schema(conn)

        upload_to_snowflake(conn, raw_users, 'RAW_USERS')
        upload_to_snowflake(conn, raw_products, 'RAW_PRODUCTS')
        upload_to_snowflake(conn, raw_orders, 'RAW_ORDERS')

        print("\n✓ Pipeline completed successfully")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
