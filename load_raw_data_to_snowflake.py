"""
Kaggle E-Commerce Dataset Ingestion Pipeline

This script downloads an e-commerce dataset from Kaggle and loads it into
Snowflake's raw schema as-is, with only a small column name cleaning because Snowflake
was having issues with some characters.
"""

import os
import re
import kagglehub
import pandas as pd
import snowflake.connector
from pathlib import Path
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas

# Configuration
# In a real project this should be in a configuration file
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': 'vfx_take_home_warehouse',
    'database': 'vfx_take_home_database',
    'schema': 'raw',
    'role': os.getenv('SNOWFLAKE_ROLE')
}


def download_dataset() -> str:
    """
    Download e-commerce dataset from Kaggle using kagglehub.

    Returns:
        str: Absolute path to the downloaded dataset directory.
    """
    path = kagglehub.dataset_download("steve1215rogg/e-commerce-dataset")
    return path


def get_csv_file(dataset_path: str) -> Path:
    """
    Locate the CSV file within the downloaded dataset directory.

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


def clean_column_name(col: str) -> str:
    """
    Minimal cleaning to make column names valid Snowflake identifiers.
    Removes all special characters that Snowflake doesn't accept.

    Args:
        col (str): Original column name.

    Returns:
        str: Cleaned column name.
    """
    # Remove all special characters (parentheses, dots, percent signs, etc.)
    # Keep only alphanumeric characters and underscores
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', col)
    # Remove consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    return cleaned


def load_raw_data(csv_path: Path) -> pd.DataFrame:
    """
    Load CSV data into a pandas DataFrame, clean column names for Snowflake,
    and add an ingestion timestamp.

    Args:
        csv_path (Path): Path to the CSV file.

    Returns:
        pd.DataFrame: Raw dataset with cleaned column names and a LOADED_AT column.
    """
    df = pd.read_csv(csv_path)

    # Basic validation
    assert not df.empty, "Dataset is empty"
    assert df.shape[0] > 0, "No rows found in dataset"

    # Clean column names for Snowflake compatibility
    df.columns = [clean_column_name(col) for col in df.columns]

    # Add ingestion timestamp
    df['LOADED_AT'] = pd.Timestamp.now()

    print(f"\nLoaded raw data:")
    print(f"  • Rows: {df.shape[0]:,}")
    print(f"  • Columns: {list(df.columns)}")

    return df


def create_raw_schema(conn: SnowflakeConnection) -> None:
    """
    Create the raw schema in Snowflake if it does not already exist.

    Args:
        conn (SnowflakeConnection): Active Snowflake connection.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
        print("Schema 'raw' ready")
    finally:
        cursor.close()


def upload_to_snowflake(conn: SnowflakeConnection, df: pd.DataFrame, table_name: str) -> bool:
    """
    Upload a DataFrame to Snowflake as a table in the raw schema.

    Args:
        conn (SnowflakeConnection): Active Snowflake connection.
        df (pd.DataFrame): DataFrame to upload.
        table_name (str): Target table name in Snowflake.

    Returns:
        bool: True if upload succeeds, False otherwise.

    Side Effects:
        - Creates or overwrites a table in the Snowflake database/schema specified
        in SNOWFLAKE_CONFIG.
        - Modifies the remote Snowflake state by inserting rows into the target table.
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
        print(f"Loaded {nrows:,} rows into {table_name}")
    else:
        print(f"Failed to load {table_name}")

    return success


def main() -> None:
    """
    Execute the complete ETL pipeline.

    Pipeline steps:
        1. Download dataset from Kaggle
        2. Load CSV file
        3. Connect to Snowflake
        4. Upload raw data to single table
    """
    # Validate configuration
    if not SNOWFLAKE_CONFIG['password']:
        print("ERROR: SNOWFLAKE_PASSWORD environment variable not set")
        print("Set via: export SNOWFLAKE_PASSWORD='your_password'")
        return

    print("Starting data ingestion pipeline...")

    # Download and load data
    dataset_path = download_dataset()
    print(f"Dataset downloaded to {dataset_path}")

    csv_file = get_csv_file(dataset_path)
    print(f"Found CSV file: {csv_file.name}")

    df = load_raw_data(csv_file)

    # Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print(f"Connected to Snowflake ("
          f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']})")

    try:
        # Load data
        create_raw_schema(conn)
        upload_to_snowflake(conn, df, 'RAW_ECOMMERCE_DATA')

        print("\n" + "="*60)
        print("Pipeline completed successfully!")
        print("Raw data loaded into RAW.RAW_ECOMMERCE_DATA")
        print("Ready for transformation in dbt")
        print("="*60)

    finally:
        conn.close()


if __name__ == "__main__":
    main()