import pandas as pd
from common_utils import standardize_column_names, clean_data, handle_outliers_zscore, COLUMN_STANDARDIZATION
from pathlib import Path
import logging

# Setup for logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define file paths
RAW_DATA_DIR = Path("data/raw")
PREPARED_DATA_DIR = Path("data/prepared")
REPORT_DIR = Path("data/reports")

def process_product_data(filename):
    df = pd.read_csv(RAW_DATA_DIR / filename, na_values=["", "N/A", "NULL"])
    change_log = []

    logger.info(f"Raw records count for {filename}: {len(df)}")

    required_columns = ["Product_ID", "Product_Name", "Product_Category", "Unit_Price_USD"]

    # Standardize column names
    df = standardize_column_names(df, COLUMN_STANDARDIZATION)

    # Clean data
    df_cleaned = clean_data(df, required_columns, change_log)

    logger.info(f"Cleaned records count for {filename}: {len(df_cleaned)}")

    # Ensure the output directories exist
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate prepared filename
    prepared_filename = PREPARED_DATA_DIR / f"prepared_{filename}"

    # Save the cleaned product data
    df_cleaned.to_csv(prepared_filename, index=False)
    logger.info(f"Prepared product data saved to {prepared_filename}")

    if change_log:
        change_log_df = pd.DataFrame({"Change Log": change_log})
        change_log_df.to_csv(REPORT_DIR / "product_cleaning_changes_report.csv", index=False)
        logger.info(f"Product data cleaning report saved.")

def main():
    process_product_data("products_data.csv")

if __name__ == "__main__":
    main()