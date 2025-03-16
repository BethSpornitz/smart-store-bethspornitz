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

def process_sales_data(filename):
    df = pd.read_csv(RAW_DATA_DIR / filename, na_values=["", "N/A", "NULL"])
    change_log = []

    logger.info(f"Raw records count for {filename}: {len(df)}")

    required_columns = [
        "Transaction_ID", "Product_ID", "Customer_ID", 
        "Purchase_Date", "Quantity_Sold", "Sale_Amount_USD"
    ]

    # Standardize column names
    df = standardize_column_names(df, COLUMN_STANDARDIZATION)

    # Clean data
    df_cleaned = clean_data(df, required_columns, change_log)

    logger.info(f"Cleaned records count for {filename}: {len(df_cleaned)}")

    # Handle outliers in Quantity_Sold
    if "Quantity_Sold" in df_cleaned.columns:
        df_cleaned = handle_outliers_zscore(df_cleaned, "Quantity_Sold", threshold=3, change_log=change_log)

    # Ensure the output directories exist
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate prepared filename
    prepared_filename = PREPARED_DATA_DIR / f"prepared_{filename}"

    # Save the cleaned sales data
    df_cleaned.to_csv(prepared_filename, index=False)
    logger.info(f"Prepared sales data saved to {prepared_filename}")

    if change_log:
        change_log_df = pd.DataFrame({"Change Log": change_log})
        change_log_df.to_csv(REPORT_DIR / "sales_cleaning_changes_report.csv", index=False)
        logger.info(f"Sales data cleaning report saved.")

def main():
    process_sales_data("sales_data.csv")

if __name__ == "__main__":
    main()