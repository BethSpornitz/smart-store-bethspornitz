from data_scrubber import DataScrubber  # Assuming DataScrubber is in a separate module
import pandas as pd
import logging
from pathlib import Path

# Setup for logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("data/raw")
PREPARED_DATA_DIR = Path("data/prepared")
REPORT_DIR = Path("data/reports")

# Define column standardization map and expected data types
COLUMN_STANDARDIZATION = {
    "TransactionID": "Transaction_ID",
    "SaleDate": "Purchase_Date",
    "CustomerID": "Customer_ID",
    "ProductID": "Product_ID",
    "StoreID": "Store_ID",
    "CampaignID": "Campaign_ID",
    "SaleAmount": "Sale_Amount_USD",
    "QuantitySold": "Quantity_Sold",
    "PaymentMethod": "Payment_Method",
    "SalesChannel": "Sales_Channel",
    "ProductName": "Product_Name",
    "Category": "Product_Category",
    "UnitPrice": "Unit_Price_USD",
    "ManufacturingCcost": "Manufacturing_Cost_USD",
    "Brand": "Brand_Name",
    "Name": "Customer_Name",
    "Region": "Customer_Region",
    "JoinDate": "Customer_Join_Date",
    "LifetimeValue": "Customer_Lifetime_Value",
    "CustomerTier": "Customer_Tier"
}

EXPECTED_DTYPES = {
    "Customer_ID": "int64",
    "Product_ID": "int64",
    "Sale_Amount_USD": "float64",
    "Purchase_Date": "datetime64",
    "Quantity_Sold": "int64"
}

def process_data(filename):
    df = pd.read_csv(RAW_DATA_DIR / filename, na_values=["", "N/A", "NULL"])
    change_log = []

    logger.info(f"Raw records count for {filename}: {len(df)}")

    # Determine the required columns based on the file type
    file_requirements = {
        "customers_data.csv": ["Customer_ID", "Customer_Name", "Customer_Region", "Customer_Join_Date", "Customer_Lifetime_Value"],
        "products_data.csv": ["Product_ID", "Product_Name", "Product_Category", "Unit_Price_USD"],
        "sales_data.csv": ["Transaction_ID", "Product_ID", "Customer_ID", "Purchase_Date", "Quantity_Sold", "Sale_Amount_USD"]
    }
    
    required_columns = file_requirements.get(filename)
    if not required_columns:
        logger.error(f"Unknown file type for {filename}. Skipping.")
        return None

    # Initialize DataScrubber
    scrubber = DataScrubber(column_standardization=COLUMN_STANDARDIZATION, expected_dtypes=EXPECTED_DTYPES)

    # Standardize column names
    df = scrubber.standardize_column_names(df)

    # Check for missing required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns {missing_columns} in file {filename}. Skipping.")
        return None

    # Apply cleaning methods from DataScrubber
    df_cleaned = scrubber.clean_data(df, required_columns, change_log)
    
    # Handle outliers if applicable
    if "Quantity_Sold" in df_cleaned.columns:
        df_cleaned = scrubber.filter_column_outliers(df_cleaned, "Quantity_Sold", change_log)

    # Ensure output directories exist
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Save cleaned data
    prepared_filename = PREPARED_DATA_DIR / f"prepared_{Path(filename).name}"
    df_cleaned.to_csv(prepared_filename, index=False)
    logger.info(f"Prepared data saved to {prepared_filename}")

    # Save change log
    if change_log:
        pd.DataFrame({"Change Log": change_log}).to_csv(REPORT_DIR / "cleaning_changes_report.csv", index=False)
        logger.info(f"Data cleaning report saved to {REPORT_DIR / 'cleaning_changes_report.csv'}")

    return df_cleaned

def main():
    for filename in ["customers_data.csv", "products_data.csv", "sales_data.csv"]:
        process_data(filename)

if __name__ == "__main__":
    main()