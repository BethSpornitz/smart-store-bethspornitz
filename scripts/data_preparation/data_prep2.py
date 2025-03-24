import logging
import os
import unittest
from pathlib import Path
import sys
from io import StringIO
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import DataScrubber from the scripts module
from scripts.data_scrubber import DataScrubber  # noqa: E402


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define paths
RAW_DATA_DIR = "data/raw"
PREPARED_DATA_DIR = "data/prepared"
REPORTS_DIR = "data/reports"  

# Constants for standardization and expected data types
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
    "SalesChannel": "Sales_Channel"
}

COLUMN_STANDARDIZATION.update({
    "ProductName": "Product_Name",
    "Category": "Product_Category",
    "UnitPrice": "Unit_Price_USD",
    "ManufacturingCcost": "Manufacturing_Cost_USD",
    "Brand": "Brand_Name"
})

COLUMN_STANDARDIZATION.update({
    "Name": "Customer_Name",
    "Region": "Customer_Region",
    "JoinDate": "Customer_Join_Date",
    "LifetimeValue": "Customer_Lifetime_Value",
    "CustomerTier": "Customer_Tier"
})

EXPECTED_DTYPES = {
    "Customer_ID": "int64",
    "Product_ID": "int64",
    "Sale_Amount_USD": "float64",
    "Purchase_Date": "datetime64[ns]",
    "Quantity_Sold": "int64"
}


def process_data(filename: str):
    try:

        # Load data from file
        df = pd.read_csv(filename)

         # Standardize column names
        df = df.rename(columns=COLUMN_STANDARDIZATION)

        # Convert columns to the expected data types
        for column, dtype in EXPECTED_DTYPES.items():
            if column in df.columns:
                df[column] = df[column].astype(dtype)

        # Initialize the DataScrubber with the loaded DataFrame
        scrubber = DataScrubber(df)

         # Initialize change log
        change_log = []

        # Step 1: Standardize column names using the DataScrubber
        df = scrubber.standardize_column_names()
        change_log.append("Standardized column names.")

        # Step 2: Perform initial consistency check (null counts, duplicates)
        consistency_before = scrubber.check_data_consistency_before_cleaning()
        logger.info(f"Consistency check before cleaning: {consistency_before}")
        change_log.append(f"Consistency before cleaning: {consistency_before}")

        # Step 3: Handle missing data (fill or drop)
        df = scrubber.handle_missing_data(drop=False, fill_value=0)  # Example of filling missing data with 0
        change_log.append("Filled missing data with 0.")

        # Step 4: Remove duplicate records
        df = scrubber.remove_duplicate_records()
        change_log.append("Removed duplicate records.")

        # Step 5:  Remove rows with outliers
        #df = scrubber.remove_outliers()
        #change_log.append("Removed rows with outliers.")

        # Step 6: Perform final consistency check after cleaning
        consistency_after = scrubber.check_data_consistency_after_cleaning()
        logger.info(f"Consistency check after cleaning: {consistency_after}")
        change_log.append(f"Consistency after cleaning: {consistency_after}")

        # Step 7:  Generate cleaned file path by maintaining the same folder structure
        prepared_file_path = os.path.join(
            PREPARED_DATA_DIR, os.path.relpath(filename, RAW_DATA_DIR)
        )
        
        # Ensure the cleaned data directory exists
        os.makedirs(os.path.dirname(prepared_file_path), exist_ok=True)

        # Save the cleaned data to the new location with "_prepared" appended to the filename
        df.to_csv(prepared_file_path, index=False)
        logger.info(f"Cleaned data saved to {prepared_file_path}")



        # Step 8:  Generate the condensed report of changes made during cleaning
        report = "\n".join(change_log)
     
  
        # Save the report to the reports folder
        
        report_filename = Path(REPORTS_DIR) / f"{Path(filename).stem}_report.txt"
        with open(report_filename, "w") as report_file:
            report_file.write(report)
        logger.info(f"Data cleaning report saved to {report_filename}")


    except Exception as e:
        logger.error(f"An error occurred: {e}")

        # Save the error message to a report file
        error_report = f"An error occurred during data cleaning: {str(e)}"
        error_report_filename = os.path.join(REPORTS_DIR, f"{os.path.basename(filename).replace('.csv', '_error_report.txt')}")
        os.makedirs(REPORTS_DIR, exist_ok=True)  # Ensure the reports folder exists
        with open(error_report_filename, "w") as error_report_file:
            error_report_file.write(error_report)
        logger.info(f"Error report saved to {error_report_filename}")

def main():
    for filename in ["data/raw/customers_data.csv", "data/raw/products_data.csv", "data/raw/sales_data.csv"]:
        process_data(filename)

if __name__ == "__main__":
    main()