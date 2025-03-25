import logging
import os
import unittest
from pathlib import Path
import sys
from io import StringIO
import pandas as pd

# Import DataScrubber from the scripts module
from scripts.data_scrubber import DataScrubber  # noqa: E402


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    "Purchase_Date":"datetime64[ns]",
    "Quantity_Sold":"int64"
}

# Define file paths
RAW_DATA_DIR = Path("data/raw")
PREPARED_DATA_DIR = Path("data/prepared")
REPORT_DIR = Path("data/report")

# Ensure the output directories exist
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

def handle_outliers_zscore(df, column, threshold=3, change_log=None):
    if column not in df.columns:
        logger.warning(f"Column {column} not found in the DataFrame. Skipping outlier handling.")
        return df

    z_scores = stats.zscore(df[column].dropna())
    abs_z_scores = np.abs(z_scores)
    outliers = abs_z_scores > threshold

    outlier_count = outliers.sum()
    if outlier_count > 0:
        if change_log is not None:
            change_log.append(f"Dropped {outlier_count} outliers from column '{column}' using Z-score method.")
        df = df[~outliers]
    return df

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
        df = scrubber.handle_missing_data(drop=True, fill_value=0)  # Example of filling missing data with 0
        change_log.append("Filled missing data with 0.")

        # Step 4: Remove duplicate records
        df = scrubber.remove_duplicate_records()
        change_log.append("Removed duplicate records.")

  # Step 5: Remove rows with outliers in 'Quantity_Sold'
  
     # Handle outliers if applicable
        if "quantity_sold" in df.columns:
        	df = scrubber.remove_outliers_zscore("quantity_sold", threshold=3, change_log=change_log)

        #df = scrubber.remove_outliers_iqr("quantity_sold", change_log=change_log)


        # Step 6: Perform final consistency check after cleaning
        consistency_after = scrubber.check_data_consistency_after_cleaning()
        logger.info(f"Consistency check after cleaning: {consistency_after}")
        change_log.append(f"Consistency after cleaning: {consistency_after}")

        # Step 7:  Generate cleaned file path by maintaining the same folder structure
        original_stem = Path(filename).stem
        new_filename = f"prepared_{original_stem}.csv"
        prepared_file_path = PREPARED_DATA_DIR / new_filename

        
        # Ensure the cleaned data directory exists
        os.makedirs(os.path.dirname(prepared_file_path), exist_ok=True)

        # Save the cleaned data to the new location with "_prepared" appended to the filename
        df.to_csv(prepared_file_path, index=False)
        logger.info(f"Cleaned data saved to {prepared_file_path}")



        # Step 8:  Generate the condensed report of changes made during cleaning
        report = "\n".join(change_log)
     
  
        # Save the report to the reports folder
        
        report_filename = Path(REPORT_DIR) / f"{Path(filename).stem}_report.txt"
        with open(report_filename, "w") as report_file:
            report_file.write(report)
        logger.info(f"Data cleaning report saved to {report_filename}")


    except Exception as e:
        logger.error(f"An error occurred: {e}")

        # Save the error message to a report file
        error_report = f"An error occurred during data cleaning: {str(e)}"
        error_report_filename = os.path.join(REPORT_DIR, f"{os.path.basename(filename).replace('.csv', '_error_report.txt')}")
        os.makedirs(REPORT_DIR, exist_ok=True)  # Ensure the reports folder exists
        with open(error_report_filename, "w") as error_report_file:
            error_report_file.write(error_report)
        logger.info(f"Error report saved to {error_report_filename}")

def main():
    raw_data_dir = Path("data/raw")
    if not raw_data_dir.exists():
        logger.error(f"Directory not found: {raw_data_dir}")
        return

    for file_path in raw_data_dir.glob("*.csv"):
        logger.info(f"Processing file: {file_path.name}")
        try:
            process_data(file_path)
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")

if __name__ == "__main__":
    main()