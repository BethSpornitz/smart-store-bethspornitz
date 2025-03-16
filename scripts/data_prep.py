import pandas as pd
import numpy as np
from scipy import stats
import logging
from pathlib import Path


# Setup for logging
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
    "Purchase_Date": "datetime64",
    "Quantity_Sold": "int64"
}

# Define file paths
RAW_DATA_DIR = Path("data/raw")
CLEANED_DATA_DIR = Path("data/cleaned")
REPORT_DIR = Path("data/reports")

def standardize_column_names(df, standardization_map):
    logger.info(f"Initial columns: {df.columns.tolist()}")
    df.columns = [standardization_map.get(col, col) for col in df.columns]
    df.columns = df.columns.str.strip()
    logger.info(f"Columns after standardization: {df.columns.tolist()}")
    return df

def clean_data(df, required_columns, change_log=None):
    initial_size = len(df)

    # Log rows with missing values before dropping them
    rows_with_missing_values = df[df.isna().any(axis=1)]
    logger.info(f"Rows with missing values before dropna:\n{rows_with_missing_values}")

    # Drop rows with any missing values across all columns
    df.dropna(how="any", inplace=True)
    dropped_rows = initial_size - len(df)
    logger.info(f"Rows after NaN removal: {len(df)}")

    if change_log is not None and dropped_rows > 0:
        change_log.append(f"Dropped {dropped_rows} rows due to missing values.")

    # Drop completely duplicate rows
    duplicates_before = len(df)
    df.drop_duplicates(inplace=True)
    duplicates_after = len(df)
    if duplicates_before > duplicates_after:
        if change_log is not None:
            change_log.append(f"Dropped {duplicates_before - duplicates_after} duplicate rows.")

    # Ensure correct data types for specified columns
    for column, dtype in EXPECTED_DTYPES.items():
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    logger.info(f"Data types after conversion: {df.dtypes.to_dict()}")

    return df

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

def process_data(filename):
    # Define missing value representations and read the file
    df = pd.read_csv(RAW_DATA_DIR / filename, na_values=["", "N/A", "NULL"])
    change_log = []

    # Identify file type and required columns
    if filename == "customers_data.csv":
        required_columns = ["Customer_ID", "Customer_Name", "Customer_Region", "Customer_Join_Date", "Customer_Lifetime_Value"]
    elif filename == "products_data.csv":
        required_columns = ["Product_ID", "Product_Name", "Product_Category", "Unit_Price_USD"]
    elif filename == "sales_data.csv":
        required_columns = ["Transaction_ID", "Product_ID", "Customer_ID", "Purchase_Date", "Quantity_Sold", "Sale_Amount_USD"]
    else:
        logger.error(f"Unknown file type for {filename}. Skipping.")
        return None

    # Proceed with standardization and cleaning
    df = standardize_column_names(df, COLUMN_STANDARDIZATION)

    # Log rows with missing values before dropna
    logger.info(f"Rows with missing values in required columns: \n{df[df[required_columns].isna().any(axis=1)]}")

    # Check for missing required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns {missing_columns} in file {filename}. Skipping.")
        return None

    # Clean data
    df_cleaned = clean_data(df, required_columns, change_log)

    # Handle outliers if applicable
    if "Quantity_Sold" in df_cleaned.columns:
        df_cleaned = handle_outliers_zscore(df_cleaned, "Quantity_Sold", threshold=3, change_log=change_log)

    # Ensure the output directories exist
    CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Save cleaned data to file
    cleaned_filename = CLEANED_DATA_DIR / f"cleaned_{filename}"
    df_cleaned.to_csv(cleaned_filename, index=False)
    logger.info(f"Cleaned data saved to {cleaned_filename}")

    # Create a summary report of changes
    if change_log:
        change_log_df = pd.DataFrame({"Change Log": change_log})
        change_log_df.to_csv(REPORT_DIR / "cleaning_changes_report.csv", index=False)
        logger.info(f"Data cleaning report saved to {REPORT_DIR / 'cleaning_changes_report.csv'}")

    return df_cleaned

def main():
    for filename in ["customers_data.csv", "products_data.csv", "sales_data.csv"]:
        process_data(filename)

if __name__ == "__main__":
    main()