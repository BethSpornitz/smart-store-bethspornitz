import pandas as pd
import numpy as np
import logging
from scipy import stats
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
PREPARED_DATA_DIR = Path("data/prepared")
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