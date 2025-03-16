
import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
CLEANED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("cleaned")
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)  # Ensure the cleaned data directory exists

# Expected column names and their standardized versions
COLUMN_STANDARDIZATION = {
    "CustID": "Customer_ID",
    "ProdID": "Product_ID",
    "SaleAmt($)": "Sale_Amount_USD",
    "Date": "Purchase_Date"
}

# Expected data types
EXPECTED_DTYPES = {
    "Customer_ID": "int64",
    "Product_ID": "int64",
    "Sale_Amount_USD": "float64",
    "Purchase_Date": "datetime64"
}

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"Reading raw data from {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs
    
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data by handling missing values, removing duplicates, 
    verifying data types, and standardizing column names."""
    
    if df.empty:
        logger.warning("DataFrame is empty. Skipping cleaning.")
        return df

    # Standardize column names
    df.rename(columns=COLUMN_STANDARDIZATION, inplace=True)
    
    # Remove rows with incorrect number of entries
    df.dropna(how="all", inplace=True)  # Drop entirely empty rows
    
    # Remove duplicates
    initial_size = len(df)
    df.drop_duplicates(inplace=True)
    logger.info(f"Removed {initial_size - len(df)} duplicate rows.")

    # Handle missing, bad, or out-of-range values
    for column in df.columns:
        if column in EXPECTED_DTYPES:
            dtype = EXPECTED_DTYPES[column]
            if dtype.startswith("float") or dtype.startswith("int"):
                df[column] = pd.to_numeric(df[column], errors="coerce")  # Convert, set bad values to NaN
                df.dropna(subset=[column], inplace=True)  # Drop rows with NaN in key numerical columns
            elif dtype == "datetime64":
                df[column] = pd.to_datetime(df[column], errors="coerce")  # Convert, set bad dates to NaT
                df.dropna(subset=[column], inplace=True)  # Drop rows with invalid dates

    # Ensure correct data types
    for column, dtype in EXPECTED_DTYPES.items():
        if column in df.columns:
            df[column] = df[column].astype(dtype)

    logger.info("Data cleaning complete.")
    return df

def process_data(file_name: str) -> None:
    """Process raw data by reading it into a pandas DataFrame object."""
    df = read_raw_data(file_name)

def main() -> None:
    """Main function for processing customer, product, and sales data."""
    logger.info("Starting data preparation...")
    process_data("customers_data.csv")
    process_data("products_data.csv")
    process_data("sales_data.csv")
    logger.info("Data preparation complete.")

if __name__ == "__main__":
    main()