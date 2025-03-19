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
CLEANED_DATA_DIR = "data/cleaned"  # Define 'cleaned' folder similar to 'prepared'

def process_data(filename: str):
    try:
        # Define the reports directory
        REPORTS_DIR = "data/reports"  # Directory where the reports will be saved

        # Load data from file
        df = pd.read_csv(filename)

        # Initialize the DataScrubber with the loaded DataFrame
        scrubber = DataScrubber(df)

        # Step 1: Standardize column names using the DataScrubber
        df = scrubber.standardize_column_names()

        # Step 2: Perform initial consistency check (null counts, duplicates)
        consistency_before = scrubber.check_data_consistency_before_cleaning()
        logger.info(f"Consistency check before cleaning: {consistency_before}")

        # Step 3: Handle missing data (fill or drop)
        df = scrubber.handle_missing_data(drop=False, fill_value=0)  # Example of filling missing data with 0

        # Step 4: Remove duplicate records
        df = scrubber.remove_duplicate_records()

        # Step 5: Perform final consistency check after cleaning
        consistency_after = scrubber.check_data_consistency_after_cleaning()
        logger.info(f"Consistency check after cleaning: {consistency_after}")

        # Generate cleaned file path by maintaining the same folder structure
        cleaned_file_path = os.path.join(
            CLEANED_DATA_DIR, os.path.relpath(filename, RAW_DATA_DIR)
        )
        
        # Ensure the cleaned data directory exists
        os.makedirs(os.path.dirname(cleaned_file_path), exist_ok=True)

        # Save the cleaned data to the new location with "_cleaned" appended to the filename
        df.to_csv(cleaned_file_path, index=False)
        logger.info(f"Cleaned data saved to {cleaned_file_path}")

        # Generate the condensed report of changes made during cleaning
        report = scrubber.generate_report()
  
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