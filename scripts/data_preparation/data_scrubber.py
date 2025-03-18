r"""
scripts/data_scrubber.py

Do not run this script directly. 
Instead, from this module (scripts.data_scrubber)
import the DataScrubber class. 

Use it to create a DataScrubber object by passing in a DataFrame with your data. 

Then, call the methods, providing arguments as needed to enjoy common, 
re-usable cleaning and preparation methods. 

See the associated test script in the tests folder. 

"""

import io
import pandas as pd
from typing import Dict, Tuple, Union, List

class DataScrubber:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.report = {}

    # Add standardize_column_names method
    def standardize_column_names(self) -> pd.DataFrame:
        """
        Standardize column names by making them lowercase and replacing spaces with underscores.
        """
        self.df.columns = [col.lower().replace(" ", "_") for col in self.df.columns]
        return self.df

    # Other methods like check_data_consistency_before_cleaning, drop_columns, etc. remain the same

    # Other existing methods ...

    def generate_report(self) -> str:
        """
        Generate a condensed report of changes made during data cleaning.
        
        Returns:
            str: A string summary of the changes made to the DataFrame.
        """
        report = []
        
        # Record null counts before and after cleaning
        report.append("Null counts before cleaning:\n")
        report.append(str(self.report.get('null_counts_before', 'Not available')))

        report.append("\nNull counts after cleaning:\n")
        report.append(str(self.report.get('null_counts_after', 'Not available')))
        
        # Record duplicate counts before and after cleaning
        report.append("\nDuplicate counts before cleaning:\n")
        report.append(str(self.report.get('duplicate_count_before', 'Not available')))
        
        report.append("\nDuplicate counts after cleaning:\n")
        report.append(str(self.report.get('duplicate_count_after', 'Not available')))
        
        # Columns dropped during cleaning (if applicable)
        report.append("\nColumns dropped during cleaning:\n")
        report.append(str(self.report.get('dropped_columns', 'None')))
        
        # Data types changed (if applicable)
        report.append("\nData types changed:\n")
        report.append(str(self.report.get('changed_data_types', 'None')))
        
        # Missing data handling (drop/fill actions)
        report.append("\nMissing data handling:\n")
        report.append(str(self.report.get('missing_data_handling', 'None')))
        
        return "\n".join(report)

    def check_data_consistency_before_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        
        # Save the report data
        self.report['null_counts_before'] = null_counts
        self.report['duplicate_count_before'] = duplicate_count
        
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def check_data_consistency_after_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()

        # Save the report data
        self.report['null_counts_after'] = null_counts
        self.report['duplicate_count_after'] = duplicate_count
        
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def drop_columns(self, columns: List[str]) -> pd.DataFrame:
        dropped_columns = []
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
            dropped_columns.append(column)
        self.df = self.df.drop(columns=columns)
        
        # Save the dropped columns to the report
        self.report['dropped_columns'] = dropped_columns
        return self.df

    def convert_column_to_new_data_type(self, column: str, new_type: type) -> pd.DataFrame:
        old_type = self.df[column].dtype
        self.df[column] = self.df[column].astype(new_type)
        
        # Save the data type change to the report
        self.report['changed_data_types'] = {column: (old_type, new_type)}
        return self.df

    def handle_missing_data(self, drop: bool = False, fill_value: Union[None, float, int, str] = None) -> pd.DataFrame:
        if drop:
            self.df = self.df.dropna()
            self.report['missing_data_handling'] = 'Dropped rows with missing data.'
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
            self.report['missing_data_handling'] = f'Filled missing data with {fill_value}.'
        return self.df