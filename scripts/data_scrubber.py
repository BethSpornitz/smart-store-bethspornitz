"""
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
from scipy import stats
import numpy as np

class DataScrubber:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.report = {}

    def remove_outliers_iqr(self, column_name: str, change_log=None) -> pd.DataFrame:
        if column_name not in self.df.columns:
            if change_log is not None:
                change_log.append(f"Column '{column_name}' not found for IQR outlier removal.")
            return self.df

        Q1 = self.df[column_name].quantile(0.25)
        Q3 = self.df[column_name].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        initial_count = len(self.df)
        self.df = self.df[(self.df[column_name] >= lower_bound) & (self.df[column_name] <= upper_bound)]
        removed = initial_count - len(self.df)

        if removed > 0 and change_log is not None:
            change_log.append(f"Removed {removed} outliers from column '{column_name}' using IQR method.")
    
        return self.df

    def remove_outliers_zscore(self, column_name: str, threshold=3, change_log=None) -> pd.DataFrame:
        if column_name not in self.df.columns:
            if change_log is not None:
                change_log.append(f"Column '{column_name}' not found for Z-score outlier removal.")
            return self.df

        z_scores = stats.zscore(self.df[column_name].dropna())
        abs_z_scores = np.abs(z_scores)
        mask = abs_z_scores <= threshold

    # Keep only rows that are not outliers
        cleaned_df = self.df.loc[self.df[column_name].dropna().index[mask]]
        removed = len(self.df) - len(cleaned_df)

        if removed > 0 and change_log is not None:
            change_log.append(f"Removed {removed} outliers from column '{column_name}' using Z-score method.")

        self.df = cleaned_df
        return self.df


    # Save to report
        self.report['outlier_dropped_rows_zscore'] = dropped_count

        return self.df
        filtered_entries = abs_z_scores < threshold
        valid_indices = col_values.index[filtered_entries]
        outlier_count = len(self.df) - len(valid_indices)
        self.df = self.df.loc[valid_indices]
        self.report['outlier_dropped_rows_zscore'] = outlier_count
        return self.df

    def check_data_consistency_before_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        self.report['null_counts_before'] = null_counts
        self.report['duplicate_count_before'] = duplicate_count
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def check_data_consistency_after_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        self.report['null_counts_after'] = null_counts
        self.report['duplicate_count_after'] = duplicate_count
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    def convert_column_to_new_data_type(self, column: str, new_type: type) -> pd.DataFrame:
        try:
            self.df[column] = self.df[column].astype(new_type)
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def drop_columns(self, columns: List[str]) -> pd.DataFrame:
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df.drop(columns=columns)
        self.report['dropped_columns'] = columns
        return self.df

    def filter_column_outliers(self, column: str, lower_bound: Union[float, int], upper_bound: Union[float, int]) -> pd.DataFrame:
        try:
            self.df = self.df[(self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)]
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def format_column_strings_to_lower_and_trim(self, column: str) -> pd.DataFrame:
        try:
            self.df[column] = self.df[column].str.lower().str.strip()
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def format_column_strings_to_upper_and_trim(self, column: str) -> pd.DataFrame:
        try:
            self.df[column] = self.df[column].str.upper().str.strip()
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def handle_missing_data(self, drop: bool = False, fill_value: Union[None, float, int, str] = None) -> pd.DataFrame:
        if drop:
            self.df = self.df.dropna()
            self.report['missing_data_handling'] = 'Dropped rows with missing data.'
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
            self.report['missing_data_handling'] = f'Filled missing data with {fill_value}.'
            self.report['null_counts_after'] = self.df.isnull().sum()
        return self.df

    def inspect_data(self) -> Tuple[str, str]:
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info_str = buffer.getvalue()
        describe_str = self.df.describe().to_string()
        return info_str, describe_str

    def parse_dates_to_add_standard_datetime(self, column: str) -> pd.DataFrame:
        try:
            self.df['StandardDateTime'] = pd.to_datetime(self.df[column])
            return self.df
        except KeyError:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")

    def remove_duplicate_records(self) -> pd.DataFrame:
        before_count = len(self.df)
        self.df = self.df.drop_duplicates()
        after_count = len(self.df)
        self.report['duplicate_count_removed'] = before_count - after_count
        return self.df

    def rename_columns(self, column_mapping: Dict[str, str]) -> pd.DataFrame:
        for old_name, new_name in column_mapping.items():
            if old_name not in self.df.columns:
                raise ValueError(f"Column '{old_name}' not found in the DataFrame.")
        self.df = self.df.rename(columns=column_mapping)
        return self.df

    def standardize_column_names(self) -> pd.DataFrame:
        self.df.columns = [col.lower().replace(" ", "_") for col in self.df.columns]
        return self.df

    def reorder_columns(self, columns: List[str]) -> pd.DataFrame:
        for column in columns:
            if column not in self.df.columns:
                raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df = self.df[columns]
        return self.df

    def generate_report(self) -> str:
        report = []
        report.append("Null counts before cleaning:\n")
        report.append(str(self.report.get('null_counts_before', 'Not available')))
        report.append("\nNull counts after cleaning:\n")
        report.append(str(self.report.get('null_counts_after', 'Not available')))
        report.append("\nDuplicate counts before cleaning:\n")
        report.append(str(self.report.get('duplicate_count_before', 'Not available')))
        report.append("\nDuplicate counts after cleaning:\n")
        report.append(str(self.report.get('duplicate_count_after', 'Not available')))
        report.append("\nColumns dropped during cleaning:\n")
        report.append(str(self.report.get('dropped_columns', 'None')))
        report.append("\nData types changed:\n")
        report.append(str(self.report.get('changed_data_types', 'None')))
        report.append("\nMissing data handling:\n")
        report.append(str(self.report.get('missing_data_handling', 'None')))
        report.append("\nRows dropped due to outliers:\n")
        report.append(str(self.report.get('outlier_dropped_rows', 'None')))
        report.append("\nRows dropped due to Z-score outliers:\n")
        report.append(str(self.report.get('outlier_dropped_rows_zscore', 'None')))
        return "\n".join(report)
