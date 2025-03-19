# BI Python w/External Packages

## Overview

This script processes raw CSV data files by performing data cleaning operations such as standardizing column names, handling missing data, removing duplicate records, and generating reports on the cleaning process. The cleaned data is saved in a designated directory, and reports are generated to summarize changes made.

## Features

- Installing Required Libraries
- Setting up important files
- Running an initial Python script
  -Standardizes column names
  -Checks data consistency before and after cleaning
  -Handles missing data by filling or dropping values
  -Removes duplicate records
  -Saves cleaned data into a specified folder
  -Generates and saves reports summarizing data cleaning steps
  -Logs errors and saves error reports if issues occur

## Project Structure

project_root/
│── data/
│ ├── raw/ # Contains raw CSV files
│ ├── cleaned/ # Stores cleaned data files
│ ├── reports/ # Stores generated reports
│── scripts/
│ ├── data_scrubber.py # DataScrubber class
│ ├── data_preparation/
│ │ ├── data_prep2.py # Main script for processing data
│── README.md

## Installation

Requires installation of:  
pandas  
matplotlib  
loguru

## Clone the Repository

git clone https://github.com/BethSpornitz/smart-store-bethspornitz

## Create Project Virtual Environment

On Windows, create a project virtual environment in the .venv folder.

## Usage

Run the script using:

python scripts/data_preparation/data_prep2.py

This will process the following raw data files:

data/raw/customers_data.csv

data/raw/products_data.csv

data/raw/sales_data.csv

## Output

Cleaned files will be saved in data/cleaned/.

Reports summarizing data cleaning steps will be saved in data/reports/.

Error reports (if any issues occur) will also be saved in data/reports/.

## Logging

The script logs important steps and errors using Python's logging module. Log messages include:

Consistency check results

Paths of saved cleaned files

Report file paths

Error messages (if any occur during processing)

## Dependencies

Python 3.x

pandas

logging

unittest

```shell

py -m venv .venv
.venv\Scripts\Activate
py -m pip install -r requirements.txt

```

## Git add and commit

```shell
git add .
git commit -m "add .gitignore, cmds to readme"
git push origin main
```
