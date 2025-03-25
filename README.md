# BI Python w/External Packages

## Overview

This script processes raw CSV data files by performing data cleaning operations such as standardizing column names, handling missing data, removing duplicate records, and generating reports on the cleaning process. The cleaned data is saved in a designated directory, and reports are generated to summarize changes made.

## Features

- Installing Required Libraries
- Setting up important files
- Running an initial Python script
- Standardizes column names
- Checks data consistency before and after cleaning
- Handles missing data by filling or dropping values
- Removes duplicate records
- Saves cleaned data into a specified folder
- Generates and saves reports summarizing data cleaning steps
- Logs errors and saves error reports if issues occur


## Installation

Requires installation of:  
pandas  
matplotlib  
loguru
numpy
scipy
sqlite3

```shell

py -m pip install -r requirements.txt

```

## Clone the Repository

```shell
git clone https://github.com/BethSpornitz/smart-store-bethspornitz
```

## Create Project Virtual Environment

On Windows, create a project virtual environment in the .venv folder.

```shell
py -m venv .venv
.venv\Scripts\Activate

```

## Usage

Run the script using:
```shell
python scripts/data_preparation/data_prep2.py

This will process the following raw data files:

data/raw/customers_data.csv

data/raw/products_data.csv

data/raw/sales_data.csv
```

Run the script to create the database using: 
```shell
python scripts/etl_to_dw.py

This will create the database using the prepared files
'''

## Output

Cleaned files will be saved in data/prepared/.

Reports summarizing data cleaning steps will be saved in data/reports/.

Error reports (if any issues occur) will also be saved in data/reports/.

Using etl_to db script:  Database with prepared data will be populated.

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


## Git add and commit

```shell
git add .
git commit -m "add .gitignore, cmds to readme"
git push origin main
```
