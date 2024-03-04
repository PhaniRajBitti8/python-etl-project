# Bank Data ETL Project

This project performs ETL (Extract, Transform, Load) operations on data related to the largest banks.

## Description

The project extracts data from a specified URL, transforms it by converting market capitalization to different currencies, and loads the transformed data into both a CSV file and a SQLite database. It also includes functions to run queries on the database.

### Process Overview

1. **Extract**: The data is extracted from a Wikipedia page listing the largest banks.
2. **Transform**: Market capitalization data is transformed into GBP, EUR, and INR using exchange rates from a provided CSV file.
3. **Load**: The transformed data is saved to a CSV file and loaded into a SQLite database.

