import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3


def log_progress(message):
    """Logs the message of a given stage of the code execution to a log file."""
    time_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open('./banks_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    """Extracts required information from the website and saves it to a data frame."""
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            bank_name = col[1].get_text(strip=True)
            market_cap = float(col[2].get_text(strip=True))
            data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    """Adds columns with transformed market cap to different currencies."""
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    print(df)
    print("The market capitalization of the 5th largest bank in billion EUR")
    print(df['MC_EUR_Billion'][4])
    return df


def load_to_csv(df, output_path):
    """Saves the data frame as a CSV file."""
    df.to_csv(output_path)


def load_to_db(dataframe, sql_connection1, table_name):
    """Saves the data frame to a database table."""
    dataframe.to_sql(table_name, sql_connection1, if_exists='replace', index=False)


def run_query(query_statement, sql_connection1):
    """Runs the query on the database table and prints the output."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection1)
    print(query_output)


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
csv_path = './Largest_banks_data.csv'
table_name = 'Largest_banks'
exchange_rate_csv = './exchange_rate.csv'

log_progress('Preliminaries Completed. Initiating ETL Process')
df = extract(url, table_attribs)
log_progress('Data Extraction Completed. Initiating Transformation Process')
df = transform(df, exchange_rate_csv)
log_progress('Transformation Completed. Loading the file to CSV')
load_to_csv(df, csv_path)
log_progress('Data Saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection Initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table, running the query')

log_progress('Print the contents of the entire table')
query_statement1 = f"SELECT * FROM Largest_banks"
run_query(query_statement1, sql_connection)

log_progress('Print the average market capitalization of all the banks in Billion USD.')
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement2, sql_connection)

log_progress('Print only the names of the top 5 banks')
query_statement3 = f"SELECT Name FROM LARGEST_BANKS LIMIT 5"
run_query(query_statement3, sql_connection)

log_progress('Process Complete')
sql_connection.close()
