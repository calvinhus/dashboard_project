#!/usr/bin/python3

import requests
import os
import mysql.connector
import stocks_database as sdb

# Fetch mySQL password from env variable
sql_pass = os.environ['MYSQL_PASSWORD']

# Fetch IEX api key from env variable
iex_api_key = os.environ['IEX_API_KEY']

# Create list with ticker stocks to make the requests
tickers = ['URTH', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'FB', 'NVDA']

# Create database connection
conn = mysql.connector.connect(
    host="localhost",
    user="calvinho",
    passwd=sql_pass
)


# Loop through tickers, make the API request and insert the data into the corresponding table
for t in tickers:
    sdb.db_insert_real_time(connection=conn, ticker=t, iex_api_key=iex_api_key)

# Close the connection
conn.close()
