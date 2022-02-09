import requests
import os
import mysql.connector
import stocks_database as sdb

# Fetch mySQL password from env variable
sql_pass = os.environ["MYSQL_PASSWORD"]

# Fetch IEX api key from env variable
iex_api_key = os.environ['IEX_API_KEY']

# Create list with ticker stocks to request
tickers = ['URTH', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'FB', 'NVDA']

# Create database connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd=sql_pass
)

#sdb.get_historical_data(conn, 'AAPL', iex_api_key)
# get_historic_data('AAPL')
