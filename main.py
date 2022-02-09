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


# def get_latest_updates(ticker, iex_api_key):
#     """This method makes the request to the API for each ticker in list
#         and returns the values of the required attributes"""
#     values_insert = []

#     api_url = f'https://cloud.iexapis.com/stable/stock/{ticker}/quote?token={iex_api_key}'

#     df = requests.get(api_url).json()

#     attributes = ['latestPrice', 'marketCap', 'change', 'changePercent',
#                   'open', 'close', 'week52High', 'week52Low', 'currency']
#     for i in attributes:
#         values_insert.append(df[i])

#     return tuple(values_insert)

# Loop through tickers, make the API request and insert the data into the corresponding table
for t in tickers:
    sdb.db_insert_real_time(connection=conn, ticker=t, iex_api_key=iex_api_key)

# Close the connection
conn.close()
