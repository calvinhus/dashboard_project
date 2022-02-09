import os
import mysql.connector
import stocks_database as sdb

# Fetch mySQL password from env variable
sql_pass = os.environ["MYSQL_PASSWORD"]

# Fetch IEX api key from env variable
iex_api_key = os.environ['IEX_API_KEY']

# Create list with ticker stocks to make the requests
#tickers = ['URTH', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'FB', 'NVDA']
tickers = ['AMZN', 'GOOGL', 'TSLA', 'FB', 'NVDA']
# Create database connection
conn = mysql.connector.connect(
    host="localhost",
    user="calvinho",
    passwd=sql_pass
)

# Call method to create database and all tables (if they don't already exist)
sdb.db_structure(conn)

# Populate database with historical data (last 2 years only)
for t in tickers:
    sdb.get_historical_data(conn, t, iex_api_key)
    print(f"Inserted historical data in {t}_2yr table.")

# Close the connection
conn.close()
