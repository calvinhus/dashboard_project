import os
import mysql.connector
import stocks_database as sdb

# Fetch mySQL password from env variable
sql_pass = os.environ["MYSQL_PASSWORD"]


# Create database connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd=sql_pass
)

# Call method to create database and all tables
sdb.db_structure(conn)

# Close the connection
conn.close()
