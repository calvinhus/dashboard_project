import requests


def db_structure(connection):
    """This method creates the database and the necessary tables. It takes the connection to the database as a parameter."""

    # Create a cursor
    c = connection.cursor()

    # Create database
    create_db = "CREATE DATABASE IF NOT EXISTS stocks_db;"
    c.execute(create_db)

    # Change database context
    c.execute("USE stocks_db;")

    # Table: URTH
    urth_table = """CREATE TABLE IF NOT EXISTS stocks_db.URTH
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'URTH',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(urth_table)

    # Table: AAPL
    aapl_table = """CREATE TABLE IF NOT EXISTS stocks_db.AAPL
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'AAPL',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(aapl_table)

    # Table: MSFT
    msft_table = """CREATE TABLE IF NOT EXISTS stocks_db.MSFT
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'MSFT',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(msft_table)

    # Table: AMZN
    amzn_table = """CREATE TABLE IF NOT EXISTS stocks_db.AMZN
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'AMZN',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(amzn_table)

    # Table: GOOGL
    googl_table = """CREATE TABLE IF NOT EXISTS stocks_db.GOOGL
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'GOOGL',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(googl_table)

    # Table: TSLA
    tsla_table = """CREATE TABLE IF NOT EXISTS stocks_db.TSLA
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'TSLA',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(tsla_table)

    # Table: FB
    fb_table = """CREATE TABLE IF NOT EXISTS stocks_db.FB
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'FB',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(fb_table)

    # Table: NVDA
    nvda_table = """CREATE TABLE IF NOT EXISTS stocks_db.NVDA
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'NVDA',
                        `price` FLOAT,
                        `marketCap` BIGINT,
                        `change` FLOAT,
                        `changePercent` FLOAT,
                        `open` FLOAT,
                        `close` FLOAT,
                        `week52High` FLOAT,
                        `week52Low` FLOAT,
                        `currency` VARCHAR(4),
                        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(nvda_table)

    # Table: URTH_2yr
    urth_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.URTH_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'URTH',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(urth_2yr_table)

    # Table: AAPL_2yr
    aapl_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.AAPL_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'AAPL',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(aapl_2yr_table)

    # Table: MSFT_2yr
    msft_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.MSFT_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'MSFT',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(msft_2yr_table)

    # Table: AMZN_2yr
    amzn_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.MSFT_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'AMZN',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(amzn_2yr_table)

    # Table: GOOGL_2yr
    googl_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.GOOGL_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'GOOGL',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(googl_2yr_table)

    # Table: TSLA_2yr
    tsla_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.TSLA_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'TSLA',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(tsla_2yr_table)

    # Table: FB_2yr
    fb_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.FB_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'FB',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(fb_2yr_table)

    # Table: NVDA_2yr
    nvda_2yr_table = """CREATE TABLE IF NOT EXISTS stocks_db.NVDA_2yr
                    (   `index` MEDIUMINT NOT NULL AUTO_INCREMENT,
                        `ticker` VARCHAR(5) DEFAULT 'NVDA',
                        `date` DATE,
                        `open` FLOAT,
                        `high` FLOAT,
                        `low` FLOAT,
                        `close` FLOAT,
                        PRIMARY KEY(`index`)
                    );"""
    c.execute(nvda_2yr_table)

    # On success:
    print("\nDatabase and tables created.\n")

    # Close cursor
    c.close()


def db_insert_real_time(connection, ticker, values_insert):
    """This method populates the database tables.
        It takes the connection to the database, the list of tickers 
        and the tuple of values to insert as parameters."""

    # Create a cursor
    c = connection.cursor()

    # Change database context
    c.execute("USE stocks_db;")

    # Build the query
    insert_query = """INSERT INTO stocks_db.""" + ticker + \
        """(`price`,`marketCap`,`change`,`changePercent`,`open`,`close`,`week52High`,`week52Low`,`currency`)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    c.executemany(insert_query, values_insert)

    # Commit the transaction
    print(f"Inserted into {ticker} table.\n")
    connection.commit()

    # Close cursor
    c.close()


def get_historical_data(connection, ticker, iex_api_key):
    """This method creates the database and the necessary tables. It takes the connection to the database as a parameter."""

    # Create a cursor
    c = connection.cursor()

    # Change database context
    c.execute("USE stocks_db;")

    # Build the query
    historic_query = """INSERT INTO stocks_db.""" + ticker + """_2yr""" \
        """(`date`,`open`,`high`,`low`,`close`)
            VALUES(%s, %s, %s, %s, %s)"""

    # request only data for past 2 years
    api_url = f"https://cloud.iexapis.com/stable/stock/{ticker}/chart/2y?token={iex_api_key}"
    # 'https://sandbox.iexapis.com/stable/stock//chart/2y?token='

    response = requests.get(api_url).json()
    values_insert = []
    for i in range(len(response)):
        _date = response[i]['date']
        _open = response[i]['open']
        _high = response[i]['high']
        _low = response[i]['low']
        _close = response[i]['close']

        values_insert.append((_date, _open, _high, _low, _close))
        print('NOW:')
        print(type(values_insert))
        print(values_insert)
        print('done')
        break
        # Execute query
        #c.executemany(historic_query, tuple(values_insert))

        # Commit the transaction
        print(f"Inserted into {ticker}_2yr table.\n")
        # connection.commit()

    # Close cursor
    c.close
