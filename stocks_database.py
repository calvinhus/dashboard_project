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

    # Success
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
