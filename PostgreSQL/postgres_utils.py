import psycopg2
from psycopg2 import sql
import logging
from configparser import ConfigParser
from pyspark.sql import DataFrame
import os

def load_config(filename: str ='/Users/jonathankyle/PycharmProjects/Udacity_DE_Capstone/PostgreSQL/config.ini', section: str ='postgresql') -> dict:
    """Connect to PostgreSQL database using the parameters in a .ini file
       Arguments:
           filename: name of config file, defaults to 'config.ini'
           section: name of relevant section in config file. Defaults to 'postgresql'
        Returns:
            config dictionary"""
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

def connect(config):
    """ Connect to the PostgreSQL database server
        Arguments:
            config: the PostgreSQL config as returned by load_config
        Returns:
            conn: pyscopg2 conection object"""
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            logging.info('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def run_sql_commands(commands: tuple | str):
    """Connects to the PostgreSQL server and run a series of SQL commands
       Arguments:
           commands: a tuple of SQL commands in triple quotes, or a string with a single command in triple quotes"""
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        config = load_config()
        connection = connect(config)
        with connection as conn:
            with conn.cursor() as cur:
                if isinstance(commands, tuple):
                    for command in commands:
                        logging.info(f"Running: {command}")
                        cur.execute(command)
                elif isinstance(commands, str):
                    logging.info(f"Running: {commands}")
                    cur.execute(commands)
            conn.commit()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

    logging.info("All commands executed successfully")

def insert_tuple_to_table(table_schema: str, table_name: str , values: list):
    """Connects to the PostgreSQL server and run a series of insert SQL commands. Inserts the values held in a list of tuples
       into the columns found in the specified table
       Arguments:
           table_schema: schema of table to insert into
           table_name: name of table to insert into
           values: values to be inserted. Must be a list of tuples, where each tuple represents one row in the correct order of columns."""
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        config = load_config()
        connection = connect(config)
        with connection as conn:
            with conn.cursor() as cur:
                query = sql.SQL("select * from {} limit 0").format(
                    sql.Identifier(table_schema, table_name))
                cur.execute(query)
                columns = tuple([desc[0] for desc in cur.description])
                logging.info(f"Inserting values into columns {columns} in table {table_schema}.{table_name}")
                # Dynamically generate the insert queries from returned columns and items in each tuple
                for value in values:
                    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table_schema, table_name),
                            sql.SQL(', ').join([sql.Identifier(column) for column in columns]),
                            sql.SQL(', ').join([sql.Literal(item) for item in value]))
                    cur.execute(query)
                conn.commit()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

    logging.info("All commands executed successfully")

def row_count_validation(table_schema: str, table_name: str, inserted_object: list | object, raise_error: bool = False):
    """
    Check that the count of rows in a table matches the number of items in a list or number of rows in a dataframe.
    Set raise_error to True if the counts must match. Defaults to false, and will warn if counts do not match.
    :param table_schema: schema of table to be checked
    :param table_name: name of table to be checked
    :param inserted_object: list or dataframe of objects that were inserted into the table
    :param raise_error: determine if function will raise an error or only log the object counts
    """
    # Get the row count from a postgres table
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        config = load_config()
        connection = connect(config)
        with connection as conn:
            with conn.cursor() as cur:
                query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_schema, table_name))
                cur.execute(query)
                table_row_count = cur.fetchone()[0]
            conn.commit()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        raise RuntimeError("Could not connect to Postgres Database")

    logging.info(f"Table {table_schema}.{table_name} contains {table_row_count} rows.")

    # Get the row count from either a list or a dataframe
    if isinstance(inserted_object, list):
        insertion_count = len(inserted_object)
    elif isinstance(inserted_object, DataFrame):
        insertion_count = inserted_object.count()
    logging.info(f"Inserted object contains {insertion_count} items.")

    if raise_error:
        if table_row_count != insertion_count: raise ValueError(f"Row count between {type(inserted_object)} and table {table_schema}.{table_name} does not match."
                                                                f"Table {table_schema}.{table_name} contains {table_row_count} rows."
                                                                f"Object {type(inserted_object)} contains {insertion_count} rows.")
        else: logging.info(f"Row count between {type(inserted_object)} and table {table_schema}.{table_name} matches. Both have {table_row_count} rows")
    else:
        if table_row_count == insertion_count:
            logging.info(f"Row count between {type(inserted_object)} and table {table_schema}.{table_name} matches. Both have {table_row_count} rows")
        else:
            logging.warning(f"Row count between {type(inserted_object)} and table {table_schema}.{table_name} does not match.")

def specific_row_count_validation(table_schema: str, table_name: str, row_count: int, raise_error: bool = False):
    """

    :param table_schema:
    :param table_name:
    :param row_count:
    """
    # Get the row count from a postgres table
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        config = load_config()
        connection = connect(config)
        with connection as conn:
            with conn.cursor() as cur:
                query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_schema, table_name))
                cur.execute(query)
                table_row_count = cur.fetchone()[0]
            conn.commit()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
    logging.info(f"Table {table_schema}.{table_name} contains {table_row_count} rows.")

    if raise_error:
        if table_row_count != row_count:
            raise ValueError(
                f"Specified row count and table {table_schema}.{table_name} row count do not match."
                f"Table {table_schema}.{table_name} contains {table_row_count} rows."
                f"Specified row count of {row_count}.")
        else:
            logging.info(
                f"Specified row count and table {table_schema}.{table_name} row count match. Both have {table_row_count} rows")
    else:
        if table_row_count == row_count:
            logging.info(
                f"Specified row count and table {table_schema}.{table_name} row count match. Both have {table_row_count} rows")
        else:
            logging.warning(f"Specified row count and table {table_schema}.{table_name} row count do not match."
                f"Table {table_schema}.{table_name} contains {table_row_count} rows."
                f"Specified row count of {row_count}.")