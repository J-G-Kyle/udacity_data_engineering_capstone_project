"""
Spark utilities. Set of functions for reading and writing spark dataframes to and from a PostgreSQL database.
Also includes functions to manage logging, creating spark sessions, and performing specific transformations
on dataframes.
"""
import configparser
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import to_date, expr, col

def logger_config():
    """Set logger configuration for default formatting of message and date.
       Sets logging to ignore unneeded pyspark and py4j messages to prevent overwhelming number of messages."""
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger('pyspark').setLevel((logging.ERROR))
    logging.getLogger("py4j").setLevel(logging.INFO)

def create_session(nodes:str = "2", driver_memory:str = "2g") -> object:
    """Create local spark session with default 2 nodes and 2g of memory.
     Sets postgresql driver .jar file from /driver/ directory.
       Returns:
           Spark session
           :rtype: object"""
    # Create spark config
    conf = SparkConf().setAppName("Pyspark SQL capstone data loader") \
                .setMaster(f"local[{nodes}]") \
                .set("spark.driver.memory", driver_memory) \
                .set("spark.jars","../driver/postgresql-42.6.2.jar")

    # Create spark session
    session = SparkSession \
            .builder \
            .config(conf=conf) \
            .getOrCreate()

    return session

def read_file_into_spark_dataframe(session, filepath: str, fileformat: str, delimiter:str =',') -> object:
    """Read data from file into spark dataframe
        Args:
            session: spark session to use
            filepath (string): filepath to file to be read
            fileformat: format of input file, e.g. 'Parquet' or 'CSV'
            delimiter: CSV file delimiter, defaults to ','
        Returns
            Spark dataframe
            :rtype: object"""
    logging.info(f"Reading {fileformat} file from {filepath} into dataframe")
    Dataframe = session.read.format(fileformat) \
        .option("header", "true") \
        .option("delimiter", delimiter) \
        .load(filepath)
    logging.info(f"Dataframe created with {Dataframe.count()} rows")

    return Dataframe

def write_spark_dataframe_to_postgres(dataframe: object, schema: str, table_name: str, mode: str):
    """Take a spark dataframe and write it to a postgres table
        Uses PostgreSQL URL, user, driver, and password from config.ini
            Args:
                dataframe: dataframe to be written to PostgreSQL table
                schema: Schema name
                table_name: Table name
                mode: PysparkSQL write mode to be used, e.g. 'overwrite', 'append'. See https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html """

    # Get url and properties from config
    config = configparser.ConfigParser()
    config.read('../PostgreSQL/config.ini')
    config_url = config['spark']['url']

    url = "jdbc:postgresql:{}".format(config_url)

    properties = {
            "user": config['postgresql']['user'],
            "driver":config['spark']['driver'],
            "password": config['postgresql']['password']
    }

    dataframe.write.jdbc(url, schema+"."+table_name, mode=mode, properties=properties)

    logging.info(f"Wrote dataframe of {dataframe.count()} rows to Postgres table {schema+'.'+table_name} using {mode} mode for user {config['postgresql']['user']}")

def read_table_from_postgres_to_spark(session: object, schema: str, table_name: str) -> object:
    """ Read a postgres table into a spark dataframe
        Arguments:
            session: spark session to use
            schema: Postgres schema you want to read from
            table_name: name of the table you want to read
        Returns: Spark dataframe object
        :rtype: object"""

    # Get url and properties from config
    config = configparser.ConfigParser()
    config.read('../PostgreSQL/config.ini')
    config_url = config['spark']['url']

    dataframe = session.read.format("jdbc") \
                .options(
                url= "jdbc:postgresql:{}".format(config_url),
                dbtable= schema+"."+table_name,
                user= config['postgresql']['user'],
                password= config['postgresql']['password'],
                driver= config['spark']['driver']
            ).load()

    return dataframe

def convert_date_to_ISO8601(dataframe: object, column_name: str, date_type: str) -> object:
    """ Convert a SAS numeric date or integer date (e.g. 20160401) into ISO8601 date format
        Arguments:
            dataframe: input dataframe for date conversion. will be returned with updated columns
            column_name: string name of column to be changed
            date_type: string type of date, must be "int" or "SAS"
        Returns:
            input dataframe with updated columns
            :rtype: object"""
    if date_type not in ["SAS", "int"]:
        raise ValueError("date_type must be one of the following: 'SAS', 'int'")
    elif date_type == "SAS":
        dataframe = dataframe.withColumn(column_name, to_date(expr(f"date_add('1960-01-01', {column_name})")))
    elif date_type == "int":
        dataframe = dataframe.withColumn(column_name, to_date(col(column_name).cast("string"), "yyy-MM-DD"))

    return dataframe

def drop_duplicates_in_dataframe(dataframe: object, column_names: list = []) -> object:
    """ Drop duplicates from one or more columns in a spark dataframe
        Arguments:
            dataframe: dataframe to drop duplicates from. Will be returned altered, without duplicates
            column_names: a list of all columns to be used. Defaults to all columns
        Returns:
            dataframe: input dataframe without duplicates in specified columns
            :rtype: object"""

    dataframe = dataframe.dropDuplicates(column_names)

    return dataframe


