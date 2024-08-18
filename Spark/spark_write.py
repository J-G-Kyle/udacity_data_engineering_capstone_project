"""
Read initial data from data directory. Reads immigration data from SAS sub-folder, us-cities-demographics,
and airport codes. Uses Spark to read them to dataframes, then writes to the relevant raw tables in PostgreSQL.
Validates count written to each table matches row counts in dataframes.
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
from Spark.spark_utils import (logger_config, create_session,
                               read_file_into_spark_dataframe, write_spark_dataframe_to_postgres)
from PostgreSQL.postgres_utils import row_count_validation, specific_row_count_validation

if __name__ == '__main__':

    # Configure the logger
    logger_config()

    # Create the spark session
    spark = create_session(nodes="*", driver_memory="2g")

    # Read each parquet file of immigration data into raw.immigration table in Postgres
    directory = Path("../data/sas_data/")
    immigration_count = 0
    for filepath in directory.iterdir(): # Loop through all files in directory
        if filepath.is_file() and filepath.suffix == ".parquet": # check paths are a parquet file
            # read into a dataframe
            spark_dataframe = read_file_into_spark_dataframe(spark, str(filepath), "parquet")
            immigration_count += spark_dataframe.count()
            # write dataframe to postgres
            write_spark_dataframe_to_postgres(spark_dataframe, "raw", "immigration", "append")
    # Validate that all dataframe rows have been written to the raw immigration table
    specific_row_count_validation("raw", "immigration", immigration_count, raise_error=True)

    # Read airport codes into raw.airport_codes table in Postgres
    airport_codes_df = read_file_into_spark_dataframe(spark,"../data/airport-codes_csv.csv","csv")
    write_spark_dataframe_to_postgres(airport_codes_df, "raw", "airport_codes", "overwrite")
    row_count_validation("raw", "airport_codes", airport_codes_df, raise_error=True)

    # Read demographics data into raw.demographics
    demographics_df = read_file_into_spark_dataframe(spark, "../data/us-cities-demographics.csv", "csv", delimiter=";")

    # Create a dictionary of how to rename the columns in the dataframe
    postgres_rename_dict = {'City':'city',
                            'State':'state',
                            'Median Age':'median_age',
                            'Male Population':'male_population',
                            'Female Population':'female_population',
                            'Total Population':'total_population',
                            'Number of Veterans':'number_of_veterans',
                            'Foreign-born':'foreign_born',
                            'Average Household Size':'average_household_size',
                            'State Code':'state_code',
                            'Race':'race',
                            'Count':'count'
                            }

    # Rename columns in dataframe to match postgres table
    for old_name, new_name in postgres_rename_dict.items():
        if old_name in demographics_df.columns:
            demographics_df = demographics_df.withColumnRenamed(old_name, new_name)

    # Write demogrpahics data to raw.demographics table
    write_spark_dataframe_to_postgres(demographics_df, "raw", "demographics", "overwrite")
    row_count_validation("raw", "demographics", demographics_df, raise_error=True)

    # Read temperature data from csv file
    global_temperatures_df = read_file_into_spark_dataframe(spark,"../data/GlobalLandTemperaturesByCountry.csv","csv")
    write_spark_dataframe_to_postgres(global_temperatures_df, "raw", "global_temperatures", "append")
    row_count_validation("raw", "global_temperatures", global_temperatures_df, raise_error=True)

    spark.stop()
