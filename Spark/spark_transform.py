from spark_utils import (logger_config, create_session,
                         read_table_from_postgres_to_spark,
                         convert_date_to_ISO8601, drop_duplicates_in_dataframe)
from pyspark.sql.types import IntegerType, StringType, LongType, FloatType, DecimalType
from pyspark.sql.functions import col, split, length
from transformation_dicts import modes, visa_types, addrs, I94CIT_I94RES
from spark_utils import write_spark_dataframe_to_postgres
from pyspark.sql import functions as sf

def immigration_transformation():
    """Reads the raw.immigration table into memory
       Casts data to more appropriate data types, replaces labels with informative values
       Writes the transformed data to int.immigration"""

    # Read the raw tables into a dataframe to perform transformations in memory
    immigration_df = read_table_from_postgres_to_spark(spark, "raw", "immigration")

    # Drop duplicate cicids to prevent potential primary key error
    immigration_df = drop_duplicates_in_dataframe(immigration_df, ['cicid'])

    # Cast datatypes to a more appropriate value
    immigration_df = immigration_df.withColumn("cicid", immigration_df["cicid"].cast(IntegerType())) \
                                   .withColumn("i94yr", immigration_df["i94yr"].cast(IntegerType())) \
                                   .withColumn("i94mon", immigration_df["i94mon"].cast(IntegerType())) \
                                   .withColumn("i94cit", immigration_df["i94cit"].cast(IntegerType())) \
                                   .withColumn("i94res", immigration_df["i94res"].cast(IntegerType())) \
                                   .withColumn("i94visa", immigration_df["i94visa"].cast(StringType())) \
                                   .withColumn("arrdate", immigration_df["arrdate"].cast(IntegerType())) \
                                   .withColumn("i94mode", immigration_df["i94mode"].cast(StringType())) \
                                   .withColumn("depdate", immigration_df["depdate"].cast(IntegerType())) \
                                   .withColumn("i94bir", immigration_df["i94bir"].cast(IntegerType())) \
                                   .withColumn("count", immigration_df["count"].cast(IntegerType())) \
                                   .withColumn("dtadfile", immigration_df["dtadfile"].cast(StringType())) \
                                   .withColumn("biryear", immigration_df["biryear"].cast(IntegerType())) \
                                   .withColumn("dtaddto", immigration_df["dtaddto"].cast(IntegerType())) \
                                   .withColumn("admnum", immigration_df["admnum"].cast(LongType()))

    # re-cast i94cit and i94res as strings for dictionary conversion
    immigration_df = immigration_df.withColumn("i94cit", immigration_df["i94cit"].cast(StringType())) \
                                   .withColumn("i94res", immigration_df["i94res"].cast(StringType()))

    # Covert date columns from SAS to ISO8601
    immigration_df = convert_date_to_ISO8601(immigration_df, "arrdate", "SAS")
    immigration_df = convert_date_to_ISO8601(immigration_df, "depdate", "SAS")

    # Covert dtadfile and dtadto into ISO8601
    immigration_df = convert_date_to_ISO8601(immigration_df, "dtadfile", "int")
    immigration_df = convert_date_to_ISO8601(immigration_df, "dtaddto", "int")

    # Convert i94port in a postgres join to add those tuple values

    # Covert i94visa to visa type
    immigration_df = immigration_df.na.replace(visa_types, None, "i94visa")

    # Convert i94mode to string
    immigration_df = immigration_df.na.replace(modes, None, "i94mode")

    # Convert i94addr to state
    immigration_df = immigration_df.na.replace(addrs, None, "i94addr")

    # Convert i94cit and i94res
    immigration_df = immigration_df.na.replace(I94CIT_I94RES, None, "i94cit") \
                                      .replace(I94CIT_I94RES, None, "i94res")

    # Drop uneeded columns
    immigration_df = immigration_df.drop(col("count"))

    # Create a dictionary of how to rename the columns in the dataframe
    postgres_rename_dict = {'i94port':'port_code',
                            'i94yr':'year',
                            'i94mon':'month',
                            'i94cit':'citizenship_country',
                            'i94res':'residency_country',
                            'arrdate':'arrival_date',
                            'i94mode':'arrival_mode',
                            'i94addr':'state',
                            'depdate':'departure_date',
                            'i94bir':'age_in_years',
                            'i94visa':'visa_purpose',
                            'dtadfile':'date_added_to_files',
                            'occup':'occupation',
                            'entdepa':'arrival_flag',
                            'entdepd': 'departure_flag',
                            'entdepu': 'update_flag',
                            'matflag': 'match_flag',
                            'biryear':'birth_year',
                            'dtaddto':'admitted_until',
                            'fltno':'flight_number',
                            'Count':'count',
                            'visatype':'visa_type'
                            }

    # Rename columns in dataframe to match postgres table
    for old_name, new_name in postgres_rename_dict.items():
        if old_name in immigration_df.columns:
            immigration_df = immigration_df.withColumnRenamed(old_name, new_name)

    # Write back to postgres int schema
    write_spark_dataframe_to_postgres(immigration_df, "int", "immigration", "overwrite")

def demographics_transformation():
    """Reads the raw.demographics table into memory
       Casts data to more appropriate data types
       Writes the transformed and pre-normalized data to int.cities, int.racial_demographics, and int.demographics"""
    # Demographics transformations
    demo_df = read_table_from_postgres_to_spark(spark, "raw", "demographics")

    # Cast to more appropriate data types
    demo_df = demo_df.withColumn("median_age", demo_df["median_age"].cast(FloatType())) \
        .withColumn("male_population", demo_df["male_population"].cast(IntegerType())) \
        .withColumn("female_population", demo_df["female_population"].cast(IntegerType())) \
        .withColumn("total_population", demo_df["total_population"].cast(IntegerType())) \
        .withColumn("number_of_veterans", demo_df["number_of_veterans"].cast(IntegerType())) \
        .withColumn("foreign_born", demo_df["foreign_born"].cast(IntegerType())) \
        .withColumn("average_household_size", demo_df["average_household_size"].cast(FloatType())) \
        .withColumn("count", demo_df["count"].cast(IntegerType()))

    demo_df = demo_df.withColumnRenamed("state", "state_name")

    # Normalize data into cities table
    cities_df = demo_df["city", "state_code", "state_name"]
    write_spark_dataframe_to_postgres(cities_df, "int", "cities", "append")

    # Normalize data into racial demographics
    racial_demo_df = demo_df["city", "state_code", "state_name", "race", "count"]
    write_spark_dataframe_to_postgres(racial_demo_df, "int", "racial_demographics", "append")

    # Normalize data into demographics
    demo_df = demo_df["city", "state_name", "median_age", "male_population", "female_population",
                      "total_population", "number_of_veterans", "foreign_born", "average_household_size", "state_code"]
    write_spark_dataframe_to_postgres(demo_df, "int", "demographics", "append")

def airport_codes_transformation():
    # Read raw.airport codes into dataframe
    air_df = read_table_from_postgres_to_spark(spark, "raw", "airport_codes")

    # Split coordinates into latitude and longitude
    split_col = split(air_df['coordinates'], ',')
    air_df = air_df.withColumn('latitude', split_col.getItem(0))
    air_df = air_df.withColumn('longitude', split_col.getItem(1))

    # Cast to more appropriate data types
    air_df = air_df.withColumn("elevation_ft", air_df["elevation_ft"].cast(IntegerType())) \
        .withColumn("latitude", air_df["latitude"].cast(DecimalType(precision=21, scale=18))) \
        .withColumn("longitude", air_df["longitude"].cast(DecimalType(precision=21, scale=18)))

    # Drop coordinates column
    air_df = air_df.drop(col("coordinates"))

    write_spark_dataframe_to_postgres(air_df, "int", "airport_codes", "append")


if __name__ == '__main__':
    # Configure the logger
    logger_config()

    # Data transformations
    # Requires higher memory allocation due to size of table
    spark = create_session(nodes="1", driver_memory="10g")
    immigration_transformation()

    # Close spark
    spark.stop()

    # Create a lower memory but more nodes spark session
    spark = create_session(nodes="*", driver_memory="3g")

    demographics_transformation()
    airport_codes_transformation()

    # Close spark
    spark.stop()



