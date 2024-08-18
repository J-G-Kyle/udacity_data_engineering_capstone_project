# Capstone ELT pipeline project
The final project from the Udacity Data Engineering Nanodegree Program

## Scope
The goal of this project is to build an ELT pipeline to move input data files into a database. For this local 
implementation Pyspark is used for all data loading and transforming, while Postgres provides the database.  
The intention behind the project is to provide a modular and scalable medallion architecture, that preserves 
traceability of data lineage. This data is intended for analytics use, with data present in the prs schema available 
for use following transformation. The raw schema preserves the original data, allowing for data lineage to be 
examined and ensure transformations are reproducible. This would be the 'bronze' layer. The int schema collects 
transformed data, acting as a 'silver' layer. Finally, the prs schema contains the final tables as materialized 
views - the 'gold' layer. Analysts can be given access to the prs schema, and if required commonly used views can be created 
  to speed up query processing.

## Datasets
The following datasets are used in this projects
- I94 Immigration data: taken from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). 
This contains records of immigrants travelling into the United States.
- World Temperature Data: a [Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data) dataset of land temperatures by city, dating back to 1750.
- U.S. City demographic data. This dataset contains information about the demographics of all US cities and 
  census-designated places with a population greater or equal to 65,000. This data comes from the [US Census 
  Bureau's 2015 American Community Survey](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- Airport Code Table: a [Datahub](https://datahub.io/core/airport-codes#data) file of airport codes and their type, 
  name, and location.

#### Requirements outside of requirements.txt
1. Local postgres installation, e.g. [Postgres Wiki - Homebrew](https://wiki.postgresql.org/wiki/Homebrew)
2. Spark jdbc driver: [Download here](https://jdbc.postgresql.org/download/)
3. Java installed
4. JDK installed (v21)

#### Versions used
1. postgres (PostgreSQL) 14.13 (Homebrew)
2. postgresql-42.6.2.jar
3. java version "21.0.4"
4. javac 21.0.4

`config.ini` must be set with the appropriate values for your Postgres database. These will have been set when you 
created the database.
`run_pipeline.sh` requires the directory paths set in order to run the python files and import the required 
functions from each directory.

## Run the pipeline
- `run_pipeline.sh`: Execute the ELT process. Resets the database schemas, dropping any existing objects. 
  Extracts the data from all files, loading it into the `raw` schema. Extracts from the raw schema, and performs the 
  required transformations before loading into the `int` schema.

## Quality checks
Row counts are checked to ensure they match between the table that records are written to, and the count of rows 
within the dataframe that was written to the table. Use of primary key or not null in the table schemas ensures that 
essential columns only contain the kind of data they are supposed to.

## Addressing Other Scenarios
#### The data was increased by 100x.
This would result in the largest table (immigration) becoming ~300M rows and ~50GB This quantity is well within 
Postgres's 
capabilities, but would exceed available local memory for performing transformations. Data would have to be batched 
for processing, and transformation times would be on the scale of hours instead of minutes. Depending on the 
requirements this may not be an issue (e.g. processing one daily update). To improve processing times and manage 
storage needs it might be necessary to look towards a cloud provider solution such as AWS EMR and RDS for PostgreSQL.
This would not require a fundamental shift in the tech stack employed by this project, as one of the goals was to 
create a scalable architecture.

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
This would require a scheduler to handle triggering of tasks. Due to the complexity of the pipeline, a DAG based 
service such as Airflow, Dagster, or Luigi would be the preferred choice. The DAG could be set to trigger the 
pipeline steps early in the morning, and handle each task sequentially as needed.

#### The database needed to be accessed by 100+ people.
For so many people to access the same database simultaneously, it would need to be hosted on a cloud service. There 
are multiple dedicated cloud database providers such as Databricks, Snowflake, or AWS Redshift, but you could also 
migrate the PostgreSQL database to a serivce such as AWS RDS, Google Cloud SQL, or Azure Database for PostgreSQL. 
Each of these offers to manage and host the database for a fee, and allow you to access it using your choice of 
query interface through a secure web connection.



