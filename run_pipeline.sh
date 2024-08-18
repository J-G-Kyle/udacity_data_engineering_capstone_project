#!/bin/bash

# Directory path
POSTGRES_DIR="<absolute path to Udacity_DE_Capstone/PostgreSQL>"
SPARK_DIR="<absolute path to Udacity_DE_Capstone/Spark>"
PROJECT_DIR="absolute path to Udacity_DE_Capstone/"

# Change to relevant directory to prevent errors in finding config.ini file
cd $POSTGRES_DIR || exit

# Set up postgres environment
python3 reset_postgres.py
python3 set_up_postgres.py

cd $SPARK_DIR || exit

python3 spark_write.py
python3 spark_transform.py

cd $POSTGRES_DIR || exit

python3 intermediate_sql.py
python3 presentation_sql.py

