#!/bin/bash

# Directory path
SPARK_DIR="<absolute path to Udacity_DE_Capstone/Spark>"
PROJECT_DIR="absolute path to Udacity_DE_Capstone/"

# Change to relevant directory to prevent errors in finding config.ini file
cd $PROJECT_DIR || exit

# Set up postgres environment

python3 PostgreSQL/reset_postgres.py
python3 PostgreSQL/set_up_postgres.py

cd $SPARK_DIR || exit

python3 spark_write.py
python3 spark_transform.py

#cd $POSTGRES_DIR || exit
cd $PROJECT_DIR || exit

python3 PostgreSQL/intermediate_sql.py

