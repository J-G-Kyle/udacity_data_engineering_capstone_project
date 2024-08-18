"""
Run the SQL commands to create the prs schema. Must run after all other pipeline steps have executed successfully.
Creates materialized views of all tables in the int schema.
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from PostgreSQL.postgres_utils import run_sql_commands
from PostgreSQL.ddl import set_up_presentation_schema

if __name__ == '__main__':
    run_sql_commands(set_up_presentation_schema)
