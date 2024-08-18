"""
Run the SQL commands to finalize the int schema transformations. Intedned to run after spark_transform.py.
Uses the SQL commands from DDL.py to normalize the demographics table, and match the expanded port code information
to the int.immigration table.
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from PostgreSQL.postgres_utils import run_sql_commands
from PostgreSQL.ddl import normalize_demographics, amend_intermediate_immigration

if __name__ == '__main__':
    run_sql_commands(normalize_demographics)
    run_sql_commands(amend_intermediate_immigration)
