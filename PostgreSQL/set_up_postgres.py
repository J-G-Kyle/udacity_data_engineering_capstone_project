"""
Set up the initial postgres environment. Runs on the database specified in the config file.
Will set up the raw and int schemas and associated tables.
Will insert port codes from PostgreSQL.immigration_labels and validate the counts match
then will format blank values to NULL
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from PostgreSQL.postgres_utils import run_sql_commands, insert_tuple_to_table, row_count_validation
from PostgreSQL.ddl import set_up_postgres, set_up_intermediate_schema, replace_blanks_in_port_codes
from PostgreSQL.immigration_labels import I94PORT

if __name__ == '__main__':
    # Create database schemas and tables as defined in the DDL queries
    run_sql_commands(set_up_postgres)
    run_sql_commands(set_up_intermediate_schema)

    # Insert port codes into the raw table and validate the counts match
    insert_tuple_to_table("raw", "i94_port_codes", I94PORT)
    row_count_validation("raw", "i94_port_codes", I94PORT, True)

    # Change blank 'state' entries to i94_port_codes to NULL
    run_sql_commands(replace_blanks_in_port_codes)
