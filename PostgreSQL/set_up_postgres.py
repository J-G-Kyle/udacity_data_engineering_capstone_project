from postgres_utils import run_sql_commands, insert_tuple_to_table, row_count_validation
from  DDL import set_up_postgres, set_up_intermediate_schema, replace_blanks_in_port_codes
from immigration_labels import I94PORT

if __name__ == '__main__':
    # Create database schemas and tables as defined in the DDL queries
    run_sql_commands(set_up_postgres)
    run_sql_commands(set_up_intermediate_schema)

    # Insert port codes into the raw table and validate the counts match
    insert_tuple_to_table("raw", "i94_port_codes", I94PORT)
    row_count_validation("raw", "i94_port_codes", I94PORT, True)

    # Change blank 'state' entries to i94_port_codes to NULL
    run_sql_commands(replace_blanks_in_port_codes)