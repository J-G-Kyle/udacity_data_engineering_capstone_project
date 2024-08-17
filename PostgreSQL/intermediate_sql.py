from PostgreSQL.postgres_utils import run_sql_commands
from PostgreSQL.ddl import normalize_demographics, amend_intermediate_immigration

if __name__ == '__main__':
    run_sql_commands(normalize_demographics)
    run_sql_commands(amend_intermediate_immigration)
