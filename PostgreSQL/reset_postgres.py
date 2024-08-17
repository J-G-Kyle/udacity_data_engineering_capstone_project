"""
Execute the reset statements from PostgreSQL in the database specified by the config file.
This will drop the schemas raw and int, and all associatated objects.
"""
import logging
import psycopg2
from PostgreSQL.postgres_utils import load_config, connect
from PostgreSQL.ddl import reset

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    try:
        config = load_config()
        connection = connect(config)
        with connection as conn:
            with conn.cursor() as cur:
                # execute the reset statements
                for command in reset:
                    logging.info(("Running: %s"), command)
                    cur.execute(command)
                conn.commit()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
