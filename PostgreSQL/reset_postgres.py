import psycopg2
from postgres_utils import load_config, connect
from DDL import reset
import logging

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
                    logging.info(f"Running: {command}")
                    cur.execute(command)
                conn.commit()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
