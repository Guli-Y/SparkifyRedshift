import configparser
import psycopg2
from queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries`.
    """
    for table, query in drop_table_queries.items():
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('ERROR: dropping table')
            print(e)

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries`.
    """
    cur.execute("SET timezone='Europe/Berlin';")
    for table, query in create_table_queries.items():
        try:
            cur.execute(query)
            conn.commit()
            print(f'{table} table created')
        except psycopg2.Error as e:
            print('ERROR: creating table')
            print(e)


def main():
    """
    - connects and gets cursor to sparkify database hosted on Redshift.
    - drops all the tables if they already exist
    - creates the tables needed
    - closes the connection
    """
    config = configparser.ConfigParser()
    config.read('config.ini')
   
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()                        

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
