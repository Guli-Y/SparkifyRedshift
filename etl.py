import configparser
import psycopg2
from queries import *
        
def load_staging_tables(conn, cur):
    """loads data from s3 to staging tables"""
    for table, query in copy_table_queries.items():
        try:
            cur.execute(query)
            conn.commit()
            print(f'{table} table loaded')
        except psycopg2.Error as e:
            print('Error: loading staging tables')
            print(e)

def insert_tables(conn, cur):
    """loads data from staging tables to star schema"""
    for table, query in insert_table_queries.items():
        try:
            cur.execute(query)
            conn.commit()
            print(f'{table} table loaded')
        except psycopg2.Error as e:
            print('Error: loading star schema tables')
            print(e)

def main():
    """
    connects and gets cur to sparkify database hosted on redshift
    load the data from s3 to staging tables
    load the data from staging tables to star schema
    close the connection to sparkify database
    """   
    # connect and get cursor to the database
    config = configparser.ConfigParser()
    config.read('config.ini')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()                         
    
    # load data from s3 to the Sparkify database hosted on Redshift
    load_staging_tables(conn, cur)
    
    # load the star schema tables 
    insert_tables(conn, cur)
    
    # close the connection
    conn.close()


if __name__ == "__main__":
    main()