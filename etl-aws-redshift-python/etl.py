import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The function to load data into staging tables
    
    Parameters:
        cur  : The cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    The function to insert rows into a table
    
    Parameters:
        cur  : The cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh-script.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()