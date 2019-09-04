import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

        
def drop_tables(cur, conn):
    """
    The function to drop database
    
    Parameters:
        cur  : The cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """
    
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    The function to create database
    
    Parameters:
        cur  : The cursor that will be used to execute queries.
        conn : The connection towards current connecting database.
    """
    
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh-script.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()