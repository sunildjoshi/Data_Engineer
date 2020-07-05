import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function drops each table using the queries in `drop_table_queries` list.
    
    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function creates each table using the queries in `create_table_queries` list.
    
    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function 
        - Connects to Redshift cluster database
        - Drops all the tables in `drop_table_queries` list if present.  
        - Creates all tables in `create_table_queries` list. 
        - Finally, closes the connection. 
        
    Arguments:
        None
    Returns:
        None 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()