import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,stagging_events_record_delete


def load_staging_tables(cur, conn):
    """
    Description: This function loads data from S3 to RedShift stagging tables  
    Arguments:
        cur: the cursor object. 
        conn: the connection object. 
    Returns:
        None 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    cur.execute(stagging_events_record_delete)
    conn.commit()

    
def insert_tables(cur, conn):  
    """
    Description: This function loads data from stagging table to final table 
    Arguments:
        cur: the cursor object. 
        conn: the connection object. 
    Returns:
        None 
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

        
def main():
    """
    Description: This function can be used to 
        - Create database connection
        - Process song and log data files
        - push records into facts and dim tables
        - Close database connection
    Arguments:
        None
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Connect to Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()