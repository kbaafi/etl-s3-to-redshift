import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries
from sql_queries import insert_table_queries, drop_staging_queries, dwh_schema_create 


def create_schema(cur,conn):
    cur.execute(dwh_schema_create)
    conn.commit()

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def prepare_database(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def drop_staging_tables(cur,conn):
    for query in drop_staging_queries:
        cur.execute(query)
        conn.commit()

def connect_to_db(host,dbname,user,password,port,schema = None):
    
    conn = psycopg2.connect(("""host={} dbname={} 
        user={} password={} port={}""").
    format(
        host,
        dbname,
        user,
        password,
        port))
    cur = conn.cursor()
    
    if schema is not None:
        cur.execute("SET search_path TO " + schema)
        conn.commit()  
    
    return conn, cur


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    host        = config.get("DWH","dwh_endpoint")
    user        = config.get("DWH","dwh_db_user")
    password    = config.get("DWH","dwh_db_password")
    port        = config.get("DWH","dwh_db_port")
    dbname      = config.get("DWH","dwh_db")
    schema      = config.get("DWH","dwh_schema")

    # Initial connection
    conn, cur = connect_to_db(host,dbname,user,password,port)

    # Create and select schema
    try:
        print("Creating data warehouse schema")
        create_schema(cur,conn)
        conn.close()

        conn, cur = connect_to_db(host, dbname,user, password, port, schema=schema)
    except Exception as e:
        print(e)
        return
    
    try:
        print("Preparing database for ETL")
        prepare_database(cur,conn)
    except Exception as e:
        print(e)
        return
    
    try:
        print("Loading staging tables")
        load_staging_tables(cur, conn)
    except Exception as e:
        print(e)
        return
    
    try:
        print("Copying tables")
        insert_tables(cur, conn)
    except Exception as e:
        print(e)
        return
    
    try:
        print("Dropping staging tables")
        drop_staging_tables(cur, conn)
    except Exception as e:
        print(e)
        return
    

    conn.close()


if __name__ == "__main__":
    main()