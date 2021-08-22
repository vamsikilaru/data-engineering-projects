from sql_queries import create_table_queries,drop_table_queries
import dabaseconnection

def drop_tables(cur,conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur,conn = dabaseconnection.connectDb()
    drop_tables(cur,conn)
    create_tables(cur,conn)
    cur.close()
    conn.close()

if __name__=="__main__":
    main()
