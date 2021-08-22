import configparser
import psycopg2
from pathlib import Path

def connectDb():
    config = configparser.ConfigParser()
    config.read(open(f"{Path(__file__).parent[0]}/dwh.cfg"))

    host = config.get(['DWH'],['DWH_HOST'])
    port = config.get(['DWH']['DWH_PORT'])
    username = config.get(['DWH']['DWH_DB_USER'])
    user_password = config.get(['DWH']['DWH_DB_USER'])
    dbname = config.get(['DWH']['DWH_DB'])

    conn = psycopg2.connect("host = {} dbname = {} user={} password={} port={}".format(host,dbname,username,user_password,port))
    cur = conn.cursor()

    return cur,conn
