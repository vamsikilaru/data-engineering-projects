import configparser
import psycopg2
from pathlib import Path

def connectDb():
    config = configparser.ConfigParser()
    #print(f"{Path(__file__).parent[0]}/dwh.cfg")
    #config.read(open("/Users/vkilaru/Desktop/Python/datawarehouse/dwh.cfg"))
    config.read_file(open(f"{Path(__file__).parents[0]}/dwh.cfg"))

    host = config.get('DWH','DWH_HOST')
    port = config.get('DWH','DWH_PORT')
    username = config.get('DWH','DWH_DB_USER')
    user_password = config.get('DWH','DWH_DB_PASSWORD')
    dbname = config.get('DWH','DWH_DB')

    conn = psycopg2.connect("host = {} dbname = {} user={} password={} port={}".format(host,dbname,username,user_password,port))
    cur = conn.cursor()

    return cur,conn
