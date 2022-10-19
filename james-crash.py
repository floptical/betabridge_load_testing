import psycopg2
import time
import random
#import concurrent.futures
import threading
from datetime import datetime
from configparser import ConfigParser
import os,sys

script_directory = os.path.dirname(os.path.realpath(__file__))


config = ConfigParser()
config.read(script_directory + os.sep + 'config.ini')
conn_info = dict(config.items('betabridge'))

host = conn_info['host']
user = conn_info['user']
database = conn_info['database']
password = conn_info['password']

conn = psycopg2.connect(host=host, dbname=database, port=5432, user=user, password=password)
conn.autocommit = True

cur = conn.cursor()
# Timeout at 9 seconds
cur.execute("SET SESSION statement_timeout = '19000'")



# Construct our table list with tables and their SRIDs
# and make sure they're not empty
tables_with_shape_stmt = """
select table_schema,table_name from information_schema.columns
    where column_name = 'shape'
    AND (table_schema = 'import' or table_schema = 'viewer')
"""
cur.execute(tables_with_shape_stmt)
results = cur.fetchall()
tables_with_shape = [x[0] + '.' + x[1] for x in results]

tables_and_srids = []
for table in tables_with_shape:
    tsplit = table.split('.')
    # exclude tables with 'test' in their name
    if 'test' in tsplit[1]:
        continue
    # Find out if it's empty first
    stmt=f'SELECT shape FROM {table} where shape is not null LIMIT 1'
    cur.execute(stmt)
    result = cur.fetchone()
    if not result:
        continue

    # Get the SRID and add to new list as a tuple with table name
    stmt = f"SELECT Find_SRID('{tsplit[0]}', '{tsplit[1]}', 'shape')"
    cur.execute(stmt)
    srid = cur.fetchone()[0]
    if int(srid) == 2272 or int(srid) == 4326 or int(srid) == 3857 or int(srid) == 6565:
        tables_and_srids.append( (table, int(srid)) )
    else:
        print(srid)



def wait_until_9th_second():
    '''Wait until the 9th second of every 10 seconds'''
    while True:
        now = datetime.now()
        if (now.second % 10) != 0:
            x = (now.second % 10) % 9
            if x == 0:
                print(now.strftime("%H:%M:%S"))
                return
            else:
                time.sleep(0.95)


def every_20th_second():
    '''Wait until the 9th second of every 10 seconds'''
    while True:
        now = datetime.now()
        x = (now.second % 20)
        if x == 0:
            print(now.strftime("%H:%M:%S"))
            return
        else:
            time.sleep(0.95)


def james_query():
    try:
        # Running queries async on a single connection seems to give us bad results, so make one conn/cur per async run.
        async_conn = psycopg2.connect(host=host, dbname=database, port=5432, user=user, password=password)
        conn.autocommit = True

        async_cur = conn.cursor()
        # Timeout at 9 seconds
        async_cur.execute("SET SESSION statement_timeout = '9000'")


        start = time.time()
        async_cur.execute(open("james.sql", "r").read())
        results = async_cur.fetchall()
        #print(f'Returned results: {len(results)}')
        end = time.time() - start
        end = '%7f'%(end)
        # ljust provides auto indendation
        msg = f'duration: {end}, results: {len(results)}'
        print(msg)
        async_conn.close()
        return end
    except Exception as e:
        async_conn.close()
        print(str(e))
        return str(e)


if __name__ == '__main__':
    amount = int(sys.argv[1])
    print(amount)

    thread_dict = {}
    loop_start = time.time()

    for i in range(1,amount+1):

        thread_dict[i] = threading.Thread(target=james_query)
        #thread_dict[i] = threading.Thread(target=intersect_select, args=(rand_table,srid,))

    for key in thread_dict.keys():
        thread_dict[key].start()

    for key in thread_dict.keys():
        thread_dict[key].join()

    loop_end = time.time() - loop_start
    print(f'Loop duration: {loop_end}')
    thread_dict = {}

    print('Done.')

