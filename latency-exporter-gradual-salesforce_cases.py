import psycopg2
import time
import random
#import concurrent.futures
import threading
from datetime import datetime
from configparser import ConfigParser


config = ConfigParser()
config.read(script_directory + os.sep + 'config.ini')
conn_info = dict(config.items('databridge'))

host = conn_info['host']
user = conn_info['user']
database = conn_info['database']
password = conn_info['password']

conn = psycopg2.connect(host=host, dbname=database, port=5432, user=user, password=password)
conn.autocommit = True

cur = conn.cursor()
# Timeout at 9 seconds
cur.execute("SET SESSION statement_timeout = '9000'")

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


def intersect_select():
    try:
        # Running queries async on a single connection seems to give us bad results, so make one per async run.
        async_conn = psycopg2.connect(host=host, dbname=database, port=5432, user=user, password=password)
        conn.autocommit = True

        async_cur = conn.cursor()
        # Timeout at 9 seconds
        async_cur.execute("SET SESSION statement_timeout = '9000'")
        #print(f'Running random intersect select on {table}')
        # I made 1382 polygons in the citygeo.loadtest_polygons2 table, randomly select them.
        #random_oid = random.randrange(1,1382+1)
        # 1321 is the small squares only
        random_oid = random.randrange(1,1321+1)
        table = 'viewer.philly311__salesforce_cases'
        stmt = f'''
        SELECT pt.* FROM {table} pt
            JOIN citygeo.loadtest_polygons2_4326 py
            ON ST_Intersects(py.shape, pt.shape)
            WHERE py.objectid = {random_oid}
            AND pt.shape is NOT NULL;
        '''
        start = time.time()
        async_cur.execute(stmt)
        results = async_cur.fetchall()
        #print(f'Returned results: {len(results)}')
        end = time.time() - start
        end = '%7f'%(end)
        # ljust provides auto indendation
        msg = f'{table},'.ljust(50) + f' duration: {end}, results: {len(results)}, OID: {random_oid}'
        print(msg)
        async_conn.close()
        return end
    except Exception as e:
        async_conn.close()
        print(str(e))
        return str(e)


count = 1
for i in range(201):
    print(f'\nLoop #{i}')
    #intersect_select(random_table)
    thread_dict = {}
    loop_start = time.time()
    for i in range(0,count):
        thread_dict[i] = threading.Thread(target=intersect_select)
        thread_dict[i].start()

    for key in thread_dict.keys():
        thread_dict[key].join()
        #return_value = thread_dict[key].result
        #print(f'{key} return value: {return_value}')

    loop_end = time.time() - loop_start
    print(f'Loop duration: {loop_end}')
    count += 1
    thread_dict = {}
    # Can't indent this how I want because there can't be any whitespace starting the lines
    export_txt = f'''# HELP dor_parcel_select_latency_seconds Latency for this query
# TYPE dor_parcel_select_latency_seconds gauge
salesforce_intersect_gradual_latency{{amount="{i+1}"}} {loop_end}'''
    #print(export_txt)

    # Wait until every 9/10 seconds in a minute (9, 19, 29, etc)
    # so we can align with the prometheus scraper exactly.
    #wait_until_9th_second()
    every_20th_second()

    html_file = open('/var/www/html/latency-exporter.html', 'w')
    html_file.write(export_txt)
    html_file.close()
    time.sleep(1)

print('Done.')


