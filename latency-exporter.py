import psycopg2
import time
import random
import os,sys
import threading
from datetime import datetime
from configparser import ConfigParser

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


def random_select(table, amount=5000):
    print(f'\nRunning random select on {table}')
    # Get the count of records
    cur.execute(f'select max(objectid) from {table}')
    #cur.execute(f'select count(*) from {table}')
    count = cur.fetchone()
    assert count[0]
    max_range = int(count[0]) - amount

    start = time.time()

    min = random.randrange(1, max_range)

    stmt=f'select * from {table} where objectid >= {min} AND objectid <= {min+amount}'
    #print(stmt)
    cur.execute(stmt)
    results = cur.fetchall()

    end = time.time() - start
    end = '%7f'%(end)
    print(f'Duration: {end}, results: {len(results)}')
    return end

def intersect_select(table):
    tsplit = table.split('.')
    stmt = f"SELECT Find_SRID('{tsplit[0]}', '{tsplit[1]}', 'shape')"
    cur.execute(stmt)
    srid = cur.fetchone()[0]

    source_shapes_tbl = f'citygeo.loadtest_polygons2_{str(srid)}'

    print(f'\nRunning random intersect select on {table}')

    # I made 1382 polygons in the citygeo.loadtest_polygons2 table, randomly select them.
    #random_oid = random.randrange(1,1382+1)
    # 1321 is the small squares only, the rest are larger. Use smaller to be more consistent for now.
    random_oid = random.randrange(1,1321+1)
    stmt = f'''
    SELECT pt.* FROM {table} pt
        JOIN {source_shapes_tbl} py
        ON ST_Intersects(py.shape, pt.shape)
        WHERE py.objectid = {random_oid};
    '''
    start = time.time()
    cur.execute(stmt)
    results = cur.fetchall()
    end = time.time() - start
    end = '%7f'%(end)
    print(f'Duration: {end}, results: {len(results)}')
    return end


for i in range(6):
    loop_start = time.time()

    try:
        dor_parcel_select_dur = random_select('viewer.dor__dor_parcel')
    except psycopg2.errors.QueryCanceled as e:
        print(str(e))
        dor_parcel_select_dur = 9
    except Exception as e:
        print(str(e))
        dor_parcel_select_dur = 9
    try:
        dor_parcel_intersect_dur = intersect_select('viewer.dor__dor_parcel')
    except psycopg2.errors.QueryCanceled as e:
        print(str(e))
        dor_parcel_intersect_dur = 9
    except Exception as e:
        print(str(e))
        dor_parcel_intersect_dur = 9


    try:
        address_summary_select_dur = random_select('viewer.ais__address_summary')
    except psycopg2.errors.QueryCanceled as e:
        print(str(e))
        address_summary_select_dur = 9
    except Exception as e:
        print(str(e))
        address_summary_select_dur = 9
    try:
        address_summary_intersect_dur = intersect_select('viewer.ais__address_summary')
    except psycopg2.errors.QueryCanceled as e:
        print(str(e))
        address_summary_intersect_dur = 9
    except Exception as e:
        print(str(e))
        address_summary_intersect_dur = 9

    try:
        rtt_summary_select_dur = random_select('viewer.dor__rtt_summary', 10000)
    except psycopg2.errors.QueryCanceled as e:
        print(str(e))
        rtt_summary_select_dur = 9
    except Exception as e:
        print(str(e))
        rtt_summary_select_dur = 9
    try:
        rtt_summary_intersect_dur = intersect_select('viewer.dor__rtt_summary')
    except psycopg2.errors.QueryCanceled as e:
        print(str(e))
        rtt_summary_intersect_dur = 9
    except Exception as e:
        print(str(e))
        rtt_summary_intersect_dur = 9

# Can't indent this how I want because there can't be any whitespace starting the lines
    export_txt = f'''
# HELP dor_parcel_select_latency_seconds Latency for this query
# TYPE dor_parcel_select_latency_seconds gauge
dor_parcel_select_latency_seconds {dor_parcel_select_dur}
# HELP address_summary_select_latency_seconds Latency or this query
# TYPE address_summary_select_latency_seconds gauge
address_summary_select_latency_seconds {address_summary_select_dur}
# HELP rtt_summary_select_latency_seconds Latency for this query
# TYPE rtt_summary_select_latency_seconds gauge
rtt_summary_select_latency_seconds {rtt_summary_select_dur}
# HELP dor_parcel_intersect_latency_seconds Latency for this query
# TYPE dor_parcel_intersect_latency_seconds gauge
dor_parcel_intersect_latency_seconds {dor_parcel_intersect_dur}
# HELP address_summary_intersect_latency_seconds Latency or this query
# TYPE address_summary_intersect_latency_seconds gauge
address_summary_intersect_latency_seconds {address_summary_intersect_dur}
# HELP rtt_summary_intersect_latency_seconds Latency for this query
# TYPE rtt_summary_intersect_latency_seconds gauge
rtt_summary_intersect_latency_seconds {rtt_summary_intersect_dur}
    '''

    print('Waiting until 9th second...')
    wait_until_9th_second()

    html_file = open('/var/www/html/latency-exporter.html', 'w')
    html_file.write(export_txt)
    html_file.close()
    loop_end = time.time() - loop_start

print('Done.')

