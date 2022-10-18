import psycopg2
import time
import random
#import concurrent.futures
import threading
from datetime import datetime
from configparser import ConfigParser


config = ConfigParser()
config.read(script_directory + os.sep + 'config.ini')
conn_info = dict(config.items('batabridge'))

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


def intersect_select(table,srid):
    try:
        # Running queries async on a single connection seems to give us bad results, so make one conn/cur per async run.
        async_conn = psycopg2.connect(host=host, dbname=database, port=5432, user=user, password=password)
        conn.autocommit = True

        async_cur = conn.cursor()
        # Timeout at 9 seconds
        async_cur.execute("SET SESSION statement_timeout = '9000'")
        #print(f'Running random intersect select on {table}')
        # I made 1382 polygons in the citygeo.loadtest_polygons2 table, randomly select them.
        #random_oid = random.randrange(1,1382+1)
        # 1321 is the small squares only, the rest are larger. Use smaller to be more consistent for now.
        random_oid = random.randrange(1,1321+1)

        source_shapes_tbl = f'citygeo.loadtest_polygons2_{str(srid)}'
        # To make these differently projected tables from my initial one, run this in arcpy:
        # import arcpy
        # arcpy.env.workspace = 'C:\\Users\\roland.macdavid\\AppData\\Roaming\\Esri\\Desktop10.8\\ArcCatalog\\betabridge-staging_citygeo.sde'
        # arcpy.Project_management(in_dataset='citygeo.loadtest_polygons2_2272', out_dataset='loadtest_polygons2_6565', out_coor_system=6565)

        stmt = f'''
        SELECT pt.* FROM {table} pt
            JOIN {source_shapes_tbl} py
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
for i in range(999):
    print(f'\nLoop #{i}')
    #intersect_select(random_table)
    thread_dict = {}
    loop_start = time.time()
    for i in range(0,count):
        # Get a random table
        random_table_index = random.randrange(0, len(tables_and_srids)-1)
        z = tables_and_srids[random_table_index]

        rand_table = z[0]
        srid = z[1]

        #thread_dict[i] = threading.Thread(target=intersect_select)
        thread_dict[i] = threading.Thread(target=intersect_select, args=(rand_table,srid,))
        thread_dict[i].start()

    for key in thread_dict.keys():
        thread_dict[key].join()
        # no worky VV
        #return_value = thread_dict[key].result
        #print(f'{key} return value: {return_value}')

    loop_end = time.time() - loop_start
    print(f'Loop duration: {loop_end}')
    count += 1
    thread_dict = {}
    # Can't indent this how I want because there can't be any whitespace starting the lines
    export_txt = f'''# HELP random_intersect_gradual_latency Latency for this query
# TYPE random_intersect_gradual_latency gauge
random_intersect_gradual_latency{{amount="{i}"}} {loop_end}'''
    #print(export_txt)

    # Wait until every 9/10 seconds in a minute (9, 19, 29, etc)
    # so we can align with the prometheus scraper exactly.
    # Actually wait every 20th second until we write
    every_20th_second()

    html_file = open('/var/www/html/latency-exporter.html', 'w')
    html_file.write(export_txt)
    html_file.close()
    #time.sleep(1)

print('Done.')

#
