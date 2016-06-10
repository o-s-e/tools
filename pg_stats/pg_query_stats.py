#!/usr/bin/python
import sys
import os
import logging
import pg8000 as db
import boto3
import botocore
from time import gmtime, strftime
from datetime import datetime, timedelta
from operator import itemgetter

# RDS settings
rds_host = "vapondem01db001.ceg1kctzk81j.us-east-1.rds.amazonaws.com"
db_user = "postgres"
password = "sKc08Z9VJaCmuzFb0adv"
db_name = "ngdb_5_7_0_187"
port = 5432

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
This  function fetches query stats from the ngdb
"""
try:
    logger.info("trying to connect to RDS instance")
    conn = db.connect(database=db_name, user=db_user, password=password, host=rds_host, port=port)
except db.Error as e:
    logger.error("ERROR: Unexpected error, Could not connect to RDS instance %s" % [rds_host])
    logger.debug(e.message)
    sys.exit()
header = ['timestamp\t',
          'database_id\t',
          'query\t',
          'total_time\t',
          'calls\t',
          'cpu_%\t',
          'cache_hit_%\t',
          'time_per_call\n']
date_fmt = strftime("%Y_%m_%d", gmtime())
timestamp = strftime("%Y_%m_%d %H:%M:%S", gmtime())
filepath = '/tmp/AXC_36906_' + date_fmt + '.csv'
instance = 'vapondem01db001'
result_file = open(filepath, 'a')


def watcher():
    now = datetime.utcnow()
    past = now - timedelta(minutes=30)
    future = now + timedelta(minutes=30)
    try:
        session = boto3.Session(profile_name='569599628740')
        cw = session.client('cloudwatch', region_name='us-west-1')
    except botocore.exceptions.ClientError as e:
        raise Exception(e.message)
    try:
        results = cw.get_metric_statistics(Namespace='AWS/RDS',
                                           MetricName='CPUUtilization',
                                           StartTime=past,
                                           EndTime=future,
                                           Period=1,
                                           Statistics=['Average'],
                                           Unit='Percent',
                                           Dimensions=[{'Name': 'InstanceId', 'Value': instance}])

        if results['ResponseMetadata']['HTTPStatusCode'] == 200:
            if len(results['Datapoints']) > 0:
                datapoints = results['Datapoints']
                last_datapoint = sorted(datapoints, key=itemgetter('Timestamp'))[-1]
                utilization = last_datapoint['Average']
                load = round((utilization / 100.0), 2)
                timestamp = str(last_datapoint['Timestamp'])
                print("{0} load at {1}".format(load, timestamp))
            else:
                logger.error("No datapoints for: {}".format(instance))
        else:
            raise Exception("Unsuccessful response: ", results['ResponseMetadata']['HTTPStatusCode'])

    except IndexError as e:
        logger.error(e.message)


def handler():
    logger.info("SUCCESS: Connection to RDS instance succeeded")

    item_count = 0
    cursor = conn.cursor()
    statement = "SELECT  dbid, query,\
                    round(total_time::numeric, 2) AS total_time,\
                    calls,\
                    round((100 * total_time /\
                    sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu,\
                    100.0 * shared_blks_hit /\
                    nullif(shared_blks_hit + shared_blks_read, 0) AS cache_hit_percent\
                    FROM    pg_stat_statements\
                    ORDER BY total_time DESC\
                    LIMIT 20;"
    try:
        cursor.execute(statement)
        logger.info(cursor.rowcount)
        results = cursor.fetchall()
        cursor.close()
    except db.Error as e:
        logger.error('Could not execute SQL statement{}'.format(unicode(statement)))
        logger.debug(e.message)
    try:
        reset = conn.cursor()
        reset.execute("SELECT pg_stat_statements_reset();")
        reset.fetchone()
        reset.close()
    except db.Error as e:
        logger.error('Could not execute reset')
        logger.debug(e.message)
    result_file.writelines(header)
    for row in results:
        item_count += 1
        logger.info(item_count)
        try:
            database_id, query, total_time, calls, cpu_percent, cache_hit = row
        except ValueError as e:
            logger.error('Something got wrong, #fields != len(row)')
            logger.debug(e.message)
        clean_query = ''.join(unicode(query).splitlines())
        total_div_count = total_time / calls
        result_file.writelines(
            '{}\t{}\t{}\t{:.2f}\t{}\t{:.2f}\t{:.2f}\t{:.2f}\n'.format(unicode(timestamp), unicode(database_id),
                                                                      unicode(clean_query),
                                                                      float(total_time), int(calls),
                                                                      float(cpu_percent), float(cache_hit),
                                                                      float(total_div_count)))
    result_file.flush()
    return


if __name__ == "__main__":
    try:
        handler()
        # watcher()
        conn.close()
    except Exception as e:
        logger.error(e.message)
        conn.close()
