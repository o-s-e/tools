from __future__ import print_function

import boto3
import re
import os
import json
import logging
from datetime import datetime, date
from dateutil import parser, tz, zoneinfo
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.endpoint import PreserveAuthSession
from botocore.credentials import Credentials
from botocore import exceptions as Botoxeption

##################################################
# Elasticsearch host name
ES_HOST = "search-logging-6osjscomfvnhvucoql45mga4ly.eu-west-1.es.amazonaws.com"

# Elasticsearch prefix for index name
INDEX_PREFIX = "s3_access_log"

# ELB name for type name
S3_BUCKET_NAME = "osetest-logs"

#################################################
# S3 access log format keys
S3_KEYS = ["owner_id", "bucket", "_timestamp", "client_ip", "requester", "request_id", "operation",
           "key", "request_uri", "http_status_code", "error_code",
           "bytes_send", "object_size", "total_time", "turn_around_time", "referrer", "user_agent", "version_id"]

# S3 access log format regex
S3_REGEX = '(\S+) (\S+) \[(.*?)\s\+0000\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]+)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ' \
           '"([^"]+)" "([^"]+)" (\S+)'

FMT_IN = '%d/%b/%Y:%H:%M:%S'
FMT_OUT = '%Y-%m-%d %H:%M:%S'

#################################################

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

R = re.compile(S3_REGEX)
INDEX = INDEX_PREFIX + "-" + datetime.strftime(datetime.now(), "%Y.%m.%d")
TYPE = S3_BUCKET_NAME


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    logger.debug('Got event{} for key {}'.format(json.dumps(event, indent=2), str(key)))

    try:
        logger.info('Get the s3 object: {}'.format(str(key)))
        s3 = boto3.client("s3")
        obj = s3.get_object(
            Bucket=bucket,
            Key=key
        )
    except Botoxeption.ClientError as e:
        logger.error('something got wrong: {}'.format(str(e)))

    logger.info('extract body')
    body = obj["Body"].read()
    data = ""

    for line in body.strip().split("\n"):
        match = R.match(line)
        if not match:
            logger.error('no match for: {}'.format(str(line)))
            continue

        values = match.groups(0)
        doc = dict(zip(S3_KEYS, values))

        doc[S3_KEYS[2]] = transform_timestamp(doc[S3_KEYS[2]])

        data += '{"index":{"_index":"' + INDEX + '","_type":"' + TYPE + '"}}\n'
        data += json.dumps(doc) + "\n"
        logger.debug('a json doc in all its glory: {}'.format(str(json.dumps(doc))))

        if len(data) > 1000000:
            _bulk(ES_HOST, data)
            data = ""

    if data:
        _bulk(ES_HOST, data)


def _bulk(host, doc):
    credentials = _get_credentials()
    url = _create_url(host, "/_bulk")
    logger.info('ingest to es: {}'.format(str(url)))
    response = es_request(url, "POST", credentials, data=doc)
    if not response.ok:
        logger.error('Response Error: {}'.format(str(response.text)))


def transform_timestamp(timestamp):
    d = datetime.strptime(timestamp, FMT_IN)
    return date.strftime(d, FMT_OUT)


def _get_credentials():
    return Credentials(
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        os.environ["AWS_SESSION_TOKEN"])


def _create_url(host, path, ssl=False):
    if not path.startswith("/"):
        path = "/" + path

    if ssl:
        return "https://" + host + path
    else:
        return "http://" + host + path


def request(url, method, credentials, service_name, region=None, headers=None, data=None):
    if not region:
        region = os.environ["AWS_REGION"]

    aws_request = AWSRequest(url=url, method=method, headers=headers, data=data)
    SigV4Auth(credentials, service_name, region).add_auth(aws_request)
    return PreserveAuthSession().send(aws_request.prepare())


def es_request(url, method, credentials, region=None, headers=None, data=None):
    return request(url, method, credentials, "es", region, headers, data)
