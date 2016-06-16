##################################################
### Elasticsearch host name
ES_HOST = "search-******************.ap-northeast-1.es.amazonaws.com"

### Elasticsearch prefix for index name
INDEX_PREFIX = "s3_access_log"

### ELB name for type name
S3_BUCKET_NAME = "*****"

### Enabled to change timezone. If you set UTC, this parameter is blank
TIMEZONE = ""

#################################################
### ELB access log format keys
S3_KEYS = ["@timestamp", "elb", "client_ip", "client_port", "backend_ip", "backend_port", "request_processing_time",
            "backend_processing_time", "response_processing_time", "elb_status_code", "backend_status_code",
            "received_bytes", "sent_bytes", "request_method", "request_url", "request_version", "user_agent"]

### ELB access log format regex
S3_REGEX = '^(.[^ ]+) (.[^ ]+) (.[^ ]+):(\\d+) (.[^ ]+):(\\d+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (.[^ ]+) (\\d+) (' \
            '\\d+) \"(\\w+) (.[^ ]+) (.[^ ]+)\" \"(.+)\"'

#################################################

import boto3
import re
import os
import json
from datetime import datetime
from dateutil import parser, tz, zoneinfo
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.endpoint import PreserveAuthSession
from botocore.credentials import Credentials

R = re.compile(S3_REGEX)
INDEX = INDEX_PREFIX + "-" + datetime.strftime(datetime.now(), "%Y%m%d")
TYPE = S3_BUCKET_NAME


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    s3 = boto3.client("s3")
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    body = obj["Body"].read()
    data = ""

    for line in body.strip().split("\n"):
        match = R.match(line)
        if not match:
            continue

        values = match.groups(0)
        doc = dict(zip(S3_KEYS, values))
        if TIMEZONE:
            doc[S3_KEYS[0]] = parser.parse(doc[S3_KEYS[0]]).replace(tzinfo=tz.tzutc()).astimezone(
                zoneinfo.gettz(TIMEZONE)).isoformat()
        data += '{"index":{"_index":"' + INDEX + '","_type":"' + TYPE + '"}}\n'
        data += json.dumps(doc) + "\n"

        if len(data) > 1000000:
            _bulk(ES_HOST, data)
            data = ""

    if data:
        print data
        _bulk(ES_HOST, data)


def _bulk(host, doc):
    credentials = _get_credentials()
    url = _create_url(host, "/_bulk")
    response = es_request(url, "POST", credentials, data=doc)
    if not response.ok:
        print response.text


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
