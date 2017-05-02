from __future__ import print_function

from aws_kinesis_agg.deaggregator import deaggregate_records
import base64
from datetime import datetime
from io import BytesIO
import gzip
import boto3
import os
import uuid

def lambda_bulk_handler(event, context):
    user_records = deaggregate_records(event['Records'])
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.environ.get('BUCKET'))
    key ="{}_{}.log.gz".format(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'), uuid.uuid1())
    s3_path = "{}/{}".format(datetime.now().strftime('%Y/%m/%d/%H'), (key))
    file_path = '/tmp/{}'.format(key)

    with gzip.open(file_path, 'wb') as f:
        for record in user_records:
            payload = base64.b64decode(record['kinesis']['data'])
            f.write(payload)
            f.write("\n")

    bucket.upload_file(file_path, s3_path)
    os.remove(file_path)

    return 'Successfully processed {} records.'.format(len(user_records))
