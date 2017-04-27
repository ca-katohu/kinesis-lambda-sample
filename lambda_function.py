from __future__ import print_function

from aws_kinesis_agg.deaggregator import deaggregate_records
import base64
from datetime import datetime
from io import BytesIO
import gzip
import boto3
import os

def lambda_bulk_handler(event, context):
    user_records = deaggregate_records(event['Records'])

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.environ.get('BUCKET'))
    key = datetime.now().strftime('%Y-%m-%d_%H-%M-%S.log.gz')
    file_path = '/tmp/{}'.format(key)

    with gzip.open(file_path, 'wb') as f:
        for record in user_records:
            payload = base64.b64decode(record['kinesis']['data'])
            f.write(payload)
            f.write("\n")

    bucket.upload_file(file_path, key)

    return 'Successfully processed {} records.'.format(len(user_records))
