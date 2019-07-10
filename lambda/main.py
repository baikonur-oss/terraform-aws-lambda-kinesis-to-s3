import base64
import datetime
import gzip
import io
import json
import logging
import os
import urllib.parse
import uuid
from json import JSONDecodeError

import boto3
import dateutil.parser
from aws_kinesis_agg.deaggregator import iter_deaggregate_records
from aws_xray_sdk.core import patch
from aws_xray_sdk.core import xray_recorder
from boto3.exceptions import S3UploadFailedError

# set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading function')

# when debug logging is needed, uncomment following lines:
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

# patch boto3 with X-Ray
libraries = ('boto3', 'botocore')
patch(libraries)

# global S3 client instance
s3 = boto3.client('s3')

# configure with env vars
PATH_PREFIX = os.environ['LOG_S3_PREFIX']
BUCKET_NAME = os.environ['LOG_S3_BUCKET']

# consts
LOG_ID_FIELD: str = os.environ['LOG_ID_FIELD']
LOG_TYPE_FIELD: str = os.environ['LOG_TYPE_FIELD']
LOG_TIMESTAMP_FIELD: str = os.environ['LOG_TIMESTAMP_FIELD']
LOG_TYPE_UNKNOWN_PREFIX: str = os.environ['LOG_TYPE_UNKNOWN_PREFIX']


def put_to_s3(key: str, bucket: str, data: str):
    # gzip and put data to s3 in-memory
    xray_recorder.begin_subsegment('gzip compress')
    data_gz = gzip.compress(data.encode(), compresslevel=9)
    xray_recorder.end_subsegment()

    xray_recorder.begin_subsegment('s3 upload')
    try:
        with io.BytesIO(data_gz) as data_gz_fileobj:
            s3_results = s3.upload_fileobj(data_gz_fileobj, bucket, key)

        logger.info(f"S3 upload errors: {s3_results}")

    except S3UploadFailedError as e:
        logger.error("Upload failed. Error:")
        logger.error(e)
        import traceback
        traceback.print_stack()
        raise
    xray_recorder.end_subsegment()


def append_to_dict(dictionary: dict, log_type: str, log_data: object, log_timestamp=None, log_id=None):
    if log_type not in dictionary:
        # we've got first record for this type, initialize value for type

        # first record timestamp to use in file path
        if log_timestamp:
            try:
                log_timestamp = dateutil.parser.parse(log_timestamp)
            except TypeError:
                logger.error(f"Bad timestamp: {log_timestamp}")
                logger.info(f"Falling back to current time for type \"{log_type}\"")
                log_timestamp = datetime.datetime.now()
        else:
            logger.info(f"No timestamp for first record")
            logger.info(f"Falling back to current time for type \"{log_type}\"")
            log_timestamp = datetime.datetime.now()

        # first record log_id field to use as filename suffix to prevent duplicate files
        if log_id:
            logger.info(f"Using first log record ID as filename suffix: {log_id}")
        else:
            log_id = str(uuid.uuid4())
            logger.info(f"First log record ID is not available, using random ID as filename suffix instead: {log_id}")

        dictionary[log_type] = {
            'records':         list(),
            'first_timestamp': log_timestamp,
            'first_id':        log_id,
        }

    dictionary[log_type]['records'].append(log_data)

    
def normalize_kinesis_payload(payload: dict):
    # Normalize messages from CloudWatch (subscription filters) and pass through anything else
    # https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html

    logger.debug(f"normalizer input: {payload}")

    payloads = []

    if len(payload) < 1:
        logger.error(f"Got weird record: \"{payload}\", skipping")
        return payloads

    # check if data is JSON and parse
    try:
        payload = json.loads(payload)

    except JSONDecodeError:
        logger.error(f"Non-JSON data found: {payload}, giving up")
        return payloads

    if 'messageType' in payload:
        logger.debug(f"Got payload looking like CloudWatch Logs via subscription filters: "
                     f"{payload}")

        if payload['messageType'] == "DATA_MESSAGE":
            if 'logEvents' in payload:
                for event in payload['logEvents']:
                    # check if data is JSON and parse
                    try:
                        logger.debug(f"message: {event['message']}")
                        payload_parsed = json.loads(event['message'])
                        logger.debug(f"parsed payload: {payload_parsed}")

                    except JSONDecodeError:
                        logger.debug(f"Non-JSON data found inside CWL message: {event}, giving up")
                        continue

                    payloads.append(payload_parsed)

            else:
                logger.error(f"Got DATA_MESSAGE from CloudWatch but logEvents are not present, "
                             f"skipping payload: {payload}")

        elif payload['messageType'] == "CONTROL_MESSAGE":
            logger.info(f"Got CONTROL_MESSAGE from CloudWatch: {payload}, skipping")
            return payloads

        else:
            logger.error(f"Got unknown messageType, shutting down")
            raise ValueError(f"Unknown messageType: {payload}")
    else:
        payloads.append(payload)
        
    return payloads


def decode_validate(raw_records: list):
    xray_recorder.begin_subsegment('decode and validate')

    log_dict = dict()

    processed_records = 0

    for record in iter_deaggregate_records(raw_records):
        logger.debug(f"raw Kinesis record: {record}")
        # Kinesis data is base64 encoded
        decoded_data = base64.b64decode(record['kinesis']['data'])

        # check if base64 contents is gzip
        # gzip magic number 0x1f 0x8b
        if decoded_data[0] == 0x1f and decoded_data[1] == 0x8b:
            decoded_data = gzip.decompress(decoded_data)

        decoded_data = decoded_data.decode()
        normalized_payloads = normalize_kinesis_payload(decoded_data)
        logger.debug(f"Normalized payloads: {normalized_payloads}")

        for normalized_payload in normalized_payloads:
            logger.debug(f"Parsing normalized payload: {normalized_payload}")

            processed_records += 1

            # get log id when available
            log_id = normalized_payload.setdefault(LOG_ID_FIELD, None)

            # check if log type field is available
            try:
                log_type = normalized_payload[LOG_TYPE_FIELD]

            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TYPE_FIELD}\" "
                             f"from payload: {normalized_payload}")
                log_type = f"{LOG_TYPE_UNKNOWN_PREFIX}/unknown_type"
                logger.error(f"Marking as {log_type}")

            # check if timestamp is present
            try:
                timestamp = normalized_payload[LOG_TIMESTAMP_FIELD]

            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TIMESTAMP_FIELD}\" "
                             f"from payload: {normalized_payload}")
                if not log_type.startswith(LOG_TYPE_UNKNOWN_PREFIX):
                    log_type = f"{LOG_TYPE_UNKNOWN_PREFIX}/{log_type}"
                log_type += "_no_timestamp"
                logger.error(f"Re-marking as {log_type} and giving up")
                append_to_dict(log_dict, log_type, normalized_payload, log_id=log_id)
                continue

            # valid data
            append_to_dict(log_dict, log_type, normalized_payload, log_timestamp=timestamp, log_id=log_id)

    logger.info(f"Processed {processed_records} records from Kinesis")
    xray_recorder.end_subsegment()
    return log_dict


def upload_by_type(log_dict: dict):
    xray_recorder.begin_subsegment('upload')
    for log_type, records_object in log_dict.items():
        logger.info(f"Processing log type {log_type}")

        records = records_object['records']

        if len(records) < 1:
            continue

        first_timestamp = records_object['first_timestamp']
        first_id = records_object['first_id']
        first_id = urllib.parse.quote(first_id)

        # slashes in S3 object keys are like "directory" separators, like in ordinary filesystem paths
        key = PATH_PREFIX + '/' + log_type + '/'
        key += first_timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S-") + first_id + ".gz"

        logging.info(f"Starting upload to S3: s3://{BUCKET_NAME}/{key}")
        subsegment = xray_recorder.begin_subsegment('upload one type')
        subsegment.put_annotation('type', log_type)
        subsegment.put_annotation('count', len(records))

        data = '\n'.join(str(record) for record in records)
        put_to_s3(key, BUCKET_NAME, data)

        xray_recorder.end_subsegment()
        logger.info(f"Upload finished successfully")

    xray_recorder.end_subsegment()


def handler(event, context):
    raw_records = event['Records']

    log_dict = decode_validate(raw_records)

    upload_by_type(log_dict)
