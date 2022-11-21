"""
  Triggered on the Cloud event and performs the data validation.
"""
import boto3
import os
import logging
import codecs
import pandas as pd

file_name = os.path.splitext(os.path.basename(__file__))[0]

default_log_args = {
    "level": logging.DEBUG if os.environ.get("DEBUG", 0) else logging.INFO,
    "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    "datefmt": "%d-%b-%y %H:%M"
}

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)


def get_logger(name):
    logging.basicConfig(**default_log_args)
    return logging.getLogger(name)


def get_split_char(key):
    if key.lower().endswith('.tsv'):
        return '\t'
    elif key.lower().endswith('.csv'):
        return ','
    elif key.lower().endswith('.txt'):
        return '|'
    else:
        return ' '


def lambda_handler(event: dict = None, context: dict = None):
    log = get_logger(f"{file_name}.{lambda_handler.__name__}")
    log.info('=' * 30 + " Init " + '=' * 30)
    log.info(f"[*] event - {event}")
    log.info(f"[*] context - {context}")
    records = event.get("Records", None)
    if not records:
        log.info(f"[-] No Records found in the events - {event}")
        return None

    new_files = [{'bucket': record["s3"]["bucket"]["name"],
                  'key': record['s3']['object']['key'],
                  'size': record['s3']['object']['size']}
                 for record in records
                 if record.get("s3")]

    s3 = boto3.resource("s3")
    sample_records = 100
    records = []
    column = []
    with_header = False
    for new_file in new_files:
        key = new_file['key']
        split_char = get_split_char(key)
        s3_object = s3.Object(new_file['bucket'], key)
        line_stream = codecs.getreader("utf-8")
        for line in line_stream(s3_object.get()['Body']):
            # log.info(f"line is {line})
            if with_header is True and sample_records == 100:
                column += [value.strip() for value in line.split(split_char)]
                continue
            records.append([value.strip() for value in line.split(split_char)])
            sample_records -= 1

    df = pd.DataFrame(data=records, columns=column)
    print(df.head())
    log.info('=' * 30 + " Exit " + '=' * 30)
