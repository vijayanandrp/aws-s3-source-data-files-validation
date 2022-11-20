"""
  Triggered on the Cloud event 
"""
import boto3
import os
import logging
import codecs


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

'''def compare_dict(x, y):
    log.info(y)
    shared_items = {k: x[k] for k in x if k in y and x[k] == y[k]}
    if len(shared_items) == len(x) == len(y):
        return True
    else:
        return False '''
column_size = 73
column_metadata = [{"index": 1, "name": "Payer Name"}, {"index": 2, "name": "File Type"}]

def compare_order(expected_order, received_columns):
    for column in received_columns:
        column_index = column["index"]
        if column_index in expected_order:
            expected_result = expected_order[column["index"]]
            if expected_result["index"] == column["index"] and expected_result["name"] == column["name"]:
                print(f"Column matching index {column['index']}")
            else:
                print(f"column not matching")
                return False
        else:
            print(f"column index not found in result {column_index}")
            return False
    return True


def compare_size(received_columns):
    if len(received_columns) == column_size:
        return True
    else:
        return False

#templates = [{1: 'User name', 2: 'Password', 3: 'Access key ID', 4: 'Secret access key', 5: 'Console login link'}, {1: 'sha1'}]

def lambda_handler(event: dict = None, context: dict = None):
    log = get_logger(f"{file_name}.{lambda_handler.__name__}")
    log.info('=' * 30 + " Init " + '=' * 30)
    log.info(f"event - {event}")
    log.info(f"context - {context}")
    records = event.get("Records", None)
    if not records:
        log.info(f"[-] No Records found in the events - {event}")
        return None

    new_files = [{'bucket': record["s3"]["bucket"]["name"],
                  'key': record['s3']['object']['key'],  # m5/insights/new_user_report/date=2022-05-22
                  'size': record['s3']['object']['size']}
                 for record in records
                 if record.get("s3")]
    
    
    for new_file in new_files:
        key = new_file['key']
        s3 = boto3.resource("s3")
        s3_object = s3.Object(new_file['bucket'], key)
        line_stream = codecs.getreader("utf-8")

        split_char = '|'
        if key.lower().endswith('.tsv'):
            split_char = '\t'
        elif key.lower().endswith('.csv'):
            split_char = ','
        elif key.lower().endswith('.txt'):
            spit_char = '|'
        headers = None
        for line in line_stream(s3_object.get()['Body']):
            #log.info(f"line is {line})
            if line.strip():
                headers = [({"index": idx + 1, "name": l.strip()}) for idx, l in enumerate(line.split(split_char))]
                break
        column_dct = {column_metadata[i]["index"]: column_metadata[i] for i in range(0, len(column_metadata), 1)}
        log.info(f"Headers is {headers}")
        size_comparison_flag = compare_size(headers)
        order_comparison_flag = compare_order(column_dct, headers)
        log.info(f"size comparison flag {size_comparison_flag}")
        log.info(f"order comparion flag {order_comparison_flag}")
    log.info('=' * 30 + " Exit " + '=' * 30)
    return
    
    
    
    
    
