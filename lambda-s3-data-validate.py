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


def get_row_count_of_s3_csv(bucket_name, path):
    req = boto3.client('s3').select_object_content(
        Bucket=bucket_name,
        Key=path,
        ExpressionType="SQL",
        Expression="""SELECT count(*) FROM s3object """,
        InputSerialization={"CSV": {"FileHeaderInfo": "Use",
                                    "AllowQuotedRecordDelimiter": True}},
        OutputSerialization={"CSV": {}},
    )
    row_count = next(int(x["Records"]["Payload"]) for x in req["Payload"])
    return row_count
    
    
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
    with_header = True
    data_validation_flag = True
    for new_file in new_files:
        key = new_file['key']
        bucket = new_file['bucket']
        row_count = get_row_count_of_s3_csv(bucket, key)
        log.info(f'>>>>>>> Row_Count = {row_count}')
        split_char = get_split_char(key)
        s3_object = s3.Object(new_file['bucket'], key)
        line_stream = codecs.getreader("utf-8")
        for line in line_stream(s3_object.get()['Body']):
            # log.info(f"line is {line}")
            if with_header is True and sample_records == 100:
                column += [value.strip() for value in line.split(split_char)]
                sample_records -= 1
                continue
            records.append([value.strip() for value in line.split(split_char)])
          
            
    if with_header:
        df = pd.DataFrame(data=records, columns=column)
    else:
        df = pd.DataFrame(data=records)
    # print(df.head())
    # print(df.describe())
    # print(df.columns)

    # dataframe.size
    size = df.size
    # dataframe.shape
    shape = df.shape
      
    # printing size and shape
    print("Size = {}\nShape ={}\nShape[0] x Shape[1] = {}".format(size, shape, shape[0]*shape[1]))
    # print(df.columns)
    column_count = shape[1]
    log.info(f'>>>>>>> Column_Count = {column_count}')
    
    csv_df = pd.read_csv(open('professional_claims_template.csv'))
    print(csv_df.columns)
    # print(csv_df.head())
    # print(csv_df[['Seq', 'ColumnName']].values.tolist())
    # print('|'.join(csv_df['ColumnName'].values.tolist()))
    expected_columns = csv_df['ColumnName'].values.tolist()
    actual_columns = list(df.columns)
    
    if len(expected_columns) == column_count:
        log.info('[+] Column Count Matched')
    else:
        log.info('[-] Column Count Unmatched')
        data_validation_flag = False
    
    if expected_columns == expected_columns:
        log.info('[+] Column Order & Name Matched')
    else:
        log.info('[-] Column Order & Name Unmatched')
        data_validation_flag = False
    log.info(f" >>> df['Member Sex'].isin(['M', 'F', 'U'] = {all(df['Member Sex'].isin(['M', 'F', 'U']).tolist())}")
    
    for col in ['Claim Paid Date', 'Payment Adjudication Date', 'Claim Submission Date', 'Member Date of Birth (DOB)', 'Member Date of Death (DOD)', 'Beginning Date of Service', 'Ending Date of Service']:
        m1 = df[col].eq('') | df[col].isna()
        m2 = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce').isna()
        log.info(f">>>> Date Validation - {col} = {m1.eq(m2).all()}")
    log.info('=' * 30 + " Exit " + '=' * 30)
