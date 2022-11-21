import boto3


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
