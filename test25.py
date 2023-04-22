import os
import io
import yaml
import boto3
import botocore
import pyarrow as pq
import pandas as pd


def read_s3_file(bucket_name, key, num_row=None):
    try:
        s3 = boto3.client('s3',
                          aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
    except botocore.exceptions.ClientError as e:
        print(f'Sorry, unable to locate credentials. Error: {e}')
        return

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError as e:
        print(f'Error fetching object from S3. Bucket: {bucket_name}, Key: {key}. Error: {e}')
        return

    file_name = key.split("/")[-1]
    buffer = io.BytesIO()
    file_ext = file_name.split(".")[-1]

    try:
        if file_ext in ["csv", "txt"]:
            df = pd.read_csv(obj['Body'])
            if df.shape[0] == 0:
                exit(500)
        elif file_name.split(".")[-1] in ["xls", "xlsx"]:
            df = pd.read_excel(io.BytesIO(obj['Body'].read()))
        elif file_name.split(".")[-1] == "json":
            num_row = None
            df = pd.read_json(obj['Body']).to_dict()
        elif file_name.split(".")[-1] == "parquet":
            s3 = boto3.resource('s3',
                                aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                                aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
            s3.Object(bucket_name=bucket_name, key=file_name).download_fileobj(buffer)
            df = pd.read_parquet(buffer)
        elif file_name.split(".")[-1] in ["yaml", "yml"]:
            df = yaml.safe_load(obj["Body"])
        else:
            print(f"{file_ext} can not be handled")
            return None
    except Exception as e:
        print(f'Error reading file from S3. Bucket: {bucket_name}, Key: {key}. Error: {e}')
        return None
    else:
        # Code to execute if no exception occurred
        print("No error occurred.")
    finally:
        # Code to execute regardless of whether an exception occurred or not
        print("Finally block executed.")

    if num_row:
        return df.head(num_row)
    return df


#secret_key='secret_key', access_key='access_key', bucket='bucket_name')
read_s3_file("benesek", "staff_sal.csv", 10)
