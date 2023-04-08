
import os
import io
from io import StringIO
import yaml
import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())  # Loads the .env file.


def read_s3_file(bucket_name, key, num_row=None):
    """
    Reads a file from an S3 bucket and returns its contents as a string.
    These are the libraries required to use this function:
    boto3
    pandas
    python-dotenv
    """
    try:
        s3 = boto3.client('s3',
                          aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
    except botocore.exceptions.ClientError as e:
        print(f"Failed to create S3 client: {e}")
        return None

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError as e:
        print(f"Failed to get S3 object: {e}")
        return None

    buffer = io.BytesIO()
    file_ext = key.split(".")[-1]
    if file_ext in ["csv", "txt"]:
        try:
            df = pd.read_csv(obj['Body'])
            if df.shape[0] == 0:
                print("CSV file is empty")
                return None
        except Exception as e:
            print(f"Failed to read CSV file: {e}")
            return None
    elif file_ext in ["xls", "xlsx"]:
        try:
            df = pd.read_excel(io.BytesIO(obj['Body'].read()))
        except Exception as e:
            print(f"Failed to read Excel file: {e}")
            return None
    elif file_ext == "json":
        num_row = None
        try:
            df = pd.read_json(obj['Body']).to_dict()
        except Exception as e:
            print(f"Failed to read JSON file: {e}")
            return None
    elif file_ext == "parquet":
        s3 = boto3.resource('s3',
                            aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
        object = s3.Object(bucket_name=bucket_name, key=key)
        try:
            object.download_fileobj(buffer)
            df = pd.read_parquet(buffer)
        except Exception as e:
            print(f"Failed to read Parquet file: {e}")
            return None
    elif file_ext in ["yaml", "yml"]:
        try:
            df = yaml.safe_load(obj["Body"])
        except Exception as e:
            print(f"Failed to read YAML file: {e}")
            return None
    else:
        print(f"{file_ext} can not be handled")
        return None

    if num_row:
        return df.head(num_row)
    return df  # End of function

