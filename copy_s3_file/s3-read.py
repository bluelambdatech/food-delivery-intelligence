# TASK 3: CREATE A FUNCTION THAT CAN READ ANY FILE FROM S3 BUCKET
import os
import io
from io import StringIO
import yaml
import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())  # Loads the .env file.

def read_s3_file(bucket_name, key, num_row = None):
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
    except botocore.exceptions.ClientError:
        exit(403)
    except botocore.exceptions.ClientError:
        print()
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    buffer = io.BytesIO()
    file_ext = key.split(".")[-1]
    if file_ext in ["csv", "txt"]:
        df = pd.read_csv(obj['Body'])
        if df.shape[0] == 0:
            exit(500)
    elif file_ext in ["xls", "xlsx"]:
        df = pd.read_excel(io.BytesIO(obj['Body'].read()))
    elif file_ext  == "json":
        num_row = None
        df = pd.read_json(obj['Body']).to_dict()
    elif file_ext == "parquet":
        s3 = boto3.resource('s3',
                      aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
        object = s3.Object(bucket_name=bucket_name, key=key)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
    elif file_ext  in ["yaml", "yml"]:
        if num_row:
            print(f"This file - {key} cannot be handled. Please try again without num_rows specified")
            exit(500)
        else:
            df = yaml.safe_load(obj["Body"])
    else:
        print(f"This file type {file_ext} cannot be handled at this time. Please try again later")
        exit(500)
    if num_row:
        return df.head(num_row)
    return df   ## End of function


bucket_name = "uk-naija-datascience-21032023"
key = "ny_apartment_cost_list.csv"  


file_contents = read_s3_file(bucket_name, key, 10)
print(file_contents)



