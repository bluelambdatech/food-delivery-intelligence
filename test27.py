import os
import io
import yaml
import boto3
import botocore
import pyarrow as pq
import pandas as pd


def read_s3_file(bucket_name: str, key: str, num_rows: int = None) -> pd.DataFrame:
    """
    Reads a file from S3 and returns a pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The S3 object key.
        num_rows (int, optional): The number of rows to return.

    Returns:
        pandas.DataFrame: The data in the file as a DataFrame.

    Raises:
        ValueError: If input parameters are invalid.
        RuntimeError: If an error occurs while reading the file.
    """
    if not bucket_name or not key:
        raise ValueError("Bucket name and key are required.")

    try:
        s3 = boto3.client('s3',
                          aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Unable to locate credentials. Error: {e}")

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Error fetching object from S3. Bucket: {bucket_name}, Key: {key}. Error: {e}")

    file_name = key.split("/")[-1]
    buffer = io.BytesIO()
    file_ext = file_name.split(".")[-1]

    try:
        if file_ext in ["csv", "txt"]:
            df = pd.read_csv(obj['Body'])
            if df.shape[0] == 0:
                raise RuntimeError("Empty DataFrame.")
        elif file_name.split(".")[-1] in ["xls", "xlsx"]:
            df = pd.read_excel(io.BytesIO(obj['Body'].read()))
        elif file_name.split(".")[-1] == "json":
            num_rows = None
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
            raise ValueError(f"File extension '{file_ext}' cannot be handled.")
    except Exception as e:
        raise RuntimeError(f"Error reading file from S3. Bucket: {bucket_name}, Key: {key}. Error: {e}")

    if num_rows:
        df = df.head(num_rows)

    return df


bucket_name = "benesek"
key = "staff_sal.csv"   #this meane the folder path to the file


file_contents = read_s3_file(bucket_name, key, 10)
print (file_contents)
#