# TASK 3: CREATE A FUNCTION THAT CAN READ ANY FILE FROM S3 BUCKET

import os
import io
from io import StringIO
import yaml
import boto3
import pyarrow.parquet as pq
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
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('ACCESS_KEY_ID'),  ## Fetch variables from the .env file.
                      aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))  ## Fetch variables from the .env file.

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)  
    #obj = s3.get_object(Bucket=bucket_name, Key=f"{key}/{file_name}")
    buffer = io.BytesIO()
    if file_name.split(".")[-1] in ["csv", "txt"]:
        df = pd.read_csv(obj['Body'])
    elif file_name.split(".")[-1] in ["xls", "xlsx"]:
        df = pd.read_excel(io.BytesIO(obj['Body'].read()))
    elif file_name.split(".")[-1] == "json":
        num_row = None
        df = pd.read_json(obj['Body']).to_dict()
    elif file_name.split(".")[-1] == "parquet":

        s3 = boto3.resource('s3',
                      aws_access_key_id=os.getenv('ACCESS_KEY_ID'),  
                      aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
        object = s3.Object(bucket_name=bucket_name, key=file_name)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
    elif file_name.split(".")[-1] in ["yaml", "yml"]:
        df = yaml.safe_load(obj["Body"])

    if num_row:
        return df.head(num_row)
    return df   ## End of function


# We can call the function as follows:
bucket_name = "uk-naija-datascience-21032023"
file_name = "ny_apartment_cost_list.csv"
key = ""

file_contents = read_s3_file(bucket_name, file_name)
print (file_contents)

# sample files in S3 are:
# omolewa.csv
# ny_apartment_cost_list.csv
# myfile.txt
# season1.json
# hyp.scratch.yaml
# gdp-countries.parquet 
# new-sales-sheet.xlsx - https://stackoverflow.com/questions/61723572/how-to-read-excel-file-from-aws-in-python-3/61723955#61723955?newreg=c9c4eb2ab84a4b5cb021bf7603b01c54
# how to read a parquet file - https://stackoverflow.com/questions/51027645/how-to-read-a-single-parquet-file-in-s3-into-pandas-dataframe-using-boto3


# read_s3_file(bucket_name, file_name)
# parquet (this will return a data frame) - OK
# json (return dict)  - OK
# excel (dataframef)  - OK
# csv (return data frame) - OK
# yaml (should return a dict) - OK
# txt - return a dataframe - OK



