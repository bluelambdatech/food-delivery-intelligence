# TASK 3: CREATE A FUNCTION THAT CAN READ ANY FILE FROM S3 BUCKET

import os
import io
import boto3
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())  # Loads the .env file.
#from io import StringIO

def read_s3_file(bucket_name, file_name, num_row = None):
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

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)  ## Gets the file from S3

    if file_name.split(".")[-1] in ["csv", "txt"]:
        df = pd.read_csv(obj['Body'])
    elif file_name.split(".")[-1] == "xlsx":
        df = pd.read_excel(io.BytesIO(obj['Body'].read()))
    elif file_name.split(".")[-1] == "json":
        df = pd.read_json(obj['Body'])
    elif file_name.split(".")[-1] == "parquet":
        df = pd.read_parquet(obj['Body'], engine='auto')
    elif file_name.split(".")[-1] == "yaml":
        df = pd.read_yaml(obj['Body'])

    if num_row:
        return df.head(num_row)
    return df   ## End of function


# We can call the function as follows:
bucket_name = "uk-naija-datascience-21032023"
file_name = "new-sales-sheet.xlsx"

file_contents = read_s3_file(bucket_name, file_name, 10)
dict_file_contents = file_contents.to_dict()
print(dict_file_contents)


# oustanding items:
# read a pacquet file
# read a yaml file
# write to an S3 bucket

# sample files in S3 are:
# omolewa.csv
# ny_apartment_cost_list.csv
# myfile.txt
# season1.json
# new-sales-sheet.xlsx


# how to read a parquet file
#read_s3_file(bucket_name, file_name)
#parquet (this will return a data frame) read_parquet(path[, engine, columns, ...])
#json (return dict)  read_json(path_or_buf, *[, orient, typ, ...])
#excel (df)  read_excel(io[, sheet_name, header, names, ...])
#csv (return data frame) read_csv(filepath_or_buffer, *[, sep, ...]) #txt
#yaml (dict)
