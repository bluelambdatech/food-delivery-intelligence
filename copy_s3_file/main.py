# TASK 3: CREATE A FUNCTION THAT CAN READ ANY FILE FROM S3 BUCKET

import boto3
import pyarrow
import fastparquet
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())  # Loads the .env file.


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
        df = pd.read_csv(obj['Body'])  ## Reads the body of the file with pandas
    elif file_name.split(".")[-1] == "xls":
        df = pd.read_excel(obj['Body'])
    elif file_name.split(".")[-1] == "json":
        df = pd.read_json(obj['Body'])
    elif file_name.split(".")[-1] == "parquet":
        df = pd.read_parquet(obj['Body'], engine='auto')
        #parquet_file = pq.ParquetFile('filename.parquet')
    elif file_name.split(".")[-1] == "yaml":
        df = pd.read_yaml(obj['Body'])

    if num_row:
        return df.head(num_row)
    return df   ## End of function


# We can call the function as follows:
bucket_name = "uk-naija-datascience-21032023"
file_name = "gdp-countries.parquet"

file_contents = read_s3_file(bucket_name, file_name, 10)
print(file_contents)


#### read a pacquet
#### read an excel



# omolewa.csv
# ny_apartment_cost_list.csv
# myfile.txt
# season1.json

#read_s3_file(bucket_name, file_name)
#parquet (this will return a data frame) read_parquet(path[, engine, columns, ...])
#json (return dict)  read_json(path_or_buf, *[, orient, typ, ...])
#excel (df)  read_excel(io[, sheet_name, header, names, ...])
#csv (return data frame) read_csv(filepath_or_buffer, *[, sep, ...]) #txt
#yaml (dict)
