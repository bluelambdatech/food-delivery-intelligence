# TASK 3: CREATE A FUNCTION THAT CAN READ ANY FILE FROM S3 BUCKET

import boto3
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
    elif file_name.split(".")[-1] == "xlsx":
        df = pd.read_excel(obj['Body'])

    if num_row:
        return df.head(num_row)
    return df   ## End of function


# We can call the function as follows:
bucket_name = "uk-naija-datascience-21032023"
file_name = "omolewa.json"
#read_s3_file(bucket_name, file_name)

file_contents = read_s3_file(bucket_name, file_name, 10)
print(file_contents)


# omolewa.csv
# ny_apartment_cost_list.csv
# myfile.txt
#parquet (this will return a data frame)
#json (return dict)
#txt
#csv (return data frame)
#excel (df)
#yaml (dict)