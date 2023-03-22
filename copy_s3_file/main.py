# pip install boto3
# pip install pandas
# pip install python-dotenv
# https://stackoverflow.com/questions/73642345/how-to-securely-pass-credentials-in-python

import boto3
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())  # Load the .env file.

def read_s3_file(bucket_name, file_name):
    s3 = boto3.client('s3',
                      aws_access_key_id = os.getenv("ACCESS_KEY_ID"), ## Fetch variables from the .env file.
                      aws_secret_access_key = os.getenv("SECRET_ACCESS_KEY"))## Fetch variables from the .env file.

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(obj['Body'])
    return df.head()

read_s3_file("uk-naija-datascience-21032023", "ny_apartment_cost_list.csv")








