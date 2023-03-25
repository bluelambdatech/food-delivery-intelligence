import boto3
import pandas as pd
import os
##from dotenv import load_dotenv, find_dotenv
# https://stackoverflow.com/questions/73642345/how-to-securely-pass-credentials-in-python

#load_dotenv(find_dotenv())  # Loads the .env file.

def read_s3_file(bucket_name, file_name):
    """
    Reads a file from an S3 bucket and returns its contents as a string.
    These are the libraries required to use this function:
    boto3
    pandas
    python-dotenv
    """
    s3 = boto3.client('s3',
                      aws_access_key_id = os.getenv('ACCESS_KEY_ID'),  ## Fetch variables from the .env file.
                      aws_secret_access_key = os.getenv("SECRET_ACCESS_KEY"))## Fetch variables from the .env file.

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(obj['Body'])
    #return df.head()
    return df



# we can call the function as below:
bucket_name = "uk-naija-datascience-21032023"
file_name = "ny_apartment_cost_list.csv"
file_contents = read_s3_file(bucket_name, file_name)
print (file_contents)