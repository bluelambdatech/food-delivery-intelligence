"""
This file is created by
    Omolewa Adaramola
"""


import io
import boto3
import botocore
import pandas as pd
import os
from io import StringIO
import yaml
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())



class ReadWriteFromS3:
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket_name, key):
        """
        :params: secret_key: str -> aws secret key
        :params: access_key: str -> aws access key id
        :params: bucket_name: str -> aws bucket name
        :params: key: str - > file name
        """

        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                                      aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
        self.s3_resource = boto3.resource('s3',
                                        aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                                        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))
        self.bucket_name = bucket_name
        self.key = key

    def read_s3_file(self, num_row=None):
        """
        Reads a file from an S3 bucket and returns its contents as a string or dictionary
        :params: num_rows: int -> number of rows to return
        :return: pd.DataFrame
        """    
        try:
            s3 = self.s3_client
        except botocore.exceptions.ClientError:
            exit(403)
        except botocore.exceptions.ClientError:
            print()

        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.key)  ## Gets the file from S3
        buffer = io.BytesIO()
        file_ext = self.key.split(".")[-1]
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
            s3 = self.s3_resource
            object = s3.Object(bucket_name=self.bucket_name, key=self.key)
            object.download_fileobj(buffer)
            df = pd.read_parquet(buffer)
        elif file_ext  in ["yaml", "yml"]:
            if num_row:
                print(f"This file - {self.key} cannot be handled. Please try again without num_rows specified")
                exit(500)
            else:
                df = yaml.safe_load(obj["Body"])
        else:
            print(f"{file_ext} cannot be handled")
            exit(500)
        if num_row:
            return df.head(num_row)
        return df   ## End of function

    def upload_from_local_to_s3(self, path, filename):

        df = pd.read_excel(f"{path}/{filename}")

        return df
    
    def write_to_s3(self):   
        s3_resource = self.s3_resource
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource.Object(self.bucket_name, self.key).put(Body=csv_buffer.getvalue())
        print("Copying file to S3 is now Done............")
              
    def write_to_s3(self):
        excel_buffer = StringIO()
        df.to_excel(excel_buffer)
        self.s3_resource.Object(self.bucketname, f'{key}/{df_name}').put(Body=excel_buffer.getvalue())


    def read_from_local(path, filename):
        df = pd.read_excel(f"{path}/{filename}")
        return df


if __name__ == '__main__':
    # We can call the function as follows:
    bucket_name = "uk-naija-datascience-21032023"
    key = "Folder1/Folder2/updated-file.csv"
    
    readwrite = ReadWriteFromS3(aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                                aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
                                bucket_name=bucket_name,
                                key=key)
    df = readwrite.read_s3_file(key)
    print(df)
    

