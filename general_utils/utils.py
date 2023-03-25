from io import StringIO
import boto3
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class ReadWriteFromS3:
    def __init__(self, secret_key, access_key, bucketname, key):
        """
        :params: bucket_name: str
        """

        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=access_key,
                                      aws_secret_access_key=secret_key
                                      )
        self.s3_resource = boto3.resource('s3',
                                          aws_access_key_id=access_key,
                                          aws_secret_access_key=secret_key
                                          )

        self.bucketname = bucketname
        self.key = key

    def read_s3_file(self, file_name, num_row=None):
        """
        Reads a file from an S3 bucket and returns its contents as a string.
        These are the libraries required to use this function:
        :params: filename: str
        :params: num_rows: int
        """
        # s3 = boto3.client('s3',
        #                   aws_access_key_id=os.getenv('ACCESS_KEY_ID'),  ## Fetch variables from the .env file.
        #                   aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))  ## Fetch variables from the .env file.

        obj = self.s3_client.get_object(Bucket=self.bucketname, Key=f"{self.key}/{file_name}")  ## Gets the file from S3
        if file_name.split(".")[-1] in ["csv", "txt"]:
            df = pd.read_csv(obj['Body'])  ## Reads the body of the file with pandas
        elif file_name.split(".")[-1] == "xlsx":
            df = pd.read_excel(obj['Body'])

        if num_row:
            return df.head(num_row)
        return df  ## End of function

    def upload_from_local_to_s3(self, path, filename):

        df = pd.read_excel(f"{path}/{filename}")

        return df

    def write_to_s3(self, df, key, df_name):
        excel_buffer = StringIO()
        df.to_excel(excel_buffer)
        self.s3_resource.Object(self.bucketname, f'{key}/{df_name}').put(Body=excel_buffer.getvalue())


def read_from_local(path, filename):
    df = pd.read_excel(f"{path}/{filename}")
    return df


if __name__ == '__main__':
    # We can call the function as follows:
    bucket_name = "bluelambdaproject"

    # read_s3_file(bucket_name, file_name)

    readwrite = ReadWriteFromS3(secret_key=os.getenv('ACCESS_KEY_ID'),
                                access_key=os.getenv("SECRET_ACCESS_KEY"),
                                bucketname=bucket_name,
                                key="projectA/newdata")
    df = readwrite.read_s3_file(file_name="ratings.csv")
    print(df)
