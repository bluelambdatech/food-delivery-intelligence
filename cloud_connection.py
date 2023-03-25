from io import StringIO

import boto3

import pandas as pd
import os

from decouple import config


class connect_to_aws:

    def __init__(self, secret_key, access_key, bucketname, key):

        self.s3_client = boto3.client('s3',
                                      aws_access_key_id = access_key,
                                      aws_secret_access_key = secret_key
                                     )
        self.s3_resources = boto3.resource('s3',
                                            aws_access_key_id = access_key,
                                            aws_secret_access_key = secret_key
                                            )
        self.bucketname = bucketname
        self.key = key

    def read_from_s3(self):
        obj = self.s3_client.get_object(Bucket=self.bucketname, Key=self.key)
        df = pd.read_excel(obj['Body'])

        return df

    def read_from_local(self, path):

        df = pd.read_excel(path)

        return df

    def write_to_s3(self, df, df_path, df_name):
        excel_buffer = StringIO()
        df.to_excel(excel_buffer)
        self.s3_resource.Object(self.bucketname, f'{df_path}/{df_name}').put(Body=excel_buffer.getvalue())


secret_key = config('secret_key')
access_key = config('access_key')

if __name__ == '__main__':
    bucket = 'bluelambda-bucket-project'
    key = pd.read_excel('/Users/labi/Documents/Delivery History.xlsx')

    connect = connect_to_aws(secret_key, access_key, bucket, key)

    connect.write_to_s3()
