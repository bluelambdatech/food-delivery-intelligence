import boto3
import os
import botocore

s3 = boto3.client('s3',
                  aws_access_key_id="AKIAXBP3C7WTLSEEBIP5",  ## Fetch variables from the .env file.
                  aws_secret_access_key="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")

obj = s3.get_object(Bucket="WWWWWWWWW", Key="test.csv")
try:
    obj = s3.get_object(Bucket="WWWWWWWWWW", Key="test.csv")
except botocore.exceptions.ClientError:
    print("Omolewa")





x = 10
epsilon = 0.000000000001
y = (-50) ** (0.5)
print(y)
try:
    print(var)
except ZeroDivisionError:
    print(x / (0 + epsilon))
    print("was able to divide now")
except NameError:
    print("Joseph")


class Nene:
    def __init__(self):
        pass

    def __str__(self):
        return "My name is Omolewa"


nene = Nene()

print(nene)