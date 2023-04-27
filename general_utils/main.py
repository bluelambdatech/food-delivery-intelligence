from utils import *


bucket_name = "uk-naija-datascience-21032023"
key = "hyp.scratch.yaml"
  
readwrite = ReadWriteFromS3(aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
                            bucket_name=bucket_name,
                            key=key)
df = readwrite.read_s3_file(25)
print(df)