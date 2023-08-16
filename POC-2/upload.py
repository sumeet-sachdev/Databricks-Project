import sys
import getopt
import pandas as pd
from configparser import ConfigParser
import boto3

config_object = ConfigParser()
config_object.read("config.ini")
access_key = config_object["AK"]
secret_access_key = config_object["SAK"]
local_path=config_object[“path”]
                         
region_name = 'ap-south-1'

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    region_name=region_name
)

bucket_name = 'bkt-poc2-input'
s3_folder = 'input'  
local_file_path = local_path

file_name = local_file_path.split('/')[-1]
s3_object_key = f'{s3_folder}/{file_name}'
s3.upload_file(local_file_path, bucket_name, s3_object_key)

print(f"{local_file_path} uploaded to {bucket_name}/{s3_object_key}")
