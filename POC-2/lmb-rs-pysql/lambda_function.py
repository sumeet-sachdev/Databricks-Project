import boto3
import logging
import os
import sys
import json
import pymysql
import csv

# rds_host  = "db-group-assignment2.ci9rghwaiseb.ap-south-1.rds.amazonaws.com"
# name = "admin"
# password = "admin123"
# db_name = "asignement2DB"
# DB_TABLE = 'tbl_sumeets_CRUD'
# AWS_REGION = 'ap-south-1'

# Access_key = os.getenv('ACCESS_KEY')
# Access_Secrete = os.getenv('SECRET_ACCESS_KEY')
dbname = os.getenv('dbname')
host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
# tablename = os.getenv('tablename')
tablename = 'product'


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(user=user, password=password, host=host, database=dbname)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to instance succeeded")

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'] 
    download_path = '/tmp/{}{}'.format(bucket, key)

    s3_client.download_file(bucket, key, download_path)

    csv_data = csv.reader(open(download_path))

    with conn.cursor() as cur:
        for idx, row in enumerate(csv_data):

            logger.info(row)
            try:
                cur.execute(f'INSERT INTO {tablename}(id, name)' \
                                'VALUES("%s", "%s")'
                                , row)
            except Exception as e:
                logger.error(e)

            if idx % 100 == 0:
                conn.commit()
        cur.execute(f'SELECT * FROM {tablename}')
        notification = cur.rowcount

        conn.commit()
    # client = boto3.client('sns', region_name=AWS_REGION)
    
    # response = client.publish(
    #     TargetArn = "arn:aws:sns:ap-south-1:475184346033:sns-sumeets-assignment1",
    #     Message = json.dumps({'default': notification}),
    #     MessageStructure = 'json'
    # )
    return 'File loaded into RedShift:' + str(download_path)


    # Follow this guide
    # https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

    # Update function from CLI
    # aws lambda update-function-code --function-name lmb-sumeets-a2-zip --zip-file fileb://sumeets_deployment_package.zip