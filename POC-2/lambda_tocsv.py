import pandas as pd
import boto3
from io import BytesIO

def lambda_handler(event, context):
    try:
        print("Event collected is {}".format(event))

        if 'Records' not in event:
            print("No 'Records' key in the event.")
            return

        s3_client = boto3.client('s3')

        for record in event['Records']:
            s3bucket = record['s3']['bucket']['name']
            print("Bucket Name is {}".format(s3bucket))
            
            s3outputbucket = "bkt-poc2-output"
            
            s3key = record['s3']['object']['key']
            print("Bucket Key name is {}".format(s3key))

            format_object = s3_client.get_object(Bucket=s3bucket, Key=s3key)
            format_data = format_object['Body'].read()

            # Create a BytesIO file-like object from the raw Parquet data
            format_file = BytesIO(format_data)
            
            if s3key.split('.')[-1].endswith('parquet'):

                df = pd.read_parquet(format_file)
                
            if s3key.split('.')[-1].endswith('json'):

                df = pd.read_json(format_file) 
                
            if s3key.split('.')[-1].endswith('csv'):

                df = pd.read_csv(format_file)
                
            csv_data = df.to_csv(index=False)

            file_name = s3key.split('/')[-1]
            file_name_without_extension = file_name.split('.')[0]
            to_key = f"{file_name_without_extension}.csv"

            s3_client.put_object(Body=csv_data.encode(), Bucket=s3outputbucket, Key=to_key)

            print(f"Converted  input file to CSV: s3://{s3bucket}/{s3key} -> s3://{s3outputbucket}/{to_key}")

    except Exception as e:
        print("An error occurred: {}".format(e))
