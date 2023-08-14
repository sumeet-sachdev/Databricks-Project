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
            s3key = record['s3']['object']['key']
            print("Bucket Key name is {}".format(s3key))

            parquet_object = s3_client.get_object(Bucket=s3bucket, Key=s3key)
            parquet_data = parquet_object['Body'].read()

            # Create a BytesIO file-like object from the raw Parquet data
            parquet_file = BytesIO(parquet_data)

            df = pd.read_parquet(parquet_file)

            csv_data = df.to_csv(index=False)

            file_name = s3key.split('/')[-1]
            file_name_without_extension = file_name.split('.')[0]
            to_key = f"csv_output_folder/{file_name_without_extension}.csv"

            s3_client.put_object(Body=csv_data.encode(), Bucket=s3bucket, Key=to_key)

            print(f"Converted Parquet file to CSV: s3://{s3bucket}/{s3key} -> s3://{s3bucket}/{to_key}")

    except Exception as e:
        print("An error occurred: {}".format(e))
