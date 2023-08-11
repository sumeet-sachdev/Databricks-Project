import boto3

# Specify your AWS credentials and region
aws_access_key_id = 'xxxxxxx'
aws_secret_access_key = 'xxxxxxx'
region_name = 'ap-south-1'

# Initialize AWS S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Specify the bucket name and file path
bucket_name = 'bkt-groupassign'
s3_folder = 'input'  
local_file_path = 'C:/Users/rohan.bangera/Downloads/data/product_data.parquet'

file_name = local_file_path.split('/')[-1]

# Construct the S3 object key (key = folder name + file name)
s3_object_key = f'{s3_folder}/{file_name}'

# Upload the file to the specified S3 folder
s3.upload_file(local_file_path, bucket_name, s3_object_key)

print(f"{local_file_path} uploaded to {bucket_name}/{s3_object_key}")

