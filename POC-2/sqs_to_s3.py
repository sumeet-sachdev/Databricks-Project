import boto3
import json
import datetime
sqs_client = boto3.client('sqs', region_name='ap-south-1')
s3_client = boto3.client('s3', region_name='ap-south-1')
s3_object_key="stream_data_collection.json"

# Defining SQS queue URL and S3 bucket name and object key for json file
sqs_queue_url = "https://sqs.ap-south-1.amazonaws.com/475184346033/streaming-logs-input "
consumer_queue_url = 'https://sqs.ap-south-1.amazonaws.com/475184346033/sqs-lambda-redshift'
s3_bucket_name = "bkt-poc2-input"
 
 #reading data from sqs and write to s3
def read_from_sqs_and_write_to_s3():
    collected_messages = []  # List to hold all the received messages
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    current_object_key = f"{s3_object_key}_{current_time}.json"
    
    while True:
        # Receive messages from SQS
        response = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10,  
            WaitTimeSeconds=20  
        )

        # Check if any messages were received
        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    message_body = json.loads(message['Body'])
                    collected_messages.append(message_body)
                    sqs_client.send_message(
                        QueueUrl=consumer_queue_url,
                        MessageBody=json.dumps(message_body)
                    )

                    # Deleting the processed message from SQS
                    sqs_client.delete_message(
                        QueueUrl=sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

                    print(f"Message {message['MessageId']} processed.")
                except Exception as e:
                    print(f"Error processing message {message['MessageId']}: {str(e)}")
        else:
            print("No messages received from SQS.")

        # Writing the collected messages to S3
        if len(collected_messages)>50:
            try:
                s3_client.put_object(
                    Bucket=s3_bucket_name,
                    Key=current_object_key,
                    Body=json.dumps(collected_messages, indent=2)
                )
                collected_messages=[]
                print(f"Collected batch of {len(collected_messages)} messages written to {current_object_key} in S3.")
                print(f"Message {message['MessageId']} processed and sent to consumer Queue.")
            except Exception as e:
                print(f"Error writing collected messages to S3: {str(e)}")

if __name__ == '__main__':
    read_from_sqs_and_write_to_s3()
