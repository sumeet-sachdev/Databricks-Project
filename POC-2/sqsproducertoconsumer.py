import boto3
import json

sqs_client = boto3.client('sqs', region_name='ap-south-1')

# Defining producer and consumer SQS queue URLs
producer_queue_url = 'https://sqs.ap-south-1.amazonaws.com/475184346033/streaming-logs-input'
consumer_queue_url = 'https://sqs.ap-south-1.amazonaws.com/475184346033/sqs-lambda-redshift'

def read_from_producer_and_write_to_consumer():
    while True:
        # Receive messages from the producer SQS queue
        response = sqs_client.receive_message(
            QueueUrl=producer_queue_url,
            MaxNumberOfMessages=10,  
            WaitTimeSeconds=20  
        )
        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    message_body = json.loads(message['Body'])
                    sqs_client.send_message(
                        QueueUrl=consumer_queue_url,
                        MessageBody=json.dumps(message_body)
                    )

                    # Deleting the processed message from the producer SQS queue
                    sqs_client.delete_message(
                        QueueUrl=producer_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

                    print(f"Message {message['MessageId']} processed and sent to consumer Queue.")
                except Exception as e:
                    print(f"Error processing message {message['MessageId']}: {str(e)}")
        else:
            print("No messages received from the producer queue.")

if __name__ == '__main__':
    read_from_producer_and_write_to_consumer()