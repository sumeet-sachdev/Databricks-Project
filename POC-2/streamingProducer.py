import boto3
import random
from datetime import datetime, timedelta

import json

sqs = boto3.client('sqs')
queue_url = "https://sqs.ap-south-1.amazonaws.com/475184346033/streaming-logs-input"

# Function to generate a random timestamp
def random_timestamp(start, end):
    time_format = '%Y-%m-%d %H:%M:%S'
    
    start_timestamp = datetime.strptime(start, time_format)
    end_timestamp = datetime.strptime(end, time_format)
    
    delta = end_timestamp - start_timestamp
    random_seconds = random.randint(0, delta.total_seconds())
    
    return (start_timestamp + timedelta(seconds=random_seconds)).strftime(time_format)

def lambda_handler(event, context):
    
    # Create JSON object in format of web_logs
    log_entry = {
        "log_id": random.randint(1000, 9999),
        "timestamp": random_timestamp("2023-08-12 00:00:00", "2023-08-12 23:59:59"),
        "customer_id": f"CUST{random.randint(100, 999)}",
        "product_id": f"PROD{random.randint(1, 10):03d}",
        "action": random.choice(["view", "add_to_cart", "purchase"]),
        "quantity": random.randint(1, 5) if random.random() < 0.5 else None,
    }
    
    # "Produce" to SQS
    response = sqs.send_message( # Send the message to the SQS queue
        QueueUrl=queue_url,
        MessageBody=str(log_entry)
    )
    
    # Logging
    print(f'Produced Log #{log_entry["log_id"]}')
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Produced Log #{log_entry["log_id"]}')
    }
