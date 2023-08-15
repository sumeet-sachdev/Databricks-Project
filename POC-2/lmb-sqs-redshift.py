import json
import sqlalchemy as sa
from sqlalchemy.sql import insert

redshift_username = 'awsuser'
redshift_password = 'Admin123'
redshift_endpoint = 'rs-poc2.c4ljcsdj1dix.ap-south-1.redshift.amazonaws.com:5439/dev'
jdbc_redshift_url = f'redshift+psycopg2://{redshift_username}:{redshift_password}@{redshift_endpoint}'
table_name = 'web_logs'

def lambda_handler(event, context):
    # TODO implement
    web_logs_str = ''
    for record in event['Records']:
        # print(record['body'])
        web_logs_str += record['body']
        
    # print(web_logs_str)
    json_payload = json.loads(web_logs_str.strip())
    # print(json_payload)
    
    sa_engine = sa.create_engine(jdbc_redshift_url)

    with sa_engine.connect() as conn:
        print("connected")

        try:
            conn.execute(f'insert into {table_name} values ({json_payload["log_id"]},\'{json_payload["timestamp"]}\',\'{json_payload["customer_id"]}\',\'{json_payload["product_id"]}\',\'{json_payload["action"]}\',{json_payload["quantity"]})')
            print('inserted into table')
        except Exception as e:
            print(e)    
        
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
