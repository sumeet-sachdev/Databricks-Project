import json
import logging
import boto3
import psycopg2
import csv
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    print("Collected Event {}".format(event))
    s3_client = boto3.client('s3')
    for record in event['Records'] :
        s3_bucket = record['s3']['bucket']['name']
        print("Bucket name is {}".format(s3_bucket))
        s3_key = record['s3']['object']['key']
        print("Object namepackag is {}".format(s3_key))
        if s3_key.startswith('stream'):
            continue
        from_path = "s3://{}/{}".format(s3_bucket, s3_key)
        print("from path {}".format(from_path))


        obj = s3_client.get_object(Bucket= s3_bucket, Key= s3_key)
        download_path = '/tmp/{}{}'.format(s3_bucket, s3_key)
        s3_client.download_file(s3_bucket, s3_key, download_path)


        dbname = os.getenv('dbname')
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')

        if s3_key.startswith('product'):
            tablename = 'product'
        if s3_key.startswith('customer'):
            tablename = 'customer'
        if s3_key.startswith('web'):
            tablename = 'web_logs'
        
        logger.info(tablename)

        connection = psycopg2.connect(dbname = dbname,
                                       host = host,
                                       port = '5439',
                                       user = user,
                                       password = password)
                                       
        print('after connection....')
        curs = connection.cursor()
        print('after cursor....')
        # querry = "COPY {} FROM '{}' CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;".format(tablename,from_path,Access_key,Access_Secrete)
        csv_data = csv.reader(open(download_path))
        next(csv_data, None)
        for idx, row in enumerate(csv_data):
            logger.info(row)
            try:
                if tablename == 'product':
                    row[3] = float(row[3])
                    curs.execute(f'INSERT INTO {tablename}(product_id, product_name, category, price)' \
                                    f'VALUES(\'{row[0]}\', \'{row[1]}\', \'{row[2]}\', {row[3]})'
                                    , row)
                    
                if tablename == 'customer':
                    query = f'INSERT INTO {tablename}(customer_id, first_name, last_name, email, phone)' \
                                    f' VALUES(\'{row[0]}\', \'{row[1]}\', \'{row[2]}\', \'{row[3]}\', \'{row[4]}\')'
                    curs.execute(query)
                    logger.info(query)
                    
                elif tablename == 'web_logs':
                    row[5] = int(float(row[5])) if row[5] else 1
                    row[0] = int(float(row[0]))
                    curs.execute(f'INSERT INTO {tablename}(log_id, tstamp, customer_id, product_id, action, quantity)' \
                                    f'VALUES({row[0]}, \'{row[1]}\', \'{row[2]}\', \'{row[3]}\', \'{row[4]}\', {row[5]})'
                                    , row)
            except Exception as e:
                logger.error(e)
        # query = '''CREATE TABLE example(emp_id int,emp_name varchar, salary decimal); '''
        # print("query is {}".format(query))
        # print('after querry....')
        # curs.execute(query)
        connection.commit()
        #print(curs.fetchmany(3))
        print('Execution Finished')
        curs.close()
        print('Cursor Closed')
        connection.close()
        print('Connection Closed')