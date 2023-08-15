import json
import psycopg2
import os

def lambda_handler(event, context):
    print("Collected Event {}".format(event))
    for record in event['Records'] :
        s3_bucket = record['s3']['bucket']['name']
        print("Bucket name is {}".format(s3_bucket))
        s3_key = record['s3']['object']['key']
        print("Object namepackag is {}".format(s3_key))
        from_path = "s3://{}/{}".format(s3_bucket, s3_key)
        print("from path {}".format(from_path))
        Access_key = os.getenv('ACCESS_KEY')
        Access_Secrete = os.getenv('SECRET_ACCESS_KEY')
        dbname = os.getenv('dbname')
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')
        # tablename = os.getenv('tablename')
        tablename = 'product'
        connection = psycopg2.connect(dbname = dbname,
                                       host = host,
                                       port = '5439',
                                       user = user,
                                       password = password)
                                       
        print('after connection....')
        curs = connection.cursor()
        print('after cursor....')
        # querry = "COPY {} FROM '{}' CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;".format(tablename,from_path,Access_key,Access_Secrete)
        query = '''CREATE TABLE example(emp_id int,emp_name varchar, salary decimal); '''
        print("query is {}".format(query))
        print('after querry....')
        curs.execute(query)
        connection.commit()
        #print(curs.fetchmany(3))
        print('Execution Finished')
        curs.close()
        print('Cursor Closed')
        connection.close()
        print('Connection Closed')