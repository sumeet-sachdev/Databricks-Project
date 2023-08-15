UNLOAD ('SELECT * FROM web_logs')
TO 's3://bkt-poc2-redshift/web_logs.csv'
iam_role 'arn:aws:iam::475184346033:role/rl-lambda-sqs-redshift-project2'
DELIMITER ',' ADDQUOTES ALLOWOVERWRITE PARALLEL OFF;

UNLOAD ('SELECT * FROM customer')
TO 's3://bkt-poc2-redshift/customer.csv'
iam_role 'arn:aws:iam::475184346033:role/rl-lambda-sqs-redshift-project2'
DELIMITER ',' ADDQUOTES ALLOWOVERWRITE PARALLEL OFF;

UNLOAD ('SELECT * FROM product')
TO 's3://bkt-poc2-redshift/product.csv'
iam_role 'arn:aws:iam::475184346033:role/rl-lambda-sqs-redshift-project2'
DELIMITER ',' ADDQUOTES ALLOWOVERWRITE PARALLEL OFF;
