# Databricks notebook source
access_key = dbutils.secrets.get(scope = "aws-account", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws-account", key = "aws-access-secret-key")

#Mount s3 Bucket on DataBricks
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "bkt-poc2-redshift"
mount_name_s3 = "groups3"
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name_s3)
# display(dbutils.fs.ls("/mnt/%s" % mount_name_s3))

# COMMAND ----------

# dbutils.fs.unmount("/mnt/%s" % mount_name_s3)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("TechCoECommerce").getOrCreate()

# COMMAND ----------

# Load web logs data
web_logs_df = spark.read.option("multiline","true").json(f"/mnt/{mount_name_s3}/web_logs.json")

# Load customer data
customer_df = spark.read.csv(f"/mnt/{mount_name_s3}/customer_data.csv", header=True, inferSchema=True)

# Load product data
product_df = spark.read.csv(f"/mnt/{mount_name_s3}/product_data.csv", header=True, inferSchema=True)

# COMMAND ----------

display(customer_df)

# COMMAND ----------

display(product_df)

# COMMAND ----------

display(web_logs_df)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

web_logs_tmp = web_logs_df.withColumn('product_id', web_logs_df['product_id'].substr(-3,3).cast(IntegerType()))
web_logs_tmp.show()

# COMMAND ----------

products_table = product_df.createOrReplaceTempView('Products')
web_logs_table = web_logs_tmp.createOrReplaceTempView('Logs')
customer_table = customer_df.createOrReplaceTempView('Customer')

# COMMAND ----------

# Query to view intermediete result
# Getting all products-logs which denote a purchase
spark.sql("SELECT * FROM Products p join Logs L on p.product_id = L.product_id where action = 'purchase' order by p.product_name").show()

# COMMAND ----------

# Calculate total sales for each product based on the order quantity and product price
spark.sql("SELECT sum(price*quantity) as sales, p.product_id, p.product_name FROM Products p join Logs L on p.product_id = L.product_id where action = 'purchase' group by p.product_id, p.product_name").show()

# COMMAND ----------

# Comprehensive dataset for analysis
full_data = spark.sql('SELECT L.log_id, L.timestamp, L.customer_id, L.product_id, L.action, L.quantity, p.product_name, p.category, p.price, c.first_name, c.last_name, c.email, c.phone FROM Products p join Logs L on p.product_id = L.product_id join Customer c  on L.customer_id = c.customer_id')

# COMMAND ----------

display(full_data)

# COMMAND ----------

storage_account = 'saprojectgroup'
container_name = 'data'
source_url = f'wasbs://{container_name}@{storage_account}.blob.core.windows.net'

# COMMAND ----------

# access_key = '71pUiK3x7dKxBpTXTSOnuQ9U3pN8s0v0RaPrVUFhWW2tOgqf2h5vBsDHpw/XlFA5CFomd3F5k6rU+AStokAniw=='

access_key = dbutils.secrets.get(
    scope= "storage-account",
    key= "sa-key"
)
mount_name_sa = "sa"
mount_point_url = f'/mnt/{mount_name_sa}'

extra_configs_key = f'fs.azure.account.key.{storage_account}.blob.core.windows.net'
extra_configs_value = access_key
extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------

# Mount Storage Account on Databricks
dbutils.fs.mount(source = source_url,
                 mount_point = mount_point_url,
                 extra_configs = {
                     extra_configs_key: extra_configs_value
                 })

# COMMAND ----------

(full_data
 .coalesce(1)
 .write
 .mode("append")
 .option("header", "true")
 .format("csv")
 .save(f"/mnt/{mount_name_sa}/full_data.csv"))

# COMMAND ----------

dbutils.fs.ls(mount_point_url)
