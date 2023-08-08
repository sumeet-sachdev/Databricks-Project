# Databricks notebook source
storage_account = 'saprojectgroup'
container_name = 'data'
source_url = f'wasbs://{container_name}@{storage_account}.blob.core.windows.net'

# COMMAND ----------

access_key = '71pUiK3x7dKxBpTXTSOnuQ9U3pN8s0v0RaPrVUFhWW2tOgqf2h5vBsDHpw/XlFA5CFomd3F5k6rU+AStokAniw=='
mount_name = "sa"
mount_point_url = f'/mnt/{mount_name}'

extra_configs_key = f'fs.azure.account.key.{storage_account}.blob.core.windows.net'
extra_configs_value = access_key
extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------

dbutils.fs.mount(source = source_url,
                 mount_point = mount_point_url,
                 extra_configs = {
                     extra_configs_key: extra_configs_value
                 })


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("TechCoECommerce").getOrCreate()

# COMMAND ----------

# Load web logs data
web_logs_df = spark.read.option("multiline","true").json(f"/mnt/{mount_name}/web_logs.json")

# Load customer data
customer_df = spark.read.csv(f"/mnt/{mount_name}/customer_data.csv", header=True, inferSchema=True)

# Load product data
product_df = spark.read.csv(f"/mnt/{mount_name}/product_data.csv", header=True, inferSchema=True)

# COMMAND ----------

display(customer_df)

# COMMAND ----------

display(product_df)

# COMMAND ----------

display(web_logs_df)

# COMMAND ----------

int('0001')

# COMMAND ----------

products_table = product_df.createOrReplaceTempView('Products')

# COMMAND ----------

spark.sql('SELECT * from Customers').show()

# COMMAND ----------

web_logs_df.show()

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

spark.sql("SELECT (price*quantity) as sales, p.product_id, p.product_name FROM Products p join Logs L on p.product_id = L.product_id where action = 'purchase'").show()

# COMMAND ----------

# Comprehensive dataset for analysis
spark.sql('SELECT * FROM Products p join Logs L on p.product_id = L.product_id join Customer c  on L.customer_id = c.customer_id').show()


# COMMAND ----------

# If our comprehensive data's name is eccomerce_data then queries for question number 3 

# How many unique customers visited the website?
unique_customers = spark.sql('SELECT COUNT(DISTINCT customer_id) as unique_customers FROM ecommerce_data WHERE action = "view" ').show()



# COMMAND ----------

# Do you think this can work?
# Should work if the functions exist
# But I think sql would be easier for joining
# Cool then. SQL it is.
# Yep, on it

# Clean and preprocess the web logs data

web_logs_df = web_logs_df.dropDuplicates().na.drop()

# Calculate total sales for each product
product_sales_temp_df = web_logs_df.join(product_df, "product_id")
product_sales_df = product_sales_temp_df.withColumn("total_sales", col("price") * col("quantity"))

# COMMAND ----------


