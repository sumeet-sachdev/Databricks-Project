# Databricks notebook source
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

# COMMAND ----------

# Load the transformed data from Storage account
storage_account = 'saprojectgroup'
container_name = 'data'
source_url = f'wasbs://{container_name}@{storage_account}.blob.core.windows.net'

access_key = dbutils.secrets.get(
    scope= "storage-account",
    key= "sa-key"
)
mount_name = "sa"
mount_point_url = f'/mnt/{mount_name}'

extra_configs_key = f'fs.azure.account.key.{storage_account}.blob.core.windows.net'
extra_configs_value = access_key
extra_configs_dict = {extra_configs_key:extra_configs_value}

dbutils.fs.mount(source = source_url,
                 mount_point = mount_point_url,
                 extra_configs = {
                     extra_configs_key: extra_configs_value
                 })

# COMMAND ----------

df = spark.read.csv(f"/mnt/{mount_name}/full_data.csv", header=True, inferSchema=True).toPandas()
df.head()

# COMMAND ----------

ax = df['action'].hist(bins=3, grid=False, figsize=(12,8), color='#86bf91', zorder=2, rwidth=0.6)

ax = ax[0]

