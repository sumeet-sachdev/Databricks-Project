import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define the paths to your CSV and Parquet files
csv_file_path = "product_data.csv"
parquet_file_path = "product_data.parquet"

# Read the CSV file into a pandas DataFrame
csv_df = pd.read_csv(csv_file_path)

# Convert pandas DataFrame to Arrow Table
table = pa.Table.from_pandas(csv_df)

# Write Arrow Table to Parquet file
pq.write_table(table, parquet_file_path)