import pandas as pd

# Load the Parquet file again
df = pd.read_parquet("df_identification_20240814_sample.parquet")

# Print all column names to find if there's a column similar to 'kalknr'
print("Column names in the Parquet file:")
print(df.columns)
