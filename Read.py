import pandas as pd

# Path to the Parquet file
PARQUET_FILE = "df_identification_20240814_sample.parquet"

# Function to read and display data or download it as a CSV file
def read_parquet_file(parquet_file):
    print(f"Reading data from Parquet file: {parquet_file}")
    try:
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(parquet_file)
        print("Data successfully loaded.")
        
        # Show the number of records
        total_records = len(df)
        print(f"Total number of records: {total_records}")
        
        # Check if records are fewer than 150
        if total_records < 100:
            # Display all records
            print("\nData Preview:")
            print(df)
        else:
            # Save DataFrame to a CSV file
            output_file = "output_data.csv"
            df.to_csv(output_file, index=False)
            print(f"Data is large. Exported to CSV file: {output_file}")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")

# Run the function
read_parquet_file(PARQUET_FILE)
