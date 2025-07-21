import boto3
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pandas as pd

# Set your S3 bucket and prefix here
bucket = "your-benchmark-data-bucket"  # <-- Change to your bucket
prefix = "benchmark-data/customers/"   # <-- Change to your table prefix

# Create S3 filesystem object for pyarrow
s3 = pafs.S3FileSystem(region='us-east-1')  # Set your region if needed

# List all Parquet files in the S3 prefix
s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

if not parquet_files:
    print("No Parquet files found in the specified S3 location.")
    exit(1)

# Read schema and sample data from the first file
first_file = f"{bucket}/{parquet_files[0]}"
print(f"Reading schema from: {first_file}")

with s3.open_input_file(first_file) as f:
    parquet_file = pq.ParquetFile(f)
    schema = parquet_file.schema_arrow
    print("Schema:")
    for field in schema:
        print(f"  {field.name}: {field.type}")

    # Read a sample of the data (first 5 rows)
    batch = parquet_file.iter_batches(batch_size=5)
    df = next(batch).to_pandas()
    print("\nSample rows:")
    print(df)

# Optionally, print all column names as a list for DDL
print("\nColumn names for DDL:")
print([field.name for field in schema]) 