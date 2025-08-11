#!/usr/bin/env python3
"""
Example script for generating Iceberg tables using Hive Metastore (for Trino).

This example demonstrates how to use the Iceberg table generator with a Hive catalog,
which is compatible with Trino query engine.

Prerequisites:
1. Hive Metastore running and accessible
2. S3 bucket configured and accessible
3. Required Python packages installed (see requirements.txt)

Usage:
    python hive_catalog_example.py
"""

import sys
import os

# Add parent directory to path to import the generator
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generate_iceberg_tables import IcebergTableGenerator

def main():
    """Example usage with Hive catalog for Trino"""
    
    # Configuration for Hive catalog
    catalog_config = {
        'hive_metastore_uri': 'thrift://localhost:9083',  # Your Hive metastore URI
        'aws_region': 'us-east-1',
        # Optional: specific AWS credentials if not using default profile
        # 'aws_access_key_id': 'your_access_key',
        # 'aws_secret_access_key': 'your_secret_key',
    }
    
    # S3 configuration
    s3_bucket = 'your-iceberg-bucket'  # Change to your bucket
    s3_prefix = 'iceberg-tables'
    
    # Create generator
    generator = IcebergTableGenerator(
        catalog_type='hive',
        catalog_config=catalog_config,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        target_size_gb=10,  # Start with 10GB for testing
        num_workers=4
    )
    
    # Generate specific tables for testing
    test_tables = ['customers', 'products', 'orders']
    
    print("Generating Iceberg tables with Hive catalog...")
    success = generator.generate_all_tables(
        database_name='benchmark_test',
        tables=test_tables
    )
    
    if success:
        print("\nSuccess! Tables created and ready for Trino queries.")
        print("\nExample Trino queries:")
        print("  SELECT COUNT(*) FROM benchmark_test.customers;")
        print("  SELECT country, COUNT(*) FROM benchmark_test.customers GROUP BY country;")
        print("  SELECT * FROM benchmark_test.orders WHERE order_date >= DATE '2024-01-01' LIMIT 10;")
    else:
        print("Failed to generate tables. Check the logs for errors.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
