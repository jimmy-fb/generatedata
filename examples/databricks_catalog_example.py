#!/usr/bin/env python3
"""
Example script for generating Iceberg tables using Databricks Unity Catalog (for Firebolt).

This example demonstrates how to use the Iceberg table generator with Databricks Unity Catalog,
which can be used with Firebolt and other compute engines.

Prerequisites:
1. Databricks workspace with Unity Catalog enabled
2. S3 bucket configured and accessible
3. Databricks access token
4. Required Python packages installed (see requirements.txt)

Usage:
    export DATABRICKS_TOKEN="your_databricks_token"
    python databricks_catalog_example.py
"""

import sys
import os

# Add parent directory to path to import the generator
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generate_iceberg_tables import IcebergTableGenerator

def main():
    """Example usage with Databricks Unity Catalog for Firebolt"""
    
    # Get Databricks token from environment
    databricks_token = os.getenv('DATABRICKS_TOKEN')
    if not databricks_token:
        print("Error: DATABRICKS_TOKEN environment variable is required")
        print("Set it with: export DATABRICKS_TOKEN='your_token'")
        return 1
    
    # Configuration for Databricks Unity Catalog
    catalog_config = {
        'catalog_uri': 'https://your-workspace.cloud.databricks.com/api/2.0/unity-catalog',  # Your Databricks REST API endpoint
        'credential': databricks_token,
        'aws_region': 'us-east-1',
    }
    
    # S3 configuration
    s3_bucket = 'your-iceberg-bucket'  # Change to your bucket
    s3_prefix = 'iceberg-tables'
    
    # Create generator
    generator = IcebergTableGenerator(
        catalog_type='databricks',
        catalog_config=catalog_config,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        target_size_gb=10,  # Start with 10GB for testing
        num_workers=4
    )
    
    # Generate specific tables for testing
    test_tables = ['customers', 'products', 'orders']
    
    print("Generating Iceberg tables with Databricks Unity Catalog...")
    success = generator.generate_all_tables(
        database_name='benchmark_test',
        tables=test_tables
    )
    
    if success:
        print("\nSuccess! Tables created and ready for Firebolt queries.")
        print("\nExample Firebolt queries:")
        print("  SELECT COUNT(*) FROM benchmark_test.customers;")
        print("  SELECT country, COUNT(*) FROM benchmark_test.customers GROUP BY country;")
        print("  SELECT * FROM benchmark_test.orders WHERE order_date >= '2024-01-01' LIMIT 10;")
        print("\nNote: Configure Firebolt to use this Unity Catalog as external catalog.")
    else:
        print("Failed to generate tables. Check the logs for errors.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
