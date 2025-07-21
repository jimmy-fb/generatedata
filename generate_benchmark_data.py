#!/usr/bin/env python3
"""
High-performance data generation script for creating 1TB of parquet data for benchmarking.
Creates 7 tables with realistic data patterns and uploads to S3 in parallel.
"""

import os
import sys
import time
import random
import string
import hashlib
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple, Any, Optional
import multiprocessing as mp

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import boto3
from tqdm import tqdm

# Configuration
CHUNK_SIZE = 1_000_000  # 1M rows per chunk
TARGET_SIZE_GB = 1000  # 1TB target
S3_BUCKET = "benchmark-data-bucket"  # Change this to your bucket name
S3_PREFIX = "benchmark-data"

# Table size distribution (approximate % of total data)
TABLE_SIZES = {
    'lineitem': 0.60,      # 600GB - Largest fact table
    'events': 0.20,        # 200GB - User activity events
    'orders': 0.08,        # 80GB - Order transactions
    'customers': 0.05,     # 50GB - Customer data
    'products': 0.03,      # 30GB - Product catalog
    'suppliers': 0.02,     # 20GB - Supplier data
    'inventory': 0.02,     # 20GB - Inventory levels
}

# Data generation settings
COUNTRIES = ['US', 'CA', 'GB', 'DE', 'FR', 'IT', 'ES', 'AU', 'JP', 'KR', 'CN', 'IN', 'BR', 'MX']
REGIONS = ['North America', 'Europe', 'Asia Pacific', 'South America', 'Middle East', 'Africa']
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Automotive', 'Beauty', 'Toys']
ORDER_STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash_on_delivery']

class DataGenerator:
    def __init__(self, s3_bucket: str, s3_prefix: str, num_workers: Optional[int] = None):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3')
        self.num_workers = num_workers if num_workers is not None else mp.cpu_count()
        
        # Pre-generate lookup data for performance
        self.setup_lookup_data()
        
    def setup_lookup_data(self):
        """Pre-generate lookup data for better performance"""
        print("Setting up lookup data...")
        
        # Generate customer IDs (10M customers)
        self.num_customers = 10_000_000
        self.customer_ids = list(range(1, self.num_customers + 1))
        
        # Generate product IDs (1M products)
        self.num_products = 1_000_000
        self.product_ids = list(range(1, self.num_products + 1))
        
        # Generate supplier IDs (100K suppliers)
        self.num_suppliers = 100_000
        self.supplier_ids = list(range(1, self.num_suppliers + 1))
        
        # Generate order IDs (100M orders)
        self.num_orders = 100_000_000
        self.order_ids = list(range(1, self.num_orders + 1))
        
        print(f"Generated {len(self.customer_ids)} customers, {len(self.product_ids)} products, {len(self.supplier_ids)} suppliers")

    def generate_fake_data(self, data_type: str, size: int) -> List[Any]:
        """Generate fake data based on type"""
        if data_type == 'name':
            return [f"Name_{random.randint(1000, 9999)}" for _ in range(size)]
        elif data_type == 'email':
            return [f"user{i}@example.com" for i in range(size)]
        elif data_type == 'phone':
            return [f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}" for _ in range(size)]
        elif data_type == 'address':
            return [f"{random.randint(1, 9999)} Main St, City {random.randint(1, 1000)}" for _ in range(size)]
        elif data_type == 'description':
            return [f"Description for item {i}" for i in range(size)]
        elif data_type == 'comment':
            return [f"Comment {random.randint(1, 1000000)}" for _ in range(size)]
        else:
            return [f"data_{i}" for i in range(size)]

    def generate_customers_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of customer data"""
        start_id = chunk_id * chunk_size + 1
        end_id = min(start_id + chunk_size, self.num_customers + 1)
        actual_size = end_id - start_id
        if actual_size <= 0:
            # No data to generate for this chunk
            return pd.DataFrame()
        return pd.DataFrame({
            'customer_id': list(range(start_id, end_id)),
            'customer_name': self.generate_fake_data('name', actual_size),
            'email': self.generate_fake_data('email', actual_size),
            'phone': self.generate_fake_data('phone', actual_size),
            'address': self.generate_fake_data('address', actual_size),
            'country': np.random.choice(COUNTRIES, actual_size),
            'region': np.random.choice(REGIONS, actual_size),
            'registration_date': pd.date_range(start='2020-01-01', periods=actual_size, freq='1H'),
            'credit_score': np.random.normal(650, 100, actual_size).astype(int),
            'lifetime_value': np.random.exponential(500, actual_size).round(2),
            'is_premium': np.random.choice([True, False], actual_size, p=[0.2, 0.8]),
            'last_login': pd.date_range(start='2023-01-01', periods=actual_size, freq='1H'),
        })

    def generate_products_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of product data"""
        start_id = chunk_id * chunk_size + 1
        end_id = min(start_id + chunk_size, self.num_products + 1)
        actual_size = end_id - start_id
        
        return pd.DataFrame({
            'product_id': list(range(start_id, end_id)),
            'product_name': [f"Product_{i}" for i in range(start_id, end_id)],
            'category': np.random.choice(PRODUCT_CATEGORIES, actual_size),
            'brand': [f"Brand_{random.randint(1, 1000)}" for _ in range(actual_size)],
            'price': np.random.exponential(50, actual_size).round(2),
            'cost': np.random.exponential(50, actual_size) * np.random.uniform(0.3, 0.8, actual_size),
            'weight': np.random.exponential(2, actual_size).round(2),
            'dimensions': [f"{random.randint(1, 50)}x{random.randint(1, 50)}x{random.randint(1, 50)}" for _ in range(actual_size)],
            'description': self.generate_fake_data('description', actual_size),
            'supplier_id': np.random.choice(self.supplier_ids, actual_size),
            'created_date': pd.date_range(start='2019-01-01', periods=actual_size, freq='1H'),
            'is_active': np.random.choice([True, False], actual_size, p=[0.8, 0.2]),
        })

    def generate_suppliers_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of supplier data"""
        start_id = chunk_id * chunk_size + 1
        end_id = min(start_id + chunk_size, self.num_suppliers + 1)
        actual_size = end_id - start_id
        
        return pd.DataFrame({
            'supplier_id': list(range(start_id, end_id)),
            'supplier_name': [f"Supplier_{i}" for i in range(start_id, end_id)],
            'contact_name': self.generate_fake_data('name', actual_size),
            'email': self.generate_fake_data('email', actual_size),
            'phone': self.generate_fake_data('phone', actual_size),
            'address': self.generate_fake_data('address', actual_size),
            'country': np.random.choice(COUNTRIES, actual_size),
            'region': np.random.choice(REGIONS, actual_size),
            'rating': np.random.uniform(1, 5, actual_size).round(1),
            'established_date': pd.date_range(start='2000-01-01', periods=actual_size, freq='1D'),
            'is_verified': np.random.choice([True, False], actual_size, p=[0.7, 0.3]),
        })

    def generate_orders_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of order data"""
        start_id = chunk_id * chunk_size + 1
        end_id = min(start_id + chunk_size, self.num_orders + 1)
        actual_size = end_id - start_id
        
        return pd.DataFrame({
            'order_id': list(range(start_id, end_id)),
            'customer_id': np.random.choice(self.customer_ids, actual_size),
            'order_date': pd.date_range(start='2022-01-01', periods=actual_size, freq='1T'),
            'ship_date': pd.date_range(start='2022-01-02', periods=actual_size, freq='1T'),
            'delivery_date': pd.date_range(start='2022-01-05', periods=actual_size, freq='1T'),
            'order_status': np.random.choice(ORDER_STATUSES, actual_size),
            'payment_method': np.random.choice(PAYMENT_METHODS, actual_size),
            'total_amount': np.random.exponential(100, actual_size).round(2),
            'tax_amount': np.random.exponential(100, actual_size) * 0.1,
            'shipping_amount': np.random.exponential(10, actual_size).round(2),
            'discount_amount': np.random.exponential(5, actual_size).round(2),
            'country': np.random.choice(COUNTRIES, actual_size),
            'region': np.random.choice(REGIONS, actual_size),
        })

    def generate_lineitem_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of lineitem data (largest table)"""
        actual_size = chunk_size
        
        return pd.DataFrame({
            'lineitem_id': list(range(chunk_id * chunk_size + 1, chunk_id * chunk_size + actual_size + 1)),
            'order_id': np.random.choice(self.order_ids, actual_size),
            'product_id': np.random.choice(self.product_ids, actual_size),
            'quantity': np.random.randint(1, 20, actual_size),
            'unit_price': np.random.exponential(25, actual_size).round(2),
            'discount': np.random.uniform(0, 0.3, actual_size).round(3),
            'tax': np.random.uniform(0.05, 0.15, actual_size).round(3),
            'extended_price': np.random.randint(1, 20, actual_size) * np.random.exponential(25, actual_size),
            'discount_amount': np.random.exponential(25, actual_size) * np.random.uniform(0, 0.3, actual_size),
            'tax_amount': np.random.exponential(25, actual_size) * np.random.uniform(0.05, 0.15, actual_size),
            'net_amount': np.random.exponential(25, actual_size) * np.random.uniform(0.8, 1.2, actual_size),
            'line_status': np.random.choice(['pending', 'confirmed', 'shipped', 'delivered'], actual_size),
            'ship_date': pd.date_range(start='2022-01-01', periods=actual_size, freq='1T'),
            'comment': self.generate_fake_data('comment', actual_size),
        })

    def generate_inventory_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of inventory data"""
        actual_size = chunk_size
        
        return pd.DataFrame({
            'inventory_id': list(range(chunk_id * chunk_size + 1, chunk_id * chunk_size + actual_size + 1)),
            'product_id': np.random.choice(self.product_ids, actual_size),
            'supplier_id': np.random.choice(self.supplier_ids, actual_size),
            'warehouse_location': np.random.choice(COUNTRIES, actual_size),
            'quantity_on_hand': np.random.randint(0, 1000, actual_size),
            'quantity_allocated': np.random.randint(0, 100, actual_size),
            'reorder_point': np.random.randint(10, 100, actual_size),
            'reorder_quantity': np.random.randint(50, 500, actual_size),
            'unit_cost': np.random.exponential(20, actual_size).round(2),
            'last_updated': pd.date_range(start='2024-01-01', periods=actual_size, freq='1H'),
        })

    def generate_events_chunk(self, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate a chunk of user event data"""
        actual_size = chunk_size
        
        event_types = ['page_view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart', 'search', 'login', 'logout']
        
        return pd.DataFrame({
            'event_id': list(range(chunk_id * chunk_size + 1, chunk_id * chunk_size + actual_size + 1)),
            'customer_id': np.random.choice(self.customer_ids, actual_size),
            'event_type': np.random.choice(event_types, actual_size),
            'event_timestamp': pd.date_range(start='2024-01-01', periods=actual_size, freq='1S'),
            'page_url': [f"/page/{random.randint(1, 1000)}" for _ in range(actual_size)],
            'user_agent': [f"Mozilla/5.0 Browser {random.randint(1, 100)}" for _ in range(actual_size)],
            'ip_address': [f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}" for _ in range(actual_size)],
            'country': np.random.choice(COUNTRIES, actual_size),
            'device_type': np.random.choice(['desktop', 'mobile', 'tablet'], actual_size),
            'session_id': [f"session_{random.randint(1, 1000000)}" for _ in range(actual_size)],
            'product_id': np.random.choice(self.product_ids + [None], actual_size),
            'search_query': [f"search term {random.randint(1, 10000)}" if random.random() > 0.7 else None for _ in range(actual_size)],
        })

    def generate_and_upload_table(self, table_name: str, target_size_gb: float) -> bool:
        """Generate and upload a complete table to S3"""
        print(f"\nGenerating {table_name} table ({target_size_gb:.1f} GB)...")
        
        # Calculate number of chunks needed
        estimated_rows_per_gb = 1_000_000  # Rough estimate
        target_rows = int(target_size_gb * estimated_rows_per_gb)
        num_chunks = (target_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        print(f"Target rows: {target_rows:,}, Chunks: {num_chunks}")
        
        # Generate function mapping
        generate_funcs = {
            'customers': self.generate_customers_chunk,
            'products': self.generate_products_chunk,
            'suppliers': self.generate_suppliers_chunk,
            'orders': self.generate_orders_chunk,
            'lineitem': self.generate_lineitem_chunk,
            'inventory': self.generate_inventory_chunk,
            'events': self.generate_events_chunk,
        }
        
        generate_func = generate_funcs[table_name]
        
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            
            for chunk_id in range(num_chunks):
                future = executor.submit(self.process_chunk, table_name, chunk_id, CHUNK_SIZE, generate_func)
                futures.append(future)
            
            # Monitor progress
            completed = 0
            with tqdm(total=num_chunks, desc=f"Processing {table_name}") as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            completed += 1
                        pbar.update(1)
                    except Exception as e:
                        print(f"Error processing chunk: {e}")
                        pbar.update(1)
        
        print(f"Completed {table_name}: {completed}/{num_chunks} chunks successful")
        return completed == num_chunks

    def process_chunk(self, table_name: str, chunk_id: int, chunk_size: int, generate_func) -> bool:
        """Process a single chunk: generate data, save to parquet, upload to S3"""
        try:
            # Generate data
            df = generate_func(chunk_id, chunk_size)
            
            # Create parquet file
            filename = f"{table_name}_chunk_{chunk_id:06d}.parquet"
            local_path = f"/tmp/{filename}"
            
            # Save to parquet with compression
            df.to_parquet(local_path, compression='snappy', index=False)
            
            # Upload to S3
            s3_key = f"{self.s3_prefix}/{table_name}/{filename}"
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            
            # Clean up local file
            os.remove(local_path)
            
            return True
            
        except Exception as e:
            print(f"Error processing chunk {chunk_id} for {table_name}: {e}")
            return False

    def generate_all_tables(self):
        """Generate all tables"""
        print(f"Starting data generation for {TARGET_SIZE_GB}GB across {len(TABLE_SIZES)} tables")
        print(f"Using {self.num_workers} workers")
        print(f"S3 Bucket: {self.s3_bucket}")
        print(f"S3 Prefix: {self.s3_prefix}")
        
        start_time = time.time()
        
        for table_name, size_ratio in TABLE_SIZES.items():
            table_size_gb = TARGET_SIZE_GB * size_ratio
            success = self.generate_and_upload_table(table_name, table_size_gb)
            
            if not success:
                print(f"Failed to generate {table_name} table")
                return False
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\nData generation completed in {duration:.2f} seconds ({duration/3600:.2f} hours)")
        print(f"Generated approximately {TARGET_SIZE_GB}GB of data")
        
        return True

    def create_manifest_file(self):
        """Create a manifest file with table information"""
        manifest = {
            'generation_date': datetime.now().isoformat(),
            'target_size_gb': TARGET_SIZE_GB,
            'tables': TABLE_SIZES,
            's3_bucket': self.s3_bucket,
            's3_prefix': self.s3_prefix,
            'chunk_size': CHUNK_SIZE,
            'data_format': 'parquet',
            'compression': 'snappy'
        }
        
        import json
        manifest_path = "/tmp/data_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Upload manifest to S3
        s3_key = f"{self.s3_prefix}/data_manifest.json"
        self.s3_client.upload_file(manifest_path, self.s3_bucket, s3_key)
        os.remove(manifest_path)
        
        print(f"Manifest uploaded to s3://{self.s3_bucket}/{s3_key}")

def main():
    parser = argparse.ArgumentParser(description='Generate 1TB of benchmark data in parquet format')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='benchmark-data', help='S3 prefix (default: benchmark-data)')
    parser.add_argument('--workers', type=int, help='Number of worker threads (default: CPU count)')
    parser.add_argument('--size-gb', type=int, default=1000, help='Target size in GB (default: 1000)')
    parser.add_argument('--table', action='append', help='Table to generate (can be specified multiple times)')
    
    args = parser.parse_args()
    
    global TARGET_SIZE_GB, S3_BUCKET, S3_PREFIX
    TARGET_SIZE_GB = args.size_gb
    S3_BUCKET = args.bucket
    S3_PREFIX = args.prefix
    
    # Validate S3 access
    try:
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=args.bucket)
        print(f"S3 bucket '{args.bucket}' is accessible")
    except Exception as e:
        print(f"Error accessing S3 bucket '{args.bucket}': {e}")
        print("Make sure your AWS credentials are configured and the bucket exists")
        return 1
    
    # Generate data
    generator = DataGenerator(args.bucket, args.prefix, args.workers)
    
    # Determine which tables to generate
    if args.table:
        tables_to_generate = [t for t in args.table if t in TABLE_SIZES]
        if not tables_to_generate:
            print(f"No valid tables specified. Valid options: {list(TABLE_SIZES.keys())}")
            return 1
        print(f"Only generating tables: {tables_to_generate}")
        for table_name in tables_to_generate:
            table_size_gb = TARGET_SIZE_GB * TABLE_SIZES[table_name]
            success = generator.generate_and_upload_table(table_name, table_size_gb)
            if not success:
                print(f"Failed to generate {table_name} table")
                return 1
        generator.create_manifest_file()
        print("\nData generation completed successfully!")
        print(f"Data available at: s3://{args.bucket}/{args.prefix}/")
        return 0
    else:
        if generator.generate_all_tables():
            generator.create_manifest_file()
            print("\nData generation completed successfully!")
            print(f"Data available at: s3://{args.bucket}/{args.prefix}/")
            return 0
        else:
            print("\nData generation failed!")
            return 1

if __name__ == "__main__":
    sys.exit(main()) 