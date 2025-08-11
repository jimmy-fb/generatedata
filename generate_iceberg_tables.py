#!/usr/bin/env python3
"""
Apache Iceberg table format data generation script.
Supports Hive metadata store (for Trino) and Databricks Unity Catalog (for Firebolt).

This script creates Iceberg tables with realistic data patterns and supports:
- Multiple catalog types (Hive, Databricks Unity Catalog)
- S3 storage backend
- Partitioned tables for performance
- Schema evolution capabilities
- Time travel and versioning
"""

import os
import sys
import time
import random
import string
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Any, Optional, Union
import multiprocessing as mp
from pathlib import Path
import uuid

import pandas as pd
import pyarrow as pa
import numpy as np
import boto3
from tqdm import tqdm

# Iceberg imports
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.table import Table
from pyiceberg import schema, types
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import days, hours, bucket, truncate

# Databricks imports (optional)
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

# Configuration
CHUNK_SIZE = 500_000  # 500K rows per chunk for Iceberg
S3_BUCKET = "iceberg-data-bucket"
S3_PREFIX = "iceberg-tables"

# Table configurations with Iceberg-optimized schemas
ICEBERG_TABLES = {
    'customers': {
        'size_ratio': 0.05,
        'partition_spec': ['country', 'registration_date'],
        'sort_order': ['customer_id'],
        'schema': pa.schema([
            pa.field('customer_id', pa.int64()),
            pa.field('customer_name', pa.string()),
            pa.field('email', pa.string()),
            pa.field('phone', pa.string()),
            pa.field('address', pa.string()),
            pa.field('country', pa.string()),
            pa.field('region', pa.string()),
            pa.field('registration_date', pa.date32()),
            pa.field('credit_score', pa.int32()),
            pa.field('lifetime_value', pa.float64()),
            pa.field('is_premium', pa.bool_()),
            pa.field('last_login', pa.timestamp('us')),
            pa.field('created_at', pa.timestamp('us')),
            pa.field('updated_at', pa.timestamp('us')),
        ])
    },
    'products': {
        'size_ratio': 0.03,
        'partition_spec': ['category', 'created_date'],
        'sort_order': ['product_id'],
        'schema': pa.schema([
            pa.field('product_id', pa.int64()),
            pa.field('product_name', pa.string()),
            pa.field('category', pa.string()),
            pa.field('brand', pa.string()),
            pa.field('price', pa.decimal128(10, 2)),
            pa.field('cost', pa.decimal128(10, 2)),
            pa.field('weight', pa.float32()),
            pa.field('dimensions', pa.string()),
            pa.field('description', pa.string()),
            pa.field('supplier_id', pa.int64()),
            pa.field('created_date', pa.date32()),
            pa.field('is_active', pa.bool_()),
            pa.field('created_at', pa.timestamp('us')),
            pa.field('updated_at', pa.timestamp('us')),
        ])
    },
    'orders': {
        'size_ratio': 0.08,
        'partition_spec': ['country', 'order_date'],
        'sort_order': ['order_id', 'order_date'],
        'schema': pa.schema([
            pa.field('order_id', pa.int64()),
            pa.field('customer_id', pa.int64()),
            pa.field('order_date', pa.date32()),
            pa.field('ship_date', pa.date32()),
            pa.field('delivery_date', pa.date32()),
            pa.field('order_status', pa.string()),
            pa.field('payment_method', pa.string()),
            pa.field('total_amount', pa.decimal128(12, 2)),
            pa.field('tax_amount', pa.decimal128(10, 2)),
            pa.field('shipping_amount', pa.decimal128(8, 2)),
            pa.field('discount_amount', pa.decimal128(8, 2)),
            pa.field('country', pa.string()),
            pa.field('region', pa.string()),
            pa.field('created_at', pa.timestamp('us')),
            pa.field('updated_at', pa.timestamp('us')),
        ])
    },
    'lineitem': {
        'size_ratio': 0.60,
        'partition_spec': ['ship_date'],
        'sort_order': ['order_id', 'lineitem_id'],
        'schema': pa.schema([
            pa.field('lineitem_id', pa.int64()),
            pa.field('order_id', pa.int64()),
            pa.field('product_id', pa.int64()),
            pa.field('quantity', pa.int32()),
            pa.field('unit_price', pa.decimal128(10, 2)),
            pa.field('discount', pa.float32()),
            pa.field('tax', pa.float32()),
            pa.field('extended_price', pa.decimal128(12, 2)),
            pa.field('discount_amount', pa.decimal128(10, 2)),
            pa.field('tax_amount', pa.decimal128(10, 2)),
            pa.field('net_amount', pa.decimal128(12, 2)),
            pa.field('line_status', pa.string()),
            pa.field('ship_date', pa.date32()),
            pa.field('comment', pa.string()),
            pa.field('created_at', pa.timestamp('us')),
            pa.field('updated_at', pa.timestamp('us')),
        ])
    },
    'events': {
        'size_ratio': 0.20,
        'partition_spec': ['event_date', 'event_type'],
        'sort_order': ['event_timestamp', 'customer_id'],
        'schema': pa.schema([
            pa.field('event_id', pa.int64()),
            pa.field('customer_id', pa.int64()),
            pa.field('event_type', pa.string()),
            pa.field('event_timestamp', pa.timestamp('us')),
            pa.field('event_date', pa.date32()),
            pa.field('page_url', pa.string()),
            pa.field('user_agent', pa.string()),
            pa.field('ip_address', pa.string()),
            pa.field('country', pa.string()),
            pa.field('device_type', pa.string()),
            pa.field('session_id', pa.string()),
            pa.field('product_id', pa.int64()),
            pa.field('search_query', pa.string()),
            pa.field('created_at', pa.timestamp('us')),
        ])
    },
    'inventory': {
        'size_ratio': 0.02,
        'partition_spec': ['warehouse_location', 'last_updated_date'],
        'sort_order': ['product_id', 'supplier_id'],
        'schema': pa.schema([
            pa.field('inventory_id', pa.int64()),
            pa.field('product_id', pa.int64()),
            pa.field('supplier_id', pa.int64()),
            pa.field('warehouse_location', pa.string()),
            pa.field('quantity_on_hand', pa.int32()),
            pa.field('quantity_allocated', pa.int32()),
            pa.field('reorder_point', pa.int32()),
            pa.field('reorder_quantity', pa.int32()),
            pa.field('unit_cost', pa.decimal128(10, 2)),
            pa.field('last_updated', pa.timestamp('us')),
            pa.field('last_updated_date', pa.date32()),
            pa.field('created_at', pa.timestamp('us')),
        ])
    },
    'suppliers': {
        'size_ratio': 0.02,
        'partition_spec': ['country', 'established_year'],
        'sort_order': ['supplier_id'],
        'schema': pa.schema([
            pa.field('supplier_id', pa.int64()),
            pa.field('supplier_name', pa.string()),
            pa.field('contact_name', pa.string()),
            pa.field('email', pa.string()),
            pa.field('phone', pa.string()),
            pa.field('address', pa.string()),
            pa.field('country', pa.string()),
            pa.field('region', pa.string()),
            pa.field('rating', pa.float32()),
            pa.field('established_date', pa.date32()),
            pa.field('established_year', pa.int32()),
            pa.field('is_verified', pa.bool_()),
            pa.field('created_at', pa.timestamp('us')),
            pa.field('updated_at', pa.timestamp('us')),
        ])
    }
}

# Data generation constants
COUNTRIES = ['US', 'CA', 'GB', 'DE', 'FR', 'IT', 'ES', 'AU', 'JP', 'KR', 'CN', 'IN', 'BR', 'MX']
REGIONS = ['North America', 'Europe', 'Asia Pacific', 'South America', 'Middle East', 'Africa']
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Automotive', 'Beauty', 'Toys']
ORDER_STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash_on_delivery']
EVENT_TYPES = ['page_view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart', 'search', 'login', 'logout']


class IcebergTableGenerator:
    def __init__(self, catalog_type: str, catalog_config: Dict[str, Any], 
                 s3_bucket: str, s3_prefix: str, target_size_gb: int = 100,
                 num_workers: Optional[int] = None):
        """
        Initialize Iceberg table generator.
        
        Args:
            catalog_type: 'hive' or 'databricks'
            catalog_config: Configuration for the catalog
            s3_bucket: S3 bucket for data storage
            s3_prefix: S3 prefix for data
            target_size_gb: Total target size in GB
            num_workers: Number of parallel workers
        """
        self.catalog_type = catalog_type
        self.catalog_config = catalog_config
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.target_size_gb = target_size_gb
        self.num_workers = num_workers if num_workers is not None else mp.cpu_count()
        self.s3_client = boto3.client('s3')
        
        # Initialize catalog
        self.catalog = self._setup_catalog()
        
        # Pre-generate lookup data
        self.setup_lookup_data()
    
    def _setup_catalog(self):
        """Setup the appropriate catalog based on type"""
        if self.catalog_type == 'hive':
            return self._setup_hive_catalog()
        elif self.catalog_type == 'databricks':
            return self._setup_databricks_catalog()
        else:
            raise ValueError(f"Unsupported catalog type: {self.catalog_type}")
    
    def _setup_hive_catalog(self):
        """Setup Hive catalog for Trino"""
        catalog_config = {
            'type': 'hive',
            'uri': self.catalog_config.get('hive_metastore_uri', 'thrift://localhost:9083'),
            'warehouse': f's3://{self.s3_bucket}/{self.s3_prefix}/',
            's3.endpoint': self.catalog_config.get('s3_endpoint', None),
            's3.access-key-id': self.catalog_config.get('aws_access_key_id', None),
            's3.secret-access-key': self.catalog_config.get('aws_secret_access_key', None),
            's3.region': self.catalog_config.get('aws_region', 'us-east-1'),
        }
        
        # Remove None values
        catalog_config = {k: v for k, v in catalog_config.items() if v is not None}
        
        print(f"Setting up Hive catalog with config: {catalog_config}")
        return load_catalog('hive_catalog', **catalog_config)
    
    def _setup_databricks_catalog(self):
        """Setup Databricks Unity Catalog for Firebolt"""
        if not DATABRICKS_AVAILABLE:
            raise ImportError("Databricks SDK not available. Install with: pip install databricks-sdk")
        
        catalog_config = {
            'type': 'rest',
            'uri': self.catalog_config.get('catalog_uri'),
            'credential': self.catalog_config.get('credential'),
            'warehouse': f's3://{self.s3_bucket}/{self.s3_prefix}/',
        }
        
        print(f"Setting up Databricks Unity Catalog with config: {catalog_config}")
        return load_catalog('unity_catalog', **catalog_config)
    
    def setup_lookup_data(self):
        """Pre-generate lookup data for better performance"""
        print("Setting up lookup data...")
        
        # Generate IDs for referential integrity
        self.num_customers = 10_000_000
        self.customer_ids = list(range(1, self.num_customers + 1))
        
        self.num_products = 1_000_000
        self.product_ids = list(range(1, self.num_products + 1))
        
        self.num_suppliers = 100_000
        self.supplier_ids = list(range(1, self.num_suppliers + 1))
        
        self.num_orders = 100_000_000
        self.order_ids = list(range(1, self.num_orders + 1))
        
        print(f"Generated lookup data: {len(self.customer_ids)} customers, "
              f"{len(self.product_ids)} products, {len(self.supplier_ids)} suppliers")
    
    def create_database_if_not_exists(self, database_name: str):
        """Create database/schema if it doesn't exist"""
        try:
            # Try to create the namespace/database
            if hasattr(self.catalog, 'create_namespace'):
                self.catalog.create_namespace(database_name)
                print(f"Created namespace: {database_name}")
        except Exception as e:
            print(f"Namespace {database_name} might already exist or creation failed: {e}")
    
    def create_iceberg_table(self, table_name: str, database_name: str = 'benchmark') -> Table:
        """Create an Iceberg table with optimized schema and partitioning"""
        table_config = ICEBERG_TABLES[table_name]
        arrow_schema = table_config['schema']
        
        # Convert PyArrow schema to Iceberg schema
        iceberg_schema = schema.Schema(*[
            schema.NestedField(
                field_id=i+1,
                name=field.name,
                field_type=self._convert_arrow_type_to_iceberg(field.type),
                required=not field.nullable
            )
            for i, field in enumerate(arrow_schema)
        ])
        
        # Create partition spec
        partition_fields = []
        for i, partition_col in enumerate(table_config['partition_spec']):
            if partition_col.endswith('_date'):
                # Daily partitioning for date columns
                partition_fields.append(
                    PartitionField(
                        source_id=next(f.field_id for f in iceberg_schema.fields if f.name == partition_col),
                        field_id=1000 + i,
                        transform=days(),
                        name=f"{partition_col}_day"
                    )
                )
            elif partition_col in ['country', 'region', 'category', 'event_type', 'warehouse_location']:
                # Identity partitioning for categorical columns
                partition_fields.append(
                    PartitionField(
                        source_id=next(f.field_id for f in iceberg_schema.fields if f.name == partition_col),
                        field_id=1000 + i,
                        transform=None,  # Identity transform
                        name=partition_col
                    )
                )
            elif partition_col == 'customer_id':
                # Bucket partitioning for high cardinality columns
                partition_fields.append(
                    PartitionField(
                        source_id=next(f.field_id for f in iceberg_schema.fields if f.name == partition_col),
                        field_id=1000 + i,
                        transform=bucket(100),
                        name=f"{partition_col}_bucket"
                    )
                )
        
        partition_spec = PartitionSpec(*partition_fields)
        
        # Create table identifier
        table_identifier = f"{database_name}.{table_name}"
        
        try:
            # Create the table
            table = self.catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                location=f's3://{self.s3_bucket}/{self.s3_prefix}/{database_name}/{table_name}/'
            )
            print(f"Created Iceberg table: {table_identifier}")
            return table
            
        except Exception as e:
            print(f"Error creating table {table_identifier}: {e}")
            # Try to load existing table
            try:
                table = self.catalog.load_table(table_identifier)
                print(f"Loaded existing table: {table_identifier}")
                return table
            except Exception as load_error:
                print(f"Error loading existing table: {load_error}")
                raise e
    
    def _convert_arrow_type_to_iceberg(self, arrow_type):
        """Convert PyArrow type to Iceberg type"""
        if pa.types.is_int64(arrow_type):
            return types.LongType()
        elif pa.types.is_int32(arrow_type):
            return types.IntegerType()
        elif pa.types.is_string(arrow_type):
            return types.StringType()
        elif pa.types.is_boolean(arrow_type):
            return types.BooleanType()
        elif pa.types.is_float32(arrow_type):
            return types.FloatType()
        elif pa.types.is_float64(arrow_type):
            return types.DoubleType()
        elif pa.types.is_date32(arrow_type):
            return types.DateType()
        elif pa.types.is_timestamp(arrow_type):
            return types.TimestampType()
        elif pa.types.is_decimal(arrow_type):
            return types.DecimalType(arrow_type.precision, arrow_type.scale)
        else:
            return types.StringType()  # Default fallback
    
    def generate_fake_data(self, data_type: str, size: int) -> List[Any]:
        """Generate fake data based on type"""
        if data_type == 'name':
            return [f"Customer_{random.randint(10000, 99999)}" for _ in range(size)]
        elif data_type == 'email':
            return [f"user{random.randint(10000, 99999)}@example.com" for _ in range(size)]
        elif data_type == 'phone':
            return [f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}" for _ in range(size)]
        elif data_type == 'address':
            return [f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'First', 'Second'])} St, City {random.randint(1, 1000)}" for _ in range(size)]
        elif data_type == 'description':
            return [f"Product description for item {random.randint(10000, 99999)}" for _ in range(size)]
        elif data_type == 'comment':
            return [f"Comment {random.randint(100000, 999999)}" for _ in range(size)]
        elif data_type == 'brand':
            return [f"Brand_{random.choice(['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon'])}{random.randint(1, 1000)}" for _ in range(size)]
        elif data_type == 'supplier_name':
            return [f"Supplier_{random.choice(['Corp', 'Inc', 'LLC', 'Ltd'])}{random.randint(1, 10000)}" for _ in range(size)]
        elif data_type == 'product_name':
            return [f"Product_{random.choice(['Pro', 'Max', 'Ultra', 'Plus', 'Elite'])}{random.randint(1, 100000)}" for _ in range(size)]
        else:
            return [f"data_{i}" for i in range(size)]
    
    def generate_table_data(self, table_name: str, chunk_id: int, chunk_size: int) -> pd.DataFrame:
        """Generate data for a specific table"""
        current_time = datetime.now()
        
        if table_name == 'customers':
            return self._generate_customers_data(chunk_id, chunk_size, current_time)
        elif table_name == 'products':
            return self._generate_products_data(chunk_id, chunk_size, current_time)
        elif table_name == 'suppliers':
            return self._generate_suppliers_data(chunk_id, chunk_size, current_time)
        elif table_name == 'orders':
            return self._generate_orders_data(chunk_id, chunk_size, current_time)
        elif table_name == 'lineitem':
            return self._generate_lineitem_data(chunk_id, chunk_size, current_time)
        elif table_name == 'inventory':
            return self._generate_inventory_data(chunk_id, chunk_size, current_time)
        elif table_name == 'events':
            return self._generate_events_data(chunk_id, chunk_size, current_time)
        else:
            raise ValueError(f"Unknown table: {table_name}")
    
    def _generate_customers_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate customer data chunk"""
        start_id = chunk_id * chunk_size + 1
        actual_size = min(chunk_size, self.num_customers - (chunk_id * chunk_size))
        if actual_size <= 0:
            return pd.DataFrame()
        
        base_date = datetime(2020, 1, 1)
        
        return pd.DataFrame({
            'customer_id': list(range(start_id, start_id + actual_size)),
            'customer_name': self.generate_fake_data('name', actual_size),
            'email': self.generate_fake_data('email', actual_size),
            'phone': self.generate_fake_data('phone', actual_size),
            'address': self.generate_fake_data('address', actual_size),
            'country': np.random.choice(COUNTRIES, actual_size),
            'region': np.random.choice(REGIONS, actual_size),
            'registration_date': pd.date_range(start=base_date, periods=actual_size, freq='1H').date,
            'credit_score': np.random.normal(650, 100, actual_size).astype(int),
            'lifetime_value': np.random.exponential(500, actual_size).round(2),
            'is_premium': np.random.choice([True, False], actual_size, p=[0.2, 0.8]),
            'last_login': pd.date_range(start='2024-01-01', periods=actual_size, freq='1H'),
            'created_at': [current_time] * actual_size,
            'updated_at': [current_time] * actual_size,
        })
    
    def _generate_products_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate product data chunk"""
        start_id = chunk_id * chunk_size + 1
        actual_size = min(chunk_size, self.num_products - (chunk_id * chunk_size))
        if actual_size <= 0:
            return pd.DataFrame()
        
        base_date = datetime(2019, 1, 1)
        
        return pd.DataFrame({
            'product_id': list(range(start_id, start_id + actual_size)),
            'product_name': self.generate_fake_data('product_name', actual_size),
            'category': np.random.choice(PRODUCT_CATEGORIES, actual_size),
            'brand': self.generate_fake_data('brand', actual_size),
            'price': np.random.exponential(50, actual_size).round(2),
            'cost': (np.random.exponential(50, actual_size) * np.random.uniform(0.3, 0.8, actual_size)).round(2),
            'weight': np.random.exponential(2, actual_size).round(2),
            'dimensions': [f"{random.randint(1, 50)}x{random.randint(1, 50)}x{random.randint(1, 50)}" for _ in range(actual_size)],
            'description': self.generate_fake_data('description', actual_size),
            'supplier_id': np.random.choice(self.supplier_ids, actual_size),
            'created_date': pd.date_range(start=base_date, periods=actual_size, freq='1H').date,
            'is_active': np.random.choice([True, False], actual_size, p=[0.8, 0.2]),
            'created_at': [current_time] * actual_size,
            'updated_at': [current_time] * actual_size,
        })
    
    def _generate_suppliers_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate supplier data chunk"""
        start_id = chunk_id * chunk_size + 1
        actual_size = min(chunk_size, self.num_suppliers - (chunk_id * chunk_size))
        if actual_size <= 0:
            return pd.DataFrame()
        
        established_dates = pd.date_range(start='2000-01-01', end='2020-01-01', periods=actual_size).date
        
        return pd.DataFrame({
            'supplier_id': list(range(start_id, start_id + actual_size)),
            'supplier_name': self.generate_fake_data('supplier_name', actual_size),
            'contact_name': self.generate_fake_data('name', actual_size),
            'email': self.generate_fake_data('email', actual_size),
            'phone': self.generate_fake_data('phone', actual_size),
            'address': self.generate_fake_data('address', actual_size),
            'country': np.random.choice(COUNTRIES, actual_size),
            'region': np.random.choice(REGIONS, actual_size),
            'rating': np.random.uniform(1, 5, actual_size).round(1),
            'established_date': established_dates,
            'established_year': [d.year for d in established_dates],
            'is_verified': np.random.choice([True, False], actual_size, p=[0.7, 0.3]),
            'created_at': [current_time] * actual_size,
            'updated_at': [current_time] * actual_size,
        })
    
    def _generate_orders_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate order data chunk"""
        actual_size = chunk_size
        start_id = chunk_id * chunk_size + 1
        
        order_dates = pd.date_range(start='2022-01-01', end='2024-01-01', periods=actual_size).date
        
        return pd.DataFrame({
            'order_id': list(range(start_id, start_id + actual_size)),
            'customer_id': np.random.choice(self.customer_ids, actual_size),
            'order_date': order_dates,
            'ship_date': [d + timedelta(days=random.randint(1, 3)) for d in order_dates],
            'delivery_date': [d + timedelta(days=random.randint(3, 10)) for d in order_dates],
            'order_status': np.random.choice(ORDER_STATUSES, actual_size),
            'payment_method': np.random.choice(PAYMENT_METHODS, actual_size),
            'total_amount': np.random.exponential(100, actual_size).round(2),
            'tax_amount': (np.random.exponential(100, actual_size) * 0.1).round(2),
            'shipping_amount': np.random.exponential(10, actual_size).round(2),
            'discount_amount': np.random.exponential(5, actual_size).round(2),
            'country': np.random.choice(COUNTRIES, actual_size),
            'region': np.random.choice(REGIONS, actual_size),
            'created_at': [current_time] * actual_size,
            'updated_at': [current_time] * actual_size,
        })
    
    def _generate_lineitem_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate lineitem data chunk"""
        actual_size = chunk_size
        start_id = chunk_id * chunk_size + 1
        
        ship_dates = pd.date_range(start='2022-01-01', end='2024-01-01', periods=actual_size).date
        quantities = np.random.randint(1, 20, actual_size)
        unit_prices = np.random.exponential(25, actual_size).round(2)
        
        return pd.DataFrame({
            'lineitem_id': list(range(start_id, start_id + actual_size)),
            'order_id': np.random.choice(self.order_ids, actual_size),
            'product_id': np.random.choice(self.product_ids, actual_size),
            'quantity': quantities,
            'unit_price': unit_prices,
            'discount': np.random.uniform(0, 0.3, actual_size).round(3),
            'tax': np.random.uniform(0.05, 0.15, actual_size).round(3),
            'extended_price': (quantities * unit_prices).round(2),
            'discount_amount': (quantities * unit_prices * np.random.uniform(0, 0.3, actual_size)).round(2),
            'tax_amount': (quantities * unit_prices * np.random.uniform(0.05, 0.15, actual_size)).round(2),
            'net_amount': (quantities * unit_prices * np.random.uniform(0.8, 1.2, actual_size)).round(2),
            'line_status': np.random.choice(['pending', 'confirmed', 'shipped', 'delivered'], actual_size),
            'ship_date': ship_dates,
            'comment': self.generate_fake_data('comment', actual_size),
            'created_at': [current_time] * actual_size,
            'updated_at': [current_time] * actual_size,
        })
    
    def _generate_inventory_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate inventory data chunk"""
        actual_size = chunk_size
        start_id = chunk_id * chunk_size + 1
        
        last_updated = pd.date_range(start='2024-01-01', periods=actual_size, freq='1H')
        
        return pd.DataFrame({
            'inventory_id': list(range(start_id, start_id + actual_size)),
            'product_id': np.random.choice(self.product_ids, actual_size),
            'supplier_id': np.random.choice(self.supplier_ids, actual_size),
            'warehouse_location': np.random.choice(COUNTRIES, actual_size),
            'quantity_on_hand': np.random.randint(0, 1000, actual_size),
            'quantity_allocated': np.random.randint(0, 100, actual_size),
            'reorder_point': np.random.randint(10, 100, actual_size),
            'reorder_quantity': np.random.randint(50, 500, actual_size),
            'unit_cost': np.random.exponential(20, actual_size).round(2),
            'last_updated': last_updated,
            'last_updated_date': last_updated.date,
            'created_at': [current_time] * actual_size,
        })
    
    def _generate_events_data(self, chunk_id: int, chunk_size: int, current_time: datetime) -> pd.DataFrame:
        """Generate event data chunk"""
        actual_size = chunk_size
        start_id = chunk_id * chunk_size + 1
        
        event_timestamps = pd.date_range(start='2024-01-01', periods=actual_size, freq='1S')
        
        return pd.DataFrame({
            'event_id': list(range(start_id, start_id + actual_size)),
            'customer_id': np.random.choice(self.customer_ids, actual_size),
            'event_type': np.random.choice(EVENT_TYPES, actual_size),
            'event_timestamp': event_timestamps,
            'event_date': event_timestamps.date,
            'page_url': [f"/page/{random.randint(1, 1000)}" for _ in range(actual_size)],
            'user_agent': [f"Mozilla/5.0 Browser {random.randint(1, 100)}" for _ in range(actual_size)],
            'ip_address': [f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}" for _ in range(actual_size)],
            'country': np.random.choice(COUNTRIES, actual_size),
            'device_type': np.random.choice(['desktop', 'mobile', 'tablet'], actual_size),
            'session_id': [f"session_{uuid.uuid4().hex[:8]}" for _ in range(actual_size)],
            'product_id': np.random.choice(self.product_ids + [None], actual_size),
            'search_query': [f"search term {random.randint(1, 10000)}" if random.random() > 0.7 else None for _ in range(actual_size)],
            'created_at': [current_time] * actual_size,
        })
    
    def write_to_iceberg_table(self, table: Table, df: pd.DataFrame):
        """Write DataFrame to Iceberg table"""
        try:
            # Convert DataFrame to PyArrow Table
            arrow_table = pa.Table.from_pandas(df)
            
            # Append data to Iceberg table
            table.append(arrow_table)
            return True
        except Exception as e:
            print(f"Error writing to Iceberg table: {e}")
            return False
    
    def process_table_chunk(self, table_name: str, table: Table, chunk_id: int, chunk_size: int) -> bool:
        """Process a single chunk for a table"""
        try:
            # Generate data
            df = self.generate_table_data(table_name, chunk_id, chunk_size)
            
            if df.empty:
                return True
            
            # Write to Iceberg table
            success = self.write_to_iceberg_table(table, df)
            
            if success:
                print(f"Successfully wrote chunk {chunk_id} for {table_name} ({len(df)} rows)")
            
            return success
            
        except Exception as e:
            print(f"Error processing chunk {chunk_id} for {table_name}: {e}")
            return False
    
    def generate_iceberg_table(self, table_name: str, database_name: str = 'benchmark') -> bool:
        """Generate complete Iceberg table"""
        print(f"\nGenerating Iceberg table: {table_name}")
        
        # Calculate target size for this table
        table_config = ICEBERG_TABLES[table_name]
        target_size_gb = self.target_size_gb * table_config['size_ratio']
        
        # Estimate number of chunks needed
        estimated_rows_per_gb = 1_000_000
        target_rows = int(target_size_gb * estimated_rows_per_gb)
        num_chunks = max(1, (target_rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
        
        print(f"Target: {target_size_gb:.1f} GB, ~{target_rows:,} rows, {num_chunks} chunks")
        
        # Create table
        table = self.create_iceberg_table(table_name, database_name)
        
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            
            for chunk_id in range(num_chunks):
                future = executor.submit(
                    self.process_table_chunk, 
                    table_name, table, chunk_id, CHUNK_SIZE
                )
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
                        print(f"Error in chunk processing: {e}")
                        pbar.update(1)
        
        success_rate = completed / num_chunks if num_chunks > 0 else 0
        print(f"Completed {table_name}: {completed}/{num_chunks} chunks successful ({success_rate:.1%})")
        
        # Print table info
        try:
            table.refresh()
            print(f"Table location: {table.location()}")
            print(f"Table schema: {table.schema()}")
            if hasattr(table, 'scan'):
                row_count = len(list(table.scan().to_arrow()))
                print(f"Total rows: {row_count:,}")
        except Exception as e:
            print(f"Could not get table info: {e}")
        
        return success_rate > 0.8  # Consider successful if 80%+ chunks completed
    
    def generate_all_tables(self, database_name: str = 'benchmark', 
                          tables: Optional[List[str]] = None) -> bool:
        """Generate all Iceberg tables"""
        tables_to_generate = tables if tables else list(ICEBERG_TABLES.keys())
        
        print(f"Starting Iceberg table generation")
        print(f"Catalog type: {self.catalog_type}")
        print(f"Target size: {self.target_size_gb} GB")
        print(f"Database: {database_name}")
        print(f"Tables: {tables_to_generate}")
        print(f"Workers: {self.num_workers}")
        
        # Create database
        self.create_database_if_not_exists(database_name)
        
        start_time = time.time()
        successful_tables = 0
        
        for table_name in tables_to_generate:
            if table_name not in ICEBERG_TABLES:
                print(f"Unknown table: {table_name}")
                continue
            
            success = self.generate_iceberg_table(table_name, database_name)
            if success:
                successful_tables += 1
            else:
                print(f"Failed to generate table: {table_name}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\nIceberg table generation completed in {duration:.2f} seconds ({duration/3600:.2f} hours)")
        print(f"Successfully generated {successful_tables}/{len(tables_to_generate)} tables")
        
        return successful_tables == len(tables_to_generate)


def main():
    parser = argparse.ArgumentParser(description='Generate Apache Iceberg tables with benchmark data')
    
    # Catalog configuration
    parser.add_argument('--catalog-type', choices=['hive', 'databricks'], required=True,
                       help='Type of catalog to use')
    parser.add_argument('--hive-metastore-uri', 
                       help='Hive metastore URI (for hive catalog)')
    parser.add_argument('--databricks-catalog-uri',
                       help='Databricks catalog URI (for databricks catalog)')
    parser.add_argument('--databricks-token',
                       help='Databricks access token (for databricks catalog)')
    
    # Storage configuration
    parser.add_argument('--s3-bucket', required=True,
                       help='S3 bucket for Iceberg data storage')
    parser.add_argument('--s3-prefix', default='iceberg-tables',
                       help='S3 prefix for Iceberg tables')
    parser.add_argument('--aws-region', default='us-east-1',
                       help='AWS region')
    
    # Generation parameters
    parser.add_argument('--database', default='benchmark',
                       help='Database/schema name')
    parser.add_argument('--size-gb', type=int, default=100,
                       help='Target total size in GB')
    parser.add_argument('--workers', type=int,
                       help='Number of worker threads')
    parser.add_argument('--tables', nargs='+',
                       help='Specific tables to generate')
    
    args = parser.parse_args()
    
    # Validate S3 access
    try:
        s3_client = boto3.client('s3', region_name=args.aws_region)
        s3_client.head_bucket(Bucket=args.s3_bucket)
        print(f"S3 bucket '{args.s3_bucket}' is accessible")
    except Exception as e:
        print(f"Error accessing S3 bucket '{args.s3_bucket}': {e}")
        return 1
    
    # Prepare catalog configuration
    catalog_config = {
        'aws_region': args.aws_region,
    }
    
    if args.catalog_type == 'hive':
        if not args.hive_metastore_uri:
            print("--hive-metastore-uri is required for hive catalog")
            return 1
        catalog_config['hive_metastore_uri'] = args.hive_metastore_uri
    
    elif args.catalog_type == 'databricks':
        if not args.databricks_catalog_uri:
            print("--databricks-catalog-uri is required for databricks catalog")
            return 1
        catalog_config['catalog_uri'] = args.databricks_catalog_uri
        if args.databricks_token:
            catalog_config['credential'] = args.databricks_token
    
    # Create generator
    try:
        generator = IcebergTableGenerator(
            catalog_type=args.catalog_type,
            catalog_config=catalog_config,
            s3_bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix,
            target_size_gb=args.size_gb,
            num_workers=args.workers
        )
        
        # Generate tables
        success = generator.generate_all_tables(
            database_name=args.database,
            tables=args.tables
        )
        
        if success:
            print(f"\nAll Iceberg tables generated successfully!")
            print(f"Data available at: s3://{args.s3_bucket}/{args.s3_prefix}/")
            print(f"Catalog type: {args.catalog_type}")
            return 0
        else:
            print("\nSome tables failed to generate!")
            return 1
            
    except Exception as e:
        print(f"Error during table generation: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
