# Benchmark Data Generation Workflow

This project provides a complete solution for generating large-scale benchmark data in Parquet format and uploading it to S3 for database performance testing.

## 📁 Project Structure

```
trvsfb/
├── generate_benchmark_data.py    # Main data generation script
├── setup_and_run.py              # Setup and utility script
├── inspect_parquet_s3.py         # S3 Parquet file inspector
├── requirements_data_gen.txt      # Python dependencies
├── README.md                     # Main project documentation
└── BENCHMARK_README.md           # This file
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
python setup_and_run.py --install
```

### 2. Configure AWS Credentials
```bash
aws configure
# OR set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### 3. Create S3 Bucket
```bash
aws s3 mb s3://your-benchmark-data-bucket
```

### 4. Generate Data
```bash
# Generate 1TB (all tables)
python generate_benchmark_data.py --bucket your-benchmark-data-bucket

# Generate specific table only
python generate_benchmark_data.py --bucket your-benchmark-data-bucket --table customers

# Generate 100GB with 8 workers
python generate_benchmark_data.py --bucket your-benchmark-data-bucket --size-gb 100 --workers 8
```

## 📊 Data Structure

The script generates 7 interconnected tables:

| Table | Size | Description | Rows (1TB) |
|-------|------|-------------|------------|
| `lineitem` | 600GB (60%) | Order line items (fact table) | ~600M |
| `events` | 200GB (20%) | User activity events | ~200M |
| `orders` | 80GB (8%) | Order transactions | ~80M |
| `customers` | 50GB (5%) | Customer information | ~50M |
| `products` | 30GB (3%) | Product catalog | ~30M |
| `suppliers` | 20GB (2%) | Supplier data | ~20M |
| `inventory` | 20GB (2%) | Inventory levels | ~20M |

## 🔧 Scripts Overview

### 1. `generate_benchmark_data.py`
**Main data generation script with features:**
- Multi-threaded parallel processing
- Chunked generation (1M rows per chunk)
- Direct S3 upload with cleanup
- Parquet format with Snappy compression
- Resume capability for individual tables
- Progress tracking with tqdm

**Usage:**
```bash
python generate_benchmark_data.py --bucket BUCKET_NAME [OPTIONS]

Options:
  --bucket BUCKET_NAME    (required) S3 bucket name
  --prefix PREFIX         (optional) S3 prefix (default: benchmark-data)
  --workers N             (optional) Number of workers (default: CPU count)
  --size-gb N             (optional) Target size in GB (default: 1000)
  --table TABLE           (optional) Specific table to generate (can be repeated)
```

**Examples:**
```bash
# Generate all tables (1TB)
python generate_benchmark_data.py --bucket my-benchmark-bucket

# Generate only customers table
python generate_benchmark_data.py --bucket my-benchmark-bucket --table customers

# Generate 100GB with 8 workers
python generate_benchmark_data.py --bucket my-benchmark-bucket --size-gb 100 --workers 8

# Generate multiple specific tables
python generate_benchmark_data.py --bucket my-benchmark-bucket --table customers --table products
```

### 2. `setup_and_run.py`
**Setup and utility script with features:**
- Dependency installation
- AWS credentials validation
- Usage instructions
- Integrated execution

**Usage:**
```bash
# Install dependencies
python setup_and_run.py --install

# Check AWS credentials
python setup_and_run.py --check-aws

# Show usage instructions
python setup_and_run.py

# Run data generation
python setup_and_run.py --run --bucket my-bucket --size-gb 100
```

### 3. `inspect_parquet_s3.py`
**S3 Parquet file inspector for:**
- Reading Parquet file schemas from S3
- Viewing sample data
- Getting column names for Firebolt DDL

**Usage:**
```bash
# Edit the script to set your bucket and prefix
python inspect_parquet_s3.py
```

## 🗄️ Database Integration

### Firebolt External Tables
Create external tables to read from S3:

```sql
-- Create S3 location
CREATE LOCATION IF NOT EXISTS tpch_customers
WITH
  SOURCE = AMAZON_S3
  CREDENTIALS = (
    AWS_ACCESS_KEY_ID = 'your_key'
    AWS_SECRET_ACCESS_KEY = 'your_secret'
  )
  URL = 's3://your-bucket/benchmark-data/customers/'
  DESCRIPTION = 'S3 location for customers data';

-- Create external table
CREATE EXTERNAL TABLE customers_ext (
    customer_id      BIGINT,
    customer_name    VARCHAR,
    email            VARCHAR,
    phone            VARCHAR,
    address          VARCHAR,
    country          VARCHAR,
    region           VARCHAR,
    registration_date TIMESTAMP,
    credit_score     INT,
    lifetime_value   DOUBLE,
    is_premium       BOOLEAN,
    last_login       TIMESTAMP
)
LOCATION = tpch_customers
OBJECT_PATTERN = '*.parquet'
TYPE = (PARQUET);
```

### Sample Complex Queries
```sql
-- Customer lifetime value analysis
SELECT 
    country, 
    region,
    AVG(lifetime_value) as avg_ltv,
    COUNT(*) as customer_count
FROM customers 
GROUP BY country, region 
ORDER BY avg_ltv DESC;

-- Top selling products with customer insights
SELECT 
    p.product_name,
    p.category,
    SUM(l.quantity) as total_sold,
    SUM(l.net_amount) as total_revenue,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM products p
JOIN lineitem l ON p.product_id = l.product_id
JOIN orders o ON l.order_id = o.order_id
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 20;
```
