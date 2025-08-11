# Apache Iceberg Table Generator

This script generates Apache Iceberg table format files with realistic benchmark data, supporting both Hive metadata store (for Trino) and Databricks Unity Catalog (for Firebolt).

## Features

- **Multiple Catalog Support**: Hive metastore (Trino) and Databricks Unity Catalog (Firebolt)
- **Optimized Iceberg Tables**: Proper partitioning, schema evolution support, and time travel capabilities
- **Realistic Data**: 7 interconnected tables with referential integrity
- **High Performance**: Parallel data generation and optimized Iceberg format
- **Flexible Configuration**: Command-line arguments, configuration files, and environment variables
- **Production Ready**: Error handling, progress monitoring, and comprehensive logging

## Table Schema

The generator creates 7 optimized Iceberg tables:

### 1. Customers (5% of data)
- **Partitioned by**: Country, Registration Date
- **Schema**: Customer demographics, contact info, lifetime value
- **Use cases**: Customer analytics, segmentation

### 2. Products (3% of data)  
- **Partitioned by**: Category, Created Date
- **Schema**: Product catalog with pricing, suppliers, descriptions
- **Use cases**: Product analytics, inventory management

### 3. Suppliers (2% of data)
- **Partitioned by**: Country, Established Year  
- **Schema**: Supplier information, ratings, verification status
- **Use cases**: Supply chain analytics

### 4. Orders (8% of data)
- **Partitioned by**: Country, Order Date
- **Schema**: Order transactions with payments, shipping
- **Use cases**: Sales analytics, order processing

### 5. Line Items (60% of data)
- **Partitioned by**: Ship Date
- **Schema**: Individual line items within orders
- **Use cases**: Detailed sales analysis, product performance

### 6. Inventory (2% of data)
- **Partitioned by**: Warehouse Location, Last Updated Date
- **Schema**: Real-time inventory levels, reorder points
- **Use cases**: Inventory optimization, stock management

### 7. Events (20% of data)
- **Partitioned by**: Event Date, Event Type
- **Schema**: User activity events, web analytics
- **Use cases**: User behavior analysis, funnel optimization

## Installation

### Prerequisites

1. **Python 3.8+** with pip
2. **AWS CLI** configured with appropriate permissions
3. **S3 bucket** for Iceberg data storage
4. **Hive Metastore** (for Trino) OR **Databricks workspace** (for Firebolt)

### Install Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- `pyiceberg>=0.6.0` - Apache Iceberg Python library
- `pyhive>=0.7.0` - Hive metastore connectivity
- `databricks-sdk>=0.18.0` - Databricks Unity Catalog support
- `pandas>=2.0.0`, `pyarrow>=10.0.0`, `numpy>=1.20.0` - Data processing
- `boto3>=1.26.0` - AWS S3 integration

## Usage

### Option 1: Command Line Interface

#### For Trino (Hive Catalog)

```bash
python generate_iceberg_tables.py \
    --catalog-type hive \
    --hive-metastore-uri thrift://localhost:9083 \
    --s3-bucket your-iceberg-bucket \
    --size-gb 100 \
    --database benchmark
```

#### For Firebolt (Databricks Unity Catalog)

```bash
export DATABRICKS_TOKEN="your_databricks_token"

python generate_iceberg_tables.py \
    --catalog-type databricks \
    --databricks-catalog-uri https://your-workspace.cloud.databricks.com/api/2.0/unity-catalog \
    --s3-bucket your-iceberg-bucket \
    --size-gb 100 \
    --database benchmark
```

### Option 2: Convenience Scripts

#### Hive/Trino Setup

```bash
./scripts/run_hive_generation.sh --bucket your-iceberg-bucket --size-gb 50
```

#### Databricks/Firebolt Setup

```bash
export DATABRICKS_TOKEN="your_token"
./scripts/run_databricks_generation.sh \
    --bucket your-iceberg-bucket \
    --catalog-uri https://workspace.cloud.databricks.com/api/2.0/unity-catalog
```

### Option 3: Python Examples

See `examples/` directory for complete Python examples:
- `hive_catalog_example.py` - Trino integration
- `databricks_catalog_example.py` - Firebolt integration

## Configuration

### Command Line Arguments

| Argument | Description | Required | Default |
|----------|-------------|----------|---------|
| `--catalog-type` | `hive` or `databricks` | Yes | - |
| `--s3-bucket` | S3 bucket for data storage | Yes | - |
| `--hive-metastore-uri` | Hive metastore URI | Yes (for hive) | - |
| `--databricks-catalog-uri` | Databricks catalog URI | Yes (for databricks) | - |
| `--size-gb` | Target total size in GB | No | 100 |
| `--database` | Database/schema name | No | benchmark |
| `--tables` | Specific tables to generate | No | all |
| `--workers` | Number of parallel workers | No | CPU count |

### Environment Variables

- `DATABRICKS_TOKEN` - Required for Databricks Unity Catalog
- `AWS_ACCESS_KEY_ID` - AWS credentials (if not using default profile)
- `AWS_SECRET_ACCESS_KEY` - AWS credentials (if not using default profile)

### Configuration Files

YAML configuration files are available in `config/`:
- `hive_catalog.yaml` - Hive/Trino configuration
- `databricks_catalog.yaml` - Databricks/Firebolt configuration

## Query Examples

### Trino Queries

```sql
-- Connect to catalog: iceberg, schema: benchmark

-- Basic counts
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;

-- Customer analysis
SELECT country, COUNT(*) as customer_count 
FROM customers 
GROUP BY country 
ORDER BY customer_count DESC;

-- Sales analysis with joins
SELECT 
    c.country,
    SUM(o.total_amount) as total_sales,
    COUNT(o.order_id) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= DATE '2024-01-01'
GROUP BY c.country
ORDER BY total_sales DESC;

-- Time travel (view data as of specific snapshot)
SELECT COUNT(*) FROM customers FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00';
```

### Firebolt Queries

```sql
-- Configure external catalog in Firebolt first

-- Basic counts  
SELECT COUNT(*) FROM unity_catalog.benchmark.customers;
SELECT COUNT(*) FROM unity_catalog.benchmark.orders;

-- Product performance
SELECT 
    p.category,
    SUM(l.net_amount) as revenue,
    COUNT(l.lineitem_id) as line_count
FROM unity_catalog.benchmark.lineitem l
JOIN unity_catalog.benchmark.products p ON l.product_id = p.product_id
WHERE l.ship_date >= '2024-01-01'
GROUP BY p.category
ORDER BY revenue DESC;
```

## Performance Optimization

### Partitioning Strategy

Tables are optimized with strategic partitioning:
- **Date partitioning**: Daily partitions for time-series data
- **Category partitioning**: Identity partitioning for low-cardinality columns  
- **Bucket partitioning**: Hash partitioning for high-cardinality columns

### Iceberg Features Used

- **Schema Evolution**: Add/remove columns without breaking existing queries
- **Time Travel**: Query historical versions of data
- **Partition Evolution**: Change partitioning without rewriting data
- **Hidden Partitioning**: Automatic partition pruning in queries
- **Copy-on-Write**: Optimized for read-heavy workloads

### Storage Optimization

- **Snappy Compression**: Balanced compression ratio and speed
- **Columnar Format**: Parquet for analytical queries
- **File Sizing**: Optimized file sizes for query performance
- **Metadata Caching**: Efficient metadata operations

## Troubleshooting

### Common Issues

1. **Hive Metastore Connection Failed**
   ```
   Solution: Verify metastore URI and network connectivity
   Check: telnet <metastore-host> 9083
   ```

2. **Databricks Authentication Failed**
   ```
   Solution: Verify DATABRICKS_TOKEN is set correctly
   Check: echo $DATABRICKS_TOKEN
   ```

3. **S3 Access Denied**
   ```
   Solution: Check AWS credentials and bucket permissions
   Test: aws s3 ls s3://your-bucket/
   ```

4. **Memory Issues with Large Data**
   ```
   Solution: Reduce --workers or --size-gb parameters
   Monitor: System memory usage during generation
   ```

### Debug Mode

Enable debug logging:
```bash
export PYTHONPATH=.
python -u generate_iceberg_tables.py --catalog-type hive ... 2>&1 | tee generation.log
```

### Validation Queries

After generation, validate data integrity:
```sql
-- Check row counts match expectations
SELECT 
    'customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders;

-- Verify referential integrity
SELECT COUNT(*) 
FROM orders o 
LEFT JOIN customers c ON o.customer_id = c.customer_id 
WHERE c.customer_id IS NULL;
-- Should return 0
```

## Architecture

### System Design

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Gen      │    │   Iceberg        │    │   Query Engine  │
│   (Python)      │───▶│   Tables         │───▶│   (Trino/       │
│                 │    │   (S3)           │    │    Firebolt)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Metadata       │
                       │   (Hive/         │
                       │    Databricks)   │
                       └──────────────────┘
```

### Data Flow

1. **Generation**: Parallel data generation with realistic patterns
2. **Partitioning**: Automatic partitioning based on table configuration  
3. **Storage**: Iceberg format with metadata tracking
4. **Cataloging**: Registration in Hive or Databricks catalog
5. **Querying**: Direct access via Trino or Firebolt

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
1. Check troubleshooting section above
2. Review example configurations in `config/` and `examples/`
3. Enable debug logging for detailed error information
4. Open an issue with complete error logs and configuration
