#!/bin/bash
# Script to generate Iceberg tables using Hive catalog for Trino

set -e

# Default values
BUCKET=""
METASTORE_URI="thrift://localhost:9083"
SIZE_GB=100
WORKERS=8
DATABASE="benchmark"
TABLES=""

# Help function
show_help() {
    cat << EOF
Usage: $0 --bucket BUCKET [OPTIONS]

Generate Iceberg tables using Hive catalog for Trino.

Required:
    --bucket BUCKET         S3 bucket for Iceberg data storage

Options:
    --metastore-uri URI     Hive metastore URI (default: thrift://localhost:9083)
    --size-gb SIZE          Target size in GB (default: 100)
    --workers NUM           Number of worker threads (default: 8)
    --database NAME         Database name (default: benchmark)
    --tables TABLE1,TABLE2  Specific tables to generate (default: all)
    --aws-region REGION     AWS region (default: us-east-1)
    --help                  Show this help message

Examples:
    # Generate all tables (100GB total)
    $0 --bucket my-iceberg-bucket

    # Generate specific tables with custom size
    $0 --bucket my-iceberg-bucket --size-gb 50 --tables customers,orders

    # Use custom metastore
    $0 --bucket my-iceberg-bucket --metastore-uri thrift://hive-metastore:9083
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --metastore-uri)
            METASTORE_URI="$2"
            shift 2
            ;;
        --size-gb)
            SIZE_GB="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --tables)
            TABLES="$2"
            shift 2
            ;;
        --aws-region)
            AWS_REGION="$2"
            shift 2
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$BUCKET" ]; then
    echo "Error: --bucket is required"
    show_help
    exit 1
fi

# Build command
CMD="python3 generate_iceberg_tables.py"
CMD="$CMD --catalog-type hive"
CMD="$CMD --hive-metastore-uri $METASTORE_URI"
CMD="$CMD --s3-bucket $BUCKET"
CMD="$CMD --size-gb $SIZE_GB"
CMD="$CMD --workers $WORKERS"
CMD="$CMD --database $DATABASE"

if [ -n "$AWS_REGION" ]; then
    CMD="$CMD --aws-region $AWS_REGION"
fi

if [ -n "$TABLES" ]; then
    # Convert comma-separated list to space-separated
    TABLES_ARRAY=(${TABLES//,/ })
    CMD="$CMD --tables ${TABLES_ARRAY[@]}"
fi

echo "Starting Iceberg table generation with Hive catalog..."
echo "Command: $CMD"
echo ""

# Execute the command
eval $CMD

echo ""
echo "Generation complete! Your Iceberg tables are ready for Trino queries."
echo ""
echo "Example Trino connection:"
echo "  Catalog: iceberg"
echo "  Schema: $DATABASE"
echo "  Tables: Available at s3://$BUCKET/iceberg-tables/"
