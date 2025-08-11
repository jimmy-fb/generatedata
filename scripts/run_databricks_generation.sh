#!/bin/bash
# Script to generate Iceberg tables using Databricks Unity Catalog for Firebolt

set -e

# Default values
BUCKET=""
CATALOG_URI=""
SIZE_GB=100
WORKERS=8
DATABASE="benchmark"
TABLES=""

# Help function
show_help() {
    cat << EOF
Usage: $0 --bucket BUCKET --catalog-uri URI [OPTIONS]

Generate Iceberg tables using Databricks Unity Catalog for Firebolt.

Required:
    --bucket BUCKET         S3 bucket for Iceberg data storage
    --catalog-uri URI       Databricks Unity Catalog URI

Environment Variables:
    DATABRICKS_TOKEN        Databricks access token (required)

Options:
    --size-gb SIZE          Target size in GB (default: 100)
    --workers NUM           Number of worker threads (default: 8)
    --database NAME         Database name (default: benchmark)
    --tables TABLE1,TABLE2  Specific tables to generate (default: all)
    --aws-region REGION     AWS region (default: us-east-1)
    --help                  Show this help message

Examples:
    # Set token and generate all tables
    export DATABRICKS_TOKEN="your_token_here"
    $0 --bucket my-iceberg-bucket --catalog-uri https://workspace.cloud.databricks.com/api/2.0/unity-catalog

    # Generate specific tables with custom size
    $0 --bucket my-iceberg-bucket --catalog-uri https://workspace.cloud.databricks.com/api/2.0/unity-catalog --size-gb 50 --tables customers,orders
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --catalog-uri)
            CATALOG_URI="$2"
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

if [ -z "$CATALOG_URI" ]; then
    echo "Error: --catalog-uri is required"
    show_help
    exit 1
fi

if [ -z "$DATABRICKS_TOKEN" ]; then
    echo "Error: DATABRICKS_TOKEN environment variable is required"
    echo "Set it with: export DATABRICKS_TOKEN='your_token'"
    exit 1
fi

# Build command
CMD="python3 generate_iceberg_tables.py"
CMD="$CMD --catalog-type databricks"
CMD="$CMD --databricks-catalog-uri '$CATALOG_URI'"
CMD="$CMD --databricks-token '$DATABRICKS_TOKEN'"
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

echo "Starting Iceberg table generation with Databricks Unity Catalog..."
echo "Command: $CMD"
echo ""

# Execute the command
eval $CMD

echo ""
echo "Generation complete! Your Iceberg tables are ready for Firebolt queries."
echo ""
echo "Configure Firebolt to use this Unity Catalog as external catalog:"
echo "  Catalog type: Unity Catalog"
echo "  Database: $DATABASE"
echo "  Tables: Available at s3://$BUCKET/iceberg-tables/"
