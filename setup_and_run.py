#!/usr/bin/env python3
"""
Setup and run script for the benchmark data generator.
This script installs dependencies and provides usage instructions.
"""

import subprocess
import sys
import os
import argparse

def install_dependencies():
    """Install required dependencies"""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements_data_gen.txt"])
        print("✅ Dependencies installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing dependencies: {e}")
        return False

def check_aws_credentials():
    """Check if AWS credentials are configured"""
    try:
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials:
            print("✅ AWS credentials found")
            return True
        else:
            print("❌ AWS credentials not found")
            return False
    except Exception as e:
        print(f"❌ Error checking AWS credentials: {e}")
        return False

def print_usage():
    """Print usage instructions"""
    print("\n" + "="*80)
    print("🚀 BENCHMARK DATA GENERATOR - USAGE INSTRUCTIONS")
    print("="*80)
    print("\n1. INSTALL DEPENDENCIES:")
    print("   python setup_and_run.py --install")
    
    print("\n2. CONFIGURE AWS CREDENTIALS (if not already done):")
    print("   aws configure")
    print("   # OR set environment variables:")
    print("   export AWS_ACCESS_KEY_ID=your_access_key")
    print("   export AWS_SECRET_ACCESS_KEY=your_secret_key")
    print("   export AWS_DEFAULT_REGION=us-east-1")
    
    print("\n3. CREATE S3 BUCKET:")
    print("   aws s3 mb s3://your-benchmark-data-bucket")
    
    print("\n4. GENERATE DATA:")
    print("   python generate_benchmark_data.py --bucket your-benchmark-data-bucket")
    
    print("\n📋 COMMAND OPTIONS:")
    print("   --bucket BUCKET_NAME    (required) S3 bucket name")
    print("   --prefix PREFIX         (optional) S3 prefix (default: benchmark-data)")
    print("   --workers N             (optional) Number of workers (default: CPU count)")
    print("   --size-gb N             (optional) Target size in GB (default: 1000)")
    
    print("\n📊 EXAMPLE COMMANDS:")
    print("   # Generate 1TB of data:")
    print("   python generate_benchmark_data.py --bucket my-benchmark-bucket")
    print("")
    print("   # Generate 100GB of data with 8 workers:")
    print("   python generate_benchmark_data.py --bucket my-benchmark-bucket --size-gb 100 --workers 8")
    print("")
    print("   # Generate data with custom prefix:")
    print("   python generate_benchmark_data.py --bucket my-benchmark-bucket --prefix test-data")
    
    print("\n🗂️  DATA STRUCTURE:")
    print("   The script generates 7 tables:")
    print("   • lineitem   (60% - 600GB) - Order line items (fact table)")
    print("   • events     (20% - 200GB) - User activity events")
    print("   • orders     (8%  - 80GB)  - Order transactions")
    print("   • customers  (5%  - 50GB)  - Customer information")
    print("   • products   (3%  - 30GB)  - Product catalog")
    print("   • suppliers  (2%  - 20GB)  - Supplier data")
    print("   • inventory  (2%  - 20GB)  - Inventory levels")
    
    print("\n📁 S3 STRUCTURE:")
    print("   s3://your-bucket/benchmark-data/")
    print("   ├── lineitem/")
    print("   │   ├── lineitem_chunk_000000.parquet")
    print("   │   ├── lineitem_chunk_000001.parquet")
    print("   │   └── ...")
    print("   ├── events/")
    print("   ├── orders/")
    print("   ├── customers/")
    print("   ├── products/")
    print("   ├── suppliers/")
    print("   ├── inventory/")
    print("   └── data_manifest.json")
    
    print("\n⚡ PERFORMANCE TIPS:")
    print("   • Use EC2 instances with high CPU and network performance")
    print("   • Ensure S3 bucket is in the same region as your compute")
    print("   • Monitor S3 request rates and costs")
    print("   • Consider using S3 Transfer Acceleration for cross-region uploads")
    
    print("\n🔧 TROUBLESHOOTING:")
    print("   • If you get permission errors, check your AWS credentials")
    print("   • If uploads are slow, check your internet connection")
    print("   • For large datasets, consider running on EC2 in same region as S3")
    print("   • Monitor /tmp directory space during generation")
    
    print("\n" + "="*80)

def main():
    parser = argparse.ArgumentParser(description='Setup and run benchmark data generator')
    parser.add_argument('--install', action='store_true', help='Install dependencies')
    parser.add_argument('--check-aws', action='store_true', help='Check AWS credentials')
    parser.add_argument('--run', action='store_true', help='Run the data generator')
    parser.add_argument('--bucket', help='S3 bucket name (required if --run is used)')
    parser.add_argument('--prefix', default='benchmark-data', help='S3 prefix')
    parser.add_argument('--workers', type=int, help='Number of workers')
    parser.add_argument('--size-gb', type=int, default=1000, help='Target size in GB')
    
    args = parser.parse_args()
    
    if args.install:
        if install_dependencies():
            print("\n✅ Setup completed successfully!")
        else:
            print("\n❌ Setup failed!")
            return 1
    
    elif args.check_aws:
        if check_aws_credentials():
            print("AWS credentials are properly configured")
        else:
            print("AWS credentials need to be configured")
            return 1
    
    elif args.run:
        if not args.bucket:
            print("Error: --bucket is required when using --run")
            return 1
        
        # Check dependencies
        try:
            import pandas, pyarrow, numpy, boto3, tqdm
        except ImportError as e:
            print(f"Missing dependency: {e}")
            print("Run with --install first to install dependencies")
            return 1
        
        # Check AWS credentials
        if not check_aws_credentials():
            print("AWS credentials are not configured")
            return 1
        
        # Run the data generator
        cmd = [sys.executable, 'generate_benchmark_data.py', '--bucket', args.bucket]
        if args.prefix:
            cmd.extend(['--prefix', args.prefix])
        if args.workers:
            cmd.extend(['--workers', str(args.workers)])
        if args.size_gb:
            cmd.extend(['--size-gb', str(args.size_gb)])
        
        print(f"Running: {' '.join(cmd)}")
        return subprocess.call(cmd)
    
    else:
        print_usage()
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 