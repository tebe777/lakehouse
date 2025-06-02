#!/usr/bin/env python3
"""
MinIO Integration Example with Enhanced Validation

This example demonstrates:
1. Connecting to MinIO using boto3
2. Downloading files with filename validation
3. Processing data with enhanced validation
4. Complete ETL pipeline integration
"""

import sys
from pathlib import Path
from datetime import datetime
import yaml

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.common.connections.s3_connection import S3ConnectionManager
from src.ingestion.minio_extractor import MinIOExtractor
from src.common.utils.config import TableConfig
from src.main.monitoring_integration import MonitoredETLPipeline


def demo_minio_connection():
    """Demonstrate basic MinIO connection and operations"""
    print("=== MinIO Connection Demo ===\n")
    
    # MinIO configuration (from dev environment)
    minio_config = {
        'endpoint': 'http://localhost:9000',
        'access_key': 'minioadmin', 
        'secret_key': 'minioadmin',
        'region': 'us-east-1',
        'use_ssl': False,
        'verify_ssl': False
    }
    
    try:
        # Create S3 connection manager
        print("1. Creating MinIO connection...")
        s3_manager = S3ConnectionManager(minio_config)
        
        # Test connection
        print("2. Testing connection...")
        if s3_manager.test_connection():
            print("   ✅ Connection successful!")
            
            # List buckets
            print("\n3. Listing available buckets:")
            buckets = s3_manager.list_buckets()
            for bucket in buckets:
                print(f"   - {bucket}")
            
            if buckets:
                # List objects in first bucket
                first_bucket = buckets[0]
                print(f"\n4. Listing objects in bucket '{first_bucket}':")
                objects = s3_manager.list_objects(first_bucket, max_keys=10)
                
                for obj in objects[:5]:  # Show first 5
                    print(f"   - {obj['key']} ({obj['size']} bytes)")
                
                if len(objects) > 5:
                    print(f"   ... and {len(objects) - 5} more objects")
            
        else:
            print("   ❌ Connection failed!")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "="*50 + "\n")


def demo_minio_data_extraction():
    """Demonstrate data extraction from MinIO with validation"""
    print("=== MinIO Data Extraction Demo ===\n")
    
    # Note: This would require PySpark to be available
    print("1. This demo shows the MinIO extraction process:")
    print("   (Would require actual Spark session and MinIO data)")
    
    # Table configuration for demo
    table_config = TableConfig(
        identifier="raw.demo.minio_data",
        schema={
            "customer_id": "bigint",
            "first_name": "string",
            "last_name": "string",
            "email": "string",
            "registration_date": "date"
        },
        key_columns=["customer_id"],
        validation={
            "null_check": ["customer_id", "first_name"],
            "date_range": {
                "registration_date": {
                    "from": "2020-01-01",
                    "to": "2030-12-31"
                }
            }
        }
    )
    
    # MinIO config
    minio_config = {
        'endpoint': 'http://localhost:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin',
        'region': 'us-east-1',
        'use_ssl': False
    }
    
    print("2. Table configuration:")
    print(f"   - Table: {table_config.identifier}")
    print(f"   - Validation rules: null_check on {table_config.validation['null_check']}")
    print(f"   - Date range validation: {list(table_config.validation['date_range'].keys())}")
    
    print("\n3. MinIO extraction process would:")
    print("   a) Connect to MinIO at", minio_config['endpoint'])
    print("   b) List files matching pattern (e.g., *.csv.ZIP)")
    print("   c) Validate filename formats using FileNameParser")
    print("   d) Download files to local temp directory")
    print("   e) Extract ZIP files and process CSVs")
    print("   f) Apply table schema and validation rules")
    print("   g) Return validated DataFrame with metrics")
    
    # Simulated extraction result
    extraction_result = {
        'success': True,
        'source_path': 'data-bucket/raw/crm/',
        'files_processed': 3,
        'records_extracted': 15420,
        'validation_status': 'passed',
        'processing_time_seconds': 45.2,
        'files_downloaded': [
            'RBUS_CRM_20250101_W_20250101120000123.csv.ZIP',
            'RBUS_CRM_20250102_W_20250102120000456.csv.ZIP',
            'RBUS_CRM_20250103_W_20250103120000789.csv.ZIP'
        ]
    }
    
    print("\n4. Simulated extraction result:")
    print(f"   ✅ Success: {extraction_result['success']}")
    print(f"   📁 Files processed: {extraction_result['files_processed']}")
    print(f"   📊 Records extracted: {extraction_result['records_extracted']}")
    print(f"   ✅ Validation: {extraction_result['validation_status']}")
    print(f"   ⏱️ Processing time: {extraction_result['processing_time_seconds']}s")
    
    print("\n" + "="*50 + "\n")


def demo_enhanced_pipeline_with_minio():
    """Demonstrate enhanced ETL pipeline with MinIO source"""
    print("=== Enhanced Pipeline with MinIO Demo ===\n")
    
    # Configuration path
    config_path = str(Path(__file__).parent.parent / "configs")
    
    print("1. Enhanced ETL Pipeline with MinIO integration:")
    print("   - Source: MinIO bucket with structured filenames")
    print("   - Validation: Enhanced filename parsing + data validation")
    print("   - Processing: Full ETL with monitoring and alerting")
    print("   - Target: Iceberg tables in data lakehouse")
    
    # Example configuration for MinIO source
    pipeline_config = {
        'source': {
            'type': 'minio',
            'endpoint': 'http://localhost:9000',
            'bucket': 'source-data',
            'prefix': 'raw/crm/daily/',
            'file_pattern': '*.csv.ZIP',
            'credentials': {
                'access_key': 'minioadmin',
                'secret_key': 'minioadmin'
            }
        },
        'processing': {
            'validate_filenames': True,
            'extract_zip': True,
            'max_files_per_batch': 50,
            'enable_enhanced_validation': True,
            'custom_validation_rules': 'configs/validation/enhanced_rules.json'
        },
        'monitoring': {
            'track_file_downloads': True,
            'alert_on_validation_failures': True,
            'log_detailed_metrics': True
        }
    }
    
    print("\n2. Pipeline configuration:")
    print(f"   - Source bucket: {pipeline_config['source']['bucket']}")
    print(f"   - File pattern: {pipeline_config['source']['file_pattern']}")
    print(f"   - Enhanced validation: {pipeline_config['processing']['enable_enhanced_validation']}")
    print(f"   - Monitoring enabled: {pipeline_config['monitoring']['track_file_downloads']}")
    
    # Simulated pipeline execution
    print("\n3. Pipeline execution steps:")
    steps = [
        "Initialize MinIO connection and test connectivity",
        "Scan source bucket for files matching pattern",
        "Validate filenames using FileNameParser",
        "Download valid files to processing area", 
        "Extract ZIP files and identify CSV contents",
        "Apply enhanced data validation rules",
        "Process data through quality checks",
        "Load validated data to Iceberg tables",
        "Generate processing metrics and alerts",
        "Cleanup temporary files and resources"
    ]
    
    for i, step in enumerate(steps, 1):
        print(f"   {i:2d}. {step}")
    
    # Simulated results
    pipeline_result = {
        'pipeline_id': 'crm_minio_ingestion',
        'execution_date': datetime.now().isoformat(),
        'success': True,
        'source_files_found': 12,
        'files_downloaded': 12,
        'files_with_valid_names': 12,
        'total_records_processed': 245680,
        'validation_passed': True,
        'quality_score': 98.5,
        'processing_time_minutes': 8.2,
        'data_loaded_gb': 1.2
    }
    
    print("\n4. Pipeline execution results:")
    print(f"   🎯 Pipeline: {pipeline_result['pipeline_id']}")
    print(f"   ✅ Success: {pipeline_result['success']}")
    print(f"   📁 Files processed: {pipeline_result['files_downloaded']}/{pipeline_result['source_files_found']}")
    print(f"   📊 Records: {pipeline_result['total_records_processed']:,}")
    print(f"   ✅ Quality score: {pipeline_result['quality_score']}%")
    print(f"   ⏱️ Processing time: {pipeline_result['processing_time_minutes']} minutes")
    print(f"   💾 Data loaded: {pipeline_result['data_loaded_gb']} GB")
    
    print("\n" + "="*50 + "\n")


def show_configuration_examples():
    """Show configuration examples for MinIO integration"""
    print("=== Configuration Examples ===\n")
    
    print("1. MinIO configuration in dev.yaml:")
    minio_config = """
minio:
  endpoint: "http://localhost:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  region: "us-east-1"
  use_ssl: false
  verify_ssl: false
  max_retries: 3
  connect_timeout: 60
  read_timeout: 300
"""
    print(minio_config)
    
    print("2. Table configuration with MinIO source:")
    table_config = """
{
  "identifier": "raw.crm.customers_minio",
  "source_type": "minio",
  "source_config": {
    "bucket": "source-data",
    "prefix": "crm/customers/",
    "file_pattern": "RBUS_CRM_*_W_*.csv.ZIP"
  },
  "schema": {
    "customer_id": "bigint",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "registration_date": "date"
  },
  "validation": {
    "null_check": ["customer_id", "first_name"],
    "date_range": {
      "registration_date": {
        "from": "2020-01-01",
        "to": "2030-12-31"
      }
    }
  },
  "filename_validation": {
    "enabled": true,
    "pattern": "RBUS_CRM_YYYYMMDD_W_YYYYMMDDHHmmssSSS.csv.ZIP",
    "extract_metadata": true
  }
}
"""
    print(table_config)
    
    print("3. Pipeline configuration for MinIO source:")
    pipeline_config = """
{
  "pipeline_id": "minio_crm_ingestion",
  "source": {
    "type": "minio_extractor",
    "connection": "minio_dev",
    "extraction_config": {
      "source_path": "source-data/crm/daily/",
      "file_pattern": "*.csv.ZIP",
      "validate_filenames": true,
      "max_files": 100
    }
  },
  "processing": {
    "enable_enhanced_validation": true,
    "validation_rules_file": "configs/validation/crm_rules.json"
  },
  "monitoring": {
    "track_downloads": true,
    "alert_on_failures": true
  }
}
"""
    print(pipeline_config)


def main():
    """Run all MinIO integration demonstrations"""
    print("MinIO Integration with Enhanced Validation Demo")
    print("=" * 60)
    print()
    
    try:
        # Run demonstrations
        demo_minio_connection()
        demo_minio_data_extraction()
        demo_enhanced_pipeline_with_minio()
        show_configuration_examples()
        
        print("🎉 All MinIO integration demonstrations completed!")
        print("\n📋 Summary of implemented features:")
        print("   ✅ S3ConnectionManager for MinIO/boto3 integration")
        print("   ✅ MinIOExtractor with enhanced validation")
        print("   ✅ Filename parsing and validation")
        print("   ✅ Data quality validation integration")
        print("   ✅ Complete ETL pipeline with monitoring")
        print("   ✅ Configuration examples and patterns")
        
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 