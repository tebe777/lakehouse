#!/usr/bin/env python3
"""
Comprehensive example demonstrating the enhanced validation and filename parsing features.

This example shows how to:
1. Use the new DataValidator for data quality validation
2. Parse filenames using FileNameParser
3. Integrate validation with existing pipeline infrastructure
4. Handle custom validation rules
5. Process files with enhanced monitoring
"""

import sys
from pathlib import Path
from datetime import datetime, date
import json

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, BooleanType

from src.common.utils.validator import DataValidator
from src.common.utils.filename_parser import FileNameParser
from src.common.utils.validation_integration import (
    IntegratedDataValidator, 
    FileProcessor,
    load_validation_rules_from_file
)
from src.common.utils.config import TableConfig
from src.main.monitoring_integration import MonitoredETLPipeline


def demo_data_validator():
    """Demonstrate the new DataValidator functionality"""
    print("=== DataValidator Demo ===\n")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Enhanced Validation Demo") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    try:
        # 1. Create sample data with validation issues
        print("1. Creating sample data with validation issues:")
        
        schema = StructType([
            StructField("customer_id", LongType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("registration_date", StringType(), True)  # Using string to test date validation
        ])
        
        # Sample data with some validation issues
        data = [
            (1, "John", "Doe", "john.doe@email.com", "2023-05-15"),
            (2, "Jane", None, "jane.smith@email.com", "2023-06-20"),  # null last_name
            (3, "Bob", "Johnson", "bob@email.com", "2019-12-31"),  # date out of range
            (4, "Alice", "Williams", None, "2023-07-10"),  # null email (allowed)
            (5, None, "Brown", "charlie@email.com", "2023-08-01")  # null customer_id (not allowed)
        ]
        
        df = spark.createDataFrame(data, schema)
        print(f"   Created DataFrame with {df.count()} rows")
        df.show()
        
        # 2. Define validation rules
        print("\n2. Defining validation rules:")
        validation_rules = {
            "null_check": ["customer_id", "first_name"],  # These columns must not be null
            "date_range": {
                "registration_date": {
                    "from": "2020-01-01",
                    "to": "2030-12-31"
                }
            }
        }
        print(f"   Validation rules: {json.dumps(validation_rules, indent=2)}")
        
        # 3. Create and use DataValidator
        print("\n3. Running DataValidator:")
        validator = DataValidator(validation_rules)
        
        try:
            validator.validate(df)
            print("   ✅ All validation checks passed!")
        except ValueError as e:
            print(f"   ❌ Validation failed: {e}")
        
        print("\n" + "="*50 + "\n")
        
    finally:
        spark.stop()


def demo_filename_parser():
    """Demonstrate the FileNameParser functionality"""
    print("=== FileNameParser Demo ===\n")
    
    # Test filenames
    test_filenames = [
        "AAA_BBB_20250501_W_20250502120515517.csv.ZIP",  # Valid
        "AAA_XXX_20250601_P_20250601143022123.csv.ZIP",     # Valid
        "invalid_filename.csv",                                    # Invalid
        "MISSING_DATE_W_20250502120515517.csv.ZIP",              # Invalid - missing date
    ]
    
    print("1. Testing filename parsing:")
    
    for filename in test_filenames:
        print(f"\n   Parsing: {filename}")
        try:
            result = FileNameParser.parse(filename)
            print(f"   ✅ Success:")
            print(f"      - Prefix: {result['prefix']}")
            print(f"      - Extract Date: {result['extract_date']}")
            print(f"      - File Type: {result['file_type']}")
            print(f"      - File Timestamp: {result['file_timestamp']}")
        except ValueError as e:
            print(f"   ❌ Failed: {e}")
    
    print("\n" + "="*50 + "\n")


def demo_integrated_validation():
    """Demonstrate the integrated validation features"""
    print("=== Integrated Validation Demo ===\n")
    
    # 1. Create table configuration
    print("1. Creating table configuration:")
    table_config = TableConfig(
        identifier="raw.demo.enhanced_validation_example",
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
    print(f"   Table: {table_config.identifier}")
    print(f"   Validation rules: {table_config.validation}")
    
    # 2. Create file processor
    print("\n2. Creating file processor:")
    file_processor = FileProcessor(table_config)
    
    # Simulate processing a file
    test_file_path = "AAA_DEMO_20250501_W_20250502120515517.csv.ZIP"
    print(f"   Processing file: {test_file_path}")
    
    # Note: In real usage, this would be an actual file path
    # For demo, we'll simulate the result
    file_result = {
        'table_identifier': table_config.identifier,
        'file_path': test_file_path,
        'processing_timestamp': datetime.now().isoformat(),
        'filename_info': {
            'prefix': 'AAA_DEMO',
            'extract_date': date(2025, 5, 1),
            'file_type': 'W',
            'file_timestamp': datetime(2025, 5, 2, 12, 5, 15)
        },
        'file_stats': {'file_exists': True, 'file_size_bytes': 1024},
        'validation_status': 'passed',
        'errors': []
    }
    
    print(f"   ✅ File processing result:")
    print(f"      - Status: {file_result['validation_status']}")
    print(f"      - Filename info: {file_result['filename_info']}")
    print(f"      - File stats: {file_result['file_stats']}")
    
    print("\n" + "="*50 + "\n")


def demo_enhanced_pipeline():
    """Demonstrate the enhanced ETL pipeline"""
    print("=== Enhanced ETL Pipeline Demo ===\n")
    
    # Create config directory path (simulated)
    config_path = str(Path(__file__).parent.parent / "configs")
    
    print("1. Initializing enhanced ETL pipeline:")
    pipeline = MonitoredETLPipeline(config_path, environment="dev")
    print(f"   ✅ Pipeline initialized")
    
    # 2. Get pipeline status
    print("\n2. Getting enhanced pipeline status:")
    status = pipeline.get_enhanced_pipeline_status()
    print(f"   Pipeline: {status['pipeline_name']}")
    print(f"   Status: {status['status']}")
    print(f"   Components: {status['components']}")
    print(f"   Capabilities: {status['capabilities']}")
    
    print("\n" + "="*50 + "\n")


def main():
    """Run all demonstrations"""
    print("Enhanced Validation and Filename Parsing Demo")
    print("=" * 50)
    print()
    
    try:
        # Run individual demonstrations
        demo_data_validator()
        demo_filename_parser()
        demo_integrated_validation()
        demo_enhanced_pipeline()
        
        print("🎉 All demonstrations completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 