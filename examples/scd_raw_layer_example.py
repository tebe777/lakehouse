#!/usr/bin/env python3
"""
Example demonstrating SCD Type 2 processing in the raw layer.

This script shows how to:
1. Load data into raw layer with SCD Type 2 enabled
2. Process incremental changes
3. Track historical data changes
4. Query historical data
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from catalog.config_manager import ConfigurationManager
from ingestion.raw_loader import RawDataLoader
from common.models.table_config import TableConfig


def create_spark_session():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("SCD_Raw_Layer_Example") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "file:///tmp/iceberg-warehouse") \
        .getOrCreate()


def create_sample_data(spark, scenario="initial"):
    """Create sample customer data for different scenarios."""
    
    schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_date", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("credit_limit", DoubleType(), True)
    ])
    
    if scenario == "initial":
        # Initial load data
        data = [
            (1, "John", "Doe", "john.doe@email.com", "555-0101", datetime(2024, 1, 1), "active", 5000.0),
            (2, "Jane", "Smith", "jane.smith@email.com", "555-0102", datetime(2024, 1, 2), "active", 7500.0),
            (3, "Bob", "Johnson", "bob.johnson@email.com", "555-0103", datetime(2024, 1, 3), "active", 3000.0),
            (4, "Alice", "Brown", "alice.brown@email.com", "555-0104", datetime(2024, 1, 4), "pending", 0.0),
            (5, "Charlie", "Wilson", "charlie.wilson@email.com", "555-0105", datetime(2024, 1, 5), "active", 10000.0)
        ]
    elif scenario == "updates":
        # Updated data with changes
        data = [
            (1, "John", "Doe", "john.doe.new@email.com", "555-0101", datetime(2024, 1, 1), "active", 6000.0),  # Email and credit limit changed
            (2, "Jane", "Smith-Jones", "jane.smith@email.com", "555-0102", datetime(2024, 1, 2), "active", 7500.0),  # Last name changed
            (3, "Bob", "Johnson", "bob.johnson@email.com", "555-0103", datetime(2024, 1, 3), "suspended", 3000.0),  # Status changed
            (4, "Alice", "Brown", "alice.brown@email.com", "555-0104", datetime(2024, 1, 4), "active", 2500.0),  # Status and credit limit changed
            (6, "David", "Miller", "david.miller@email.com", "555-0106", datetime(2024, 1, 6), "active", 4000.0)  # New customer
        ]
    elif scenario == "more_updates":
        # Additional updates
        data = [
            (1, "John", "Doe", "john.doe.latest@email.com", "555-0101", datetime(2024, 1, 1), "active", 6500.0),  # Email and credit limit changed again
            (2, "Jane", "Smith-Jones", "jane.smith@email.com", "555-0102", datetime(2024, 1, 2), "inactive", 7500.0),  # Status changed
            (6, "David", "Miller", "david.miller@email.com", "555-0999", datetime(2024, 1, 6), "active", 4000.0),  # Phone changed
            (7, "Eva", "Garcia", "eva.garcia@email.com", "555-0107", datetime(2024, 1, 7), "active", 5500.0)  # New customer
        ]
    
    return spark.createDataFrame(data, schema)


def demonstrate_scd_raw_layer():
    """Demonstrate SCD Type 2 processing in the raw layer."""
    
    print("=== SCD Type 2 Raw Layer Demo ===\n")
    
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        # Initialize configuration manager
        config_dir = Path(__file__).parent.parent / "configs"
        config_manager = ConfigurationManager(str(config_dir))
        
        # Get table configuration
        table_config = config_manager.get_table_config("raw.crm.customers")
        if not table_config:
            print("❌ Table configuration not found!")
            return
        
        print(f"✅ Loaded table configuration: {table_config.identifier}")
        print(f"   SCD Enabled: {table_config.enable_scd}")
        print(f"   Business Keys: {table_config.business_keys}")
        print(f"   SCD Columns: {getattr(table_config, 'scd_columns', [])}")
        print()
        
        # Initialize raw data loader
        raw_loader = RawDataLoader(spark, table_config)
        
        target_table = "local.raw_crm_customers"
        
        # Step 1: Initial Load
        print("📥 Step 1: Initial Load")
        initial_data = create_sample_data(spark, "initial")
        print(f"   Loading {initial_data.count()} initial records...")
        
        initial_result = raw_loader.load_data(
            df=initial_data,
            target_table=target_table,
            write_mode="append",
            enable_scd=True,
            batch_id="initial_load_001"
        )
        
        print(f"   ✅ Initial load completed:")
        print(f"      Records loaded: {initial_result['records_loaded']}")
        print(f"      New records: {initial_result['new_records']}")
        print()
        
        # Query initial data
        print("🔍 Querying initial data:")
        initial_query = spark.sql(f"""
            SELECT customer_id, first_name, last_name, email, status, credit_limit,
                   valid_from, valid_to, is_current
            FROM {target_table}
            ORDER BY customer_id
        """)
        initial_query.show(truncate=False)
        print()
        
        # Step 2: Process Updates
        print("📥 Step 2: Processing Updates")
        updated_data = create_sample_data(spark, "updates")
        print(f"   Processing {updated_data.count()} records with updates...")
        
        update_result = raw_loader.load_data(
            df=updated_data,
            target_table=target_table,
            write_mode="append",
            enable_scd=True,
            batch_id="update_batch_001",
            processing_timestamp=datetime.now() + timedelta(hours=1)
        )
        
        print(f"   ✅ Update processing completed:")
        print(f"      Records processed: {update_result['records_loaded']}")
        print(f"      New records: {update_result['new_records']}")
        print(f"      Updated records: {update_result['updated_records']}")
        print(f"      Unchanged records: {update_result['unchanged_records']}")
        print()
        
        # Query current data after updates
        print("🔍 Querying current data after updates:")
        current_query = spark.sql(f"""
            SELECT customer_id, first_name, last_name, email, status, credit_limit,
                   valid_from, valid_to, is_current
            FROM {target_table}
            WHERE is_current = true
            ORDER BY customer_id
        """)
        current_query.show(truncate=False)
        print()
        
        # Query historical data
        print("🔍 Querying historical data (all versions):")
        history_query = spark.sql(f"""
            SELECT customer_id, first_name, last_name, email, status, credit_limit,
                   valid_from, valid_to, is_current
            FROM {target_table}
            ORDER BY customer_id, valid_from
        """)
        history_query.show(truncate=False)
        print()
        
        # Step 3: More Updates
        print("📥 Step 3: Processing More Updates")
        more_updates = create_sample_data(spark, "more_updates")
        print(f"   Processing {more_updates.count()} additional records...")
        
        final_result = raw_loader.load_data(
            df=more_updates,
            target_table=target_table,
            write_mode="append",
            enable_scd=True,
            batch_id="update_batch_002",
            processing_timestamp=datetime.now() + timedelta(hours=2)
        )
        
        print(f"   ✅ Final update processing completed:")
        print(f"      Records processed: {final_result['records_loaded']}")
        print(f"      New records: {final_result['new_records']}")
        print(f"      Updated records: {final_result['updated_records']}")
        print(f"      Unchanged records: {final_result['unchanged_records']}")
        print()
        
        # Final queries
        print("🔍 Final current data:")
        final_current = spark.sql(f"""
            SELECT customer_id, first_name, last_name, email, status, credit_limit,
                   valid_from, valid_to, is_current
            FROM {target_table}
            WHERE is_current = true
            ORDER BY customer_id
        """)
        final_current.show(truncate=False)
        print()
        
        # Show change history for specific customer
        print("🔍 Change history for Customer ID 1:")
        customer_history = spark.sql(f"""
            SELECT customer_id, first_name, last_name, email, status, credit_limit,
                   valid_from, valid_to, is_current
            FROM {target_table}
            WHERE customer_id = 1
            ORDER BY valid_from
        """)
        customer_history.show(truncate=False)
        print()
        
        # Summary statistics
        print("📊 Summary Statistics:")
        total_records = spark.sql(f"SELECT COUNT(*) as total FROM {target_table}").collect()[0]['total']
        current_records = spark.sql(f"SELECT COUNT(*) as current FROM {target_table} WHERE is_current = true").collect()[0]['current']
        historical_records = total_records - current_records
        
        print(f"   Total records in table: {total_records}")
        print(f"   Current records: {current_records}")
        print(f"   Historical records: {historical_records}")
        print()
        
        print("✅ SCD Type 2 Raw Layer Demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    demonstrate_scd_raw_layer() 