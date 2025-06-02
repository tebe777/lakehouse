import unittest
import tempfile
import zipfile
import csv
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType

from src.ingestion.raw_loader import RawDataLoader, SchemaEvolutionError
from src.common.models.table_config import TableConfig, ColumnConfig, DataType
from src.common.exceptions import LoadError, ValidationError


class TestRawDataLoader(unittest.TestCase):
    """Unit tests for RawDataLoader"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for tests"""
        cls.spark = SparkSession.builder \
            .appName("RawDataLoaderTests") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test table configuration
        self.table_config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test",
            table_name="customers",
            columns=[
                ColumnConfig(name="customer_id", data_type=DataType.LONG, nullable=False),
                ColumnConfig(name="first_name", data_type=DataType.STRING, nullable=False),
                ColumnConfig(name="last_name", data_type=DataType.STRING, nullable=False),
                ColumnConfig(name="email", data_type=DataType.STRING, nullable=True)
            ],
            business_keys=["customer_id"]
        )
        
        # Create test DataFrame
        self.test_data = [
            (1, "John", "Doe", "john.doe@test.com"),
            (2, "Jane", "Smith", "jane.smith@test.com"),
            (3, "Bob", "Johnson", "bob.johnson@test.com")
        ]
        
        schema = StructType([
            StructField("customer_id", LongType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("email", StringType(), True)
        ])
        
        self.test_df = self.spark.createDataFrame(self.test_data, schema)
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    # ==========================================
    # INITIALIZATION TESTS
    # ==========================================
    
    def test_init_dataframe_mode(self):
        """Test initialization for DataFrame-based loading"""
        loader = RawDataLoader(
            spark=self.spark,
            table_config=self.table_config
        )
        
        self.assertEqual(loader.spark, self.spark)
        self.assertEqual(loader.table_config, self.table_config)
        self.assertIsNone(loader.catalog)
        self.assertIsNotNone(loader.logger)
    
    def test_init_zip_mode(self):
        """Test initialization for ZIP-based loading"""
        loader = RawDataLoader(
            spark=self.spark,
            iceberg_catalog_name="test_catalog",
            warehouse_path="/tmp/warehouse",
            temp_dir=self.temp_dir
        )
        
        self.assertEqual(loader.spark, self.spark)
        self.assertEqual(loader.iceberg_catalog_name, "test_catalog")
        self.assertEqual(loader.warehouse_path, "/tmp/warehouse")
        self.assertEqual(str(loader.temp_dir), self.temp_dir)
    
    # ==========================================
    # DATAFRAME VALIDATION TESTS
    # ==========================================
    
    def test_validate_input_data_valid(self):
        """Test validation of valid DataFrame"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        # Should not raise exception
        loader._validate_input_data(self.test_df)
    
    def test_validate_input_data_none(self):
        """Test validation of None DataFrame"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        with self.assertRaises(ValidationError):
            loader._validate_input_data(None)
    
    def test_validate_input_data_empty(self):
        """Test validation of empty DataFrame"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        empty_df = self.spark.createDataFrame([], self.test_df.schema)
        
        # Should not raise exception but log warning
        loader._validate_input_data(empty_df)
    
    def test_validate_input_data_missing_columns(self):
        """Test validation with missing required columns"""
        # Create table config with non-nullable column
        table_config = TableConfig(
            identifier="raw.test.customers",
            layer="raw",
            source_type="test", 
            table_name="customers",
            columns=[
                ColumnConfig(name="customer_id", data_type=DataType.LONG, nullable=False),
                ColumnConfig(name="required_column", data_type=DataType.STRING, nullable=False)
            ],
            business_keys=["customer_id"]
        )
        
        loader = RawDataLoader(spark=self.spark, table_config=table_config)
        
        with self.assertRaises(ValidationError):
            loader._validate_input_data(self.test_df)  # Missing required_column
    
    # ==========================================
    # DATA PREPARATION TESTS
    # ==========================================
    
    def test_prepare_data_for_load(self):
        """Test data preparation adds technical columns"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        prepared_df = loader._prepare_data_for_load(self.test_df)
        
        # Should have original columns plus technical columns
        self.assertIn('_load_timestamp', prepared_df.columns)
        self.assertIn('_batch_id', prepared_df.columns)
        self.assertIn('_record_hash', prepared_df.columns)
        
        # Should have same number of rows
        self.assertEqual(prepared_df.count(), self.test_df.count())
    
    def test_add_record_hash(self):
        """Test record hash generation"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        df_with_hash = loader._add_record_hash(self.test_df)
        
        self.assertIn('_record_hash', df_with_hash.columns)
        
        # Hash should be different for different rows
        hashes = [row['_record_hash'] for row in df_with_hash.collect()]
        self.assertEqual(len(set(hashes)), len(hashes))  # All unique
    
    # ==========================================
    # ZIP PROCESSING TESTS
    # ==========================================
    
    def create_test_csv(self, filename: str, rows: int = 10) -> str:
        """Helper to create test CSV file"""
        file_path = os.path.join(self.temp_dir, filename)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header
            writer.writerow(['customer_id', 'first_name', 'last_name', 'email'])
            
            # Data
            for i in range(rows):
                writer.writerow([
                    str(i + 1),
                    f'First_{i+1}',
                    f'Last_{i+1}',
                    f'user{i+1}@test.com'
                ])
        
        return file_path
    
    def create_test_zip(self, filename: str, csv_files: list) -> str:
        """Helper to create test ZIP file"""
        zip_path = os.path.join(self.temp_dir, filename)
        
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for csv_file in csv_files:
                zipf.write(csv_file, os.path.basename(csv_file))
        
        return zip_path
    
    def test_validate_zip_input_valid(self):
        """Test validation of valid ZIP file"""
        csv_file = self.create_test_csv("test.csv")
        zip_file = self.create_test_zip("test.zip", [csv_file])
        
        loader = RawDataLoader(spark=self.spark, temp_dir=self.temp_dir)
        
        result = loader._validate_zip_input(zip_file)
        self.assertTrue(result)
    
    def test_validate_zip_input_missing_file(self):
        """Test validation of missing ZIP file"""
        loader = RawDataLoader(spark=self.spark, temp_dir=self.temp_dir)
        
        result = loader._validate_zip_input("/nonexistent/file.zip")
        self.assertFalse(result)
    
    def test_validate_zip_input_no_csv(self):
        """Test validation of ZIP with no CSV files"""
        # Create ZIP with non-CSV file
        txt_file = os.path.join(self.temp_dir, "test.txt")
        with open(txt_file, 'w') as f:
            f.write("test content")
        
        zip_file = self.create_test_zip("test.zip", [txt_file])
        
        loader = RawDataLoader(spark=self.spark, temp_dir=self.temp_dir)
        
        result = loader._validate_zip_input(zip_file)
        self.assertFalse(result)
    
    def test_extract_zip_file(self):
        """Test ZIP file extraction"""
        csv_file1 = self.create_test_csv("test1.csv", 5)
        csv_file2 = self.create_test_csv("test2.csv", 10)
        zip_file = self.create_test_zip("test.zip", [csv_file1, csv_file2])
        
        loader = RawDataLoader(spark=self.spark, temp_dir=self.temp_dir)
        
        extracted_files = loader._extract_zip_file(zip_file, "2024-01-01T10:00:00")
        
        self.assertEqual(len(extracted_files), 2)
        
        # Check files exist and are CSV
        for file_path in extracted_files:
            self.assertTrue(os.path.exists(file_path))
            self.assertTrue(file_path.endswith('.csv'))
    
    # ==========================================
    # SCHEMA BUILDING TESTS  
    # ==========================================
    
    def test_build_spark_schema(self):
        """Test Spark schema building from table config"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        schema = loader._build_spark_schema(self.table_config)
        
        self.assertIsInstance(schema, StructType)
        self.assertEqual(len(schema.fields), 4)
        
        field_names = [field.name for field in schema.fields]
        self.assertIn('customer_id', field_names)
        self.assertIn('first_name', field_names)
        self.assertIn('last_name', field_names)
        self.assertIn('email', field_names)
    
    def test_get_spark_type_conversion(self):
        """Test data type conversion to Spark types"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        from pyspark.sql.types import StringType, LongType, BooleanType, TimestampType
        
        self.assertIsInstance(loader._get_spark_type('string'), StringType)
        self.assertIsInstance(loader._get_spark_type('long'), LongType)
        self.assertIsInstance(loader._get_spark_type('boolean'), BooleanType)
        self.assertIsInstance(loader._get_spark_type('timestamp'), TimestampType)
        
        # Unknown type should default to StringType
        self.assertIsInstance(loader._get_spark_type('unknown'), StringType)
    
    # ==========================================
    # TABLE CREATION TESTS
    # ==========================================
    
    def test_build_create_table_sql_basic(self):
        """Test SQL generation for table creation"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        sql = loader._build_create_table_sql("test.customers")
        
        self.assertIn("CREATE TABLE IF NOT EXISTS", sql)
        self.assertIn("test.customers", sql)
        self.assertIn("customer_id LONG NOT NULL", sql)
        self.assertIn("first_name STRING NOT NULL", sql)
        self.assertIn("email STRING", sql)  # nullable
        self.assertIn("USING ICEBERG", sql)
        self.assertIn("TBLPROPERTIES", sql)
    
    def test_build_create_table_sql_with_iceberg_properties(self):
        """Test SQL generation includes Iceberg properties"""
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        sql = loader._build_create_table_sql("test.customers")
        
        # Should include Iceberg properties from table config
        self.assertIn("'format-version' = '2'", sql)
        self.assertIn("'write.delete.mode' = 'merge-on-read'", sql)
        self.assertIn("'write.target-file-size-bytes'", sql)
    
    # ==========================================
    # ERROR HANDLING TESTS
    # ==========================================
    
    def test_load_data_without_table_config(self):
        """Test that DataFrame loading requires table config"""
        loader = RawDataLoader(spark=self.spark)  # No table_config
        
        with self.assertRaises(ValidationError):
            loader.load_data(self.test_df, "test.table")
    
    @patch('src.ingestion.raw_loader.RawDataLoader._table_exists')
    @patch('src.ingestion.raw_loader.RawDataLoader._load_direct')
    def test_load_data_success(self, mock_load_direct, mock_table_exists):
        """Test successful data loading"""
        # Mock methods
        mock_table_exists.return_value = True
        mock_load_direct.return_value = {'records_loaded': 3, 'load_type': 'direct'}
        
        loader = RawDataLoader(spark=self.spark, table_config=self.table_config)
        
        result = loader.load_data(self.test_df, "test.customers")
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['records_loaded'], 3)
    
    # ==========================================
    # UTILITY TESTS
    # ==========================================
    
    def test_add_technical_columns_zip(self):
        """Test adding technical columns for ZIP processing"""
        loader = RawDataLoader(spark=self.spark, temp_dir=self.temp_dir)
        
        df_with_tech = loader._add_technical_columns_zip(
            self.test_df, 
            "/path/to/source.csv", 
            "2024-01-01T10:00:00"
        )
        
        self.assertIn('_load_timestamp', df_with_tech.columns)
        self.assertIn('_batch_id', df_with_tech.columns)
        self.assertIn('_source_file', df_with_tech.columns)
        
        # Check values
        first_row = df_with_tech.collect()[0]
        self.assertEqual(first_row['_source_file'], 'source.csv')
        self.assertEqual(first_row['_batch_id'], '2024-01-01T10-00-00')
    
    def test_cleanup_temp_files(self):
        """Test cleanup of temporary files"""
        # Create test files
        test_files = []
        for i in range(3):
            file_path = os.path.join(self.temp_dir, f"test_{i}.csv")
            with open(file_path, 'w') as f:
                f.write("test content")
            test_files.append(file_path)
        
        # Verify files exist
        for file_path in test_files:
            self.assertTrue(os.path.exists(file_path))
        
        loader = RawDataLoader(spark=self.spark, temp_dir=self.temp_dir)
        loader._cleanup_temp_files(test_files)
        
        # Verify files are deleted
        for file_path in test_files:
            self.assertFalse(os.path.exists(file_path))


if __name__ == '__main__':
    unittest.main() 