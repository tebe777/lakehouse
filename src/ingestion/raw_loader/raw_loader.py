# src/ingestion/raw_loader/raw_loader.py
import os
import zipfile
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from src.common.models.interfaces import IDataProcessor
from src.common.models.table_config import TableConfig, LayerType
from src.common.utils.spark_utils import SparkUtils
from src.common.monitoring.logger import ETLLogger


class SchemaEvolutionError(Exception):
    """Raised when schema evolution cannot be handled"""
    pass


class RawDataLoader(IDataProcessor):
    """
    Loads raw data from ZIP files to Iceberg Raw layer with schema evolution support.
    
    Features:
    - ZIP file extraction and validation
    - CSV parsing with configurable delimiters
    - Schema enforcement and evolution
    - Technical metadata columns addition
    - Iceberg table creation and data writing
    - Error handling and dead letter queue
    """
    
    def __init__(self, spark: SparkSession, iceberg_catalog_name: str, 
                 warehouse_path: str, temp_dir: str = "/tmp/etl"):
        """
        Initialize RawDataLoader
        
        Args:
            spark: SparkSession with Iceberg configuration
            iceberg_catalog_name: Name of Iceberg catalog
            warehouse_path: Path to Iceberg warehouse
            temp_dir: Temporary directory for file extraction
        """
        self.spark = spark
        self.iceberg_catalog_name = iceberg_catalog_name
        self.warehouse_path = warehouse_path
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Iceberg catalog
        self.catalog = load_catalog(
            name=iceberg_catalog_name,
            **{
                "type": "rest",
                "uri": f"http://nessie:19120/api/v1",
                "warehouse": warehouse_path
            }
        )
        
        self.logger = ETLLogger(self.__class__.__name__)
        self.spark_utils = SparkUtils(spark)
        
        # CSV parsing configuration
        self.csv_options = {
            "header": "true",
            "inferSchema": "false",  # We'll enforce schema
            "encoding": "UTF-8",
            "sep": ",",
            "quote": '"',
            "escape": '"',
            "multiLine": "true",
            "ignoreLeadingWhiteSpace": "true",
            "ignoreTrailingWhiteSpace": "true"
        }
    
    def process(self, zip_path: str, table_config: TableConfig, 
                execution_date: str = None) -> Dict[str, Any]:
        """
        Main processing method: extract ZIP, validate, and load to Iceberg
        
        Args:
            zip_path: Path to ZIP file containing CSV files
            table_config: Table configuration with schema and rules
            execution_date: Execution date for partitioning
            
        Returns:
            Dict with processing results and statistics
        """
        if execution_date is None:
            execution_date = datetime.now().isoformat()
            
        self.logger.info(f"Starting raw data loading for {table_config.identifier}")
        self.logger.info(f"ZIP file: {zip_path}, Execution date: {execution_date}")
        
        results = {
            "table_name": table_config.identifier,
            "execution_date": execution_date,
            "zip_file": zip_path,
            "files_processed": 0,
            "rows_loaded": 0,
            "errors": [],
            "warnings": [],
            "success": False,
            "processing_time_seconds": 0
        }
        
        start_time = datetime.now()
        
        try:
            # Step 1: Validate input
            if not self.validate_input(zip_path):
                raise ValueError(f"Invalid ZIP file: {zip_path}")
            
            # Step 2: Extract ZIP file
            extracted_files = self._extract_zip_file(zip_path, execution_date)
            self.logger.info(f"Extracted {len(extracted_files)} CSV files")
            
            # Step 3: Ensure Iceberg table exists
            self._ensure_table_exists(table_config)
            
            # Step 4: Process each CSV file
            total_rows = 0
            for csv_file in extracted_files:
                try:
                    rows_processed = self._process_csv_file(csv_file, table_config, execution_date)
                    total_rows += rows_processed
                    results["files_processed"] += 1
                    self.logger.info(f"Processed {csv_file}: {rows_processed} rows")
                except Exception as e:
                    error_msg = f"Failed to process {csv_file}: {str(e)}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    # Move to dead letter queue
                    self._move_to_dlq(csv_file, str(e))
            
            results["rows_loaded"] = total_rows
            results["success"] = results["files_processed"] > 0
            
            # Step 5: Cleanup temporary files
            self._cleanup_temp_files(extracted_files)
            
        except Exception as e:
            error_msg = f"Raw data loading failed: {str(e)}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            results["success"] = False
        
        finally:
            processing_time = (datetime.now() - start_time).total_seconds()
            results["processing_time_seconds"] = processing_time
            
            self.logger.info(f"Raw data loading completed. Success: {results['success']}")
            self.logger.info(f"Files processed: {results['files_processed']}, "
                           f"Rows loaded: {results['rows_loaded']}, "
                           f"Processing time: {processing_time:.2f}s")
        
        return results
    
    def validate_input(self, zip_path: str) -> bool:
        """Validate ZIP file exists and is readable"""
        try:
            zip_file = Path(zip_path)
            if not zip_file.exists():
                self.logger.error(f"ZIP file does not exist: {zip_path}")
                return False
            
            if not zipfile.is_zipfile(zip_path):
                self.logger.error(f"File is not a valid ZIP archive: {zip_path}")
                return False
            
            # Test ZIP file can be opened
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if not csv_files:
                    self.logger.error(f"No CSV files found in ZIP: {zip_path}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"ZIP file validation failed: {str(e)}")
            return False
    
    def _extract_zip_file(self, zip_path: str, execution_date: str) -> List[str]:
        """Extract ZIP file to temporary directory"""
        extract_dir = self.temp_dir / f"extract_{execution_date.replace(':', '-')}"
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        extracted_files = []
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for file_info in zf.infolist():
                if file_info.filename.endswith('.csv'):
                    # Extract file
                    extracted_path = zf.extract(file_info, extract_dir)
                    extracted_files.append(extracted_path)
        
        return extracted_files
    
    def _ensure_table_exists(self, table_config: TableConfig) -> None:
        """Ensure Iceberg table exists, create if necessary"""
        try:
            # Try to load existing table
            table = self.catalog.load_table(table_config.identifier)
            self.logger.info(f"Table {table_config.identifier} already exists")
            
            # Check for schema evolution
            self._handle_schema_evolution(table, table_config)
            
        except NoSuchTableError:
            # Create new table
            self.logger.info(f"Creating new table: {table_config.identifier}")
            self._create_iceberg_table(table_config)
    
    def _create_iceberg_table(self, table_config: TableConfig) -> None:
        """Create new Iceberg table with schema and partitioning"""
        # Build Iceberg schema
        iceberg_schema = self._build_iceberg_schema(table_config)
        
        # Build partition spec
        partition_spec = None
        if table_config.partition_spec:
            # For raw layer, typically partition by ingestion date
            partition_spec = [("_ingested_at", "month")]
        
        # Create table
        table = self.catalog.create_table(
            identifier=table_config.identifier,
            schema=iceberg_schema,
            partition_spec=partition_spec
        )
        
        self.logger.info(f"Created Iceberg table: {table_config.identifier}")
    
    def _build_iceberg_schema(self, table_config: TableConfig) -> pa.Schema:
        """Build PyArrow schema for Iceberg table"""
        fields = []
        
        # Add business columns from config
        for col_name, col_type in table_config.schema.items():
            pa_type = self._spark_type_to_pyarrow(col_type)
            fields.append(pa.field(col_name, pa_type, nullable=True))
        
        # Add technical columns
        technical_fields = [
            pa.field("_ingested_at", pa.timestamp('us'), nullable=False),
            pa.field("_source_file", pa.string(), nullable=True),
            pa.field("_execution_date", pa.string(), nullable=False),
            pa.field("_row_number", pa.int64(), nullable=True)
        ]
        
        fields.extend(technical_fields)
        
        return pa.schema(fields)
    
    def _spark_type_to_pyarrow(self, spark_type: str) -> pa.DataType:
        """Convert Spark SQL type string to PyArrow type"""
        type_mapping = {
            "string": pa.string(),
            "long": pa.int64(),
            "int": pa.int32(),
            "double": pa.float64(),
            "float": pa.float32(),
            "boolean": pa.bool_(),
            "timestamp": pa.timestamp('us'),
            "date": pa.date32(),
            "decimal": pa.decimal128(10, 2)
        }
        
        return type_mapping.get(spark_type.lower(), pa.string())
    
    def _process_csv_file(self, csv_path: str, table_config: TableConfig, 
                         execution_date: str) -> int:
        """Process single CSV file and append to Iceberg table"""
        self.logger.info(f"Processing CSV file: {csv_path}")
        
        # Read CSV with Spark
        df = self._read_csv_with_schema_enforcement(csv_path, table_config)
        
        # Add technical columns
        df_with_metadata = self._add_technical_columns(df, csv_path, execution_date)
        
        # Write to Iceberg using Spark SQL
        self._write_to_iceberg(df_with_metadata, table_config)
        
        row_count = df_with_metadata.count()
        return row_count
    
    def _read_csv_with_schema_enforcement(self, csv_path: str, 
                                        table_config: TableConfig) -> DataFrame:
        """Read CSV with strict schema enforcement"""
        # Build Spark schema from config
        spark_schema = self._build_spark_schema(table_config)
        
        try:
            # Read CSV with enforced schema
            df = self.spark.read \
                .options(**self.csv_options) \
                .schema(spark_schema) \
                .csv(csv_path)
            
            # Validate all required columns are present
            missing_cols = set(table_config.schema.keys()) - set(df.columns)
            if missing_cols:
                raise SchemaEvolutionError(f"Missing columns in CSV: {missing_cols}")
            
            # Check for extra columns (schema evolution)
            extra_cols = set(df.columns) - set(table_config.schema.keys())
            if extra_cols:
                self.logger.warning(f"Extra columns found in CSV: {extra_cols}")
                # For now, we'll keep extra columns and add them to table config
                # In production, this might trigger a schema evolution process
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV with schema enforcement: {str(e)}")
            # Fallback: read without schema and try to cast
            return self._read_csv_with_fallback(csv_path, table_config)
    
    def _read_csv_with_fallback(self, csv_path: str, table_config: TableConfig) -> DataFrame:
        """Fallback CSV reading with type casting"""
        self.logger.warning(f"Using fallback CSV reading for: {csv_path}")
        
        # Read without schema
        df = self.spark.read.options(**self.csv_options).csv(csv_path)
        
        # Try to cast columns to expected types
        for col_name, col_type in table_config.schema.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(col_type))
        
        return df
    
    def _build_spark_schema(self, table_config: TableConfig) -> StructType:
        """Build Spark StructType schema from table config"""
        fields = []
        
        for col_name, col_type in table_config.schema.items():
            spark_type = self._get_spark_type(col_type)
            fields.append(StructField(col_name, spark_type, True))
        
        return StructType(fields)
    
    def _get_spark_type(self, type_string: str):
        """Convert type string to Spark DataType"""
        type_mapping = {
            "string": StringType(),
            "long": LongType(),
            "timestamp": TimestampType(),
            "boolean": BooleanType()
        }
        return type_mapping.get(type_string.lower(), StringType())
    
    def _add_technical_columns(self, df: DataFrame, source_file: str, 
                             execution_date: str) -> DataFrame:
        """Add technical metadata columns"""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        # Add technical columns
        df_with_tech = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_file", lit(source_file)) \
            .withColumn("_execution_date", lit(execution_date)) \
            .withColumn("_row_number", row_number().over(Window.orderBy(lit(1))))
        
        return df_with_tech
    
    def _write_to_iceberg(self, df: DataFrame, table_config: TableConfig) -> None:
        """Write DataFrame to Iceberg table using Spark SQL"""
        # Write using Spark's Iceberg integration
        df.writeTo(table_config.identifier) \
          .option("write-audit-publish", "true") \
          .option("check-nullability", "false") \
          .append()
        
        self.logger.info(f"Successfully wrote data to {table_config.identifier}")
    
    def _handle_schema_evolution(self, table, table_config: TableConfig) -> None:
        """Handle schema evolution for existing table"""
        # This is a simplified version - in production you'd want more sophisticated logic
        current_schema = table.schema()
        config_schema = self._build_iceberg_schema(table_config)
        
        # For now, just log differences
        current_fields = {field.name for field in current_schema}
        config_fields = {field.name for field in config_schema}
        
        new_fields = config_fields - current_fields
        if new_fields:
            self.logger.info(f"New fields detected in config: {new_fields}")
            # In production: trigger schema evolution process
    
    def _move_to_dlq(self, file_path: str, error_message: str) -> None:
        """Move failed file to dead letter queue"""
        dlq_dir = self.temp_dir / "dlq"
        dlq_dir.mkdir(parents=True, exist_ok=True)
        
        file_name = Path(file_path).name
        dlq_path = dlq_dir / f"{datetime.now().isoformat()}_{file_name}"
        
        try:
            Path(file_path).rename(dlq_path)
            
            # Write error details
            error_file = dlq_path.with_suffix('.error')
            with open(error_file, 'w') as f:
                f.write(f"Error: {error_message}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Original file: {file_path}\n")
            
            self.logger.info(f"Moved failed file to DLQ: {dlq_path}")
        except Exception as e:
            self.logger.error(f"Failed to move file to DLQ: {str(e)}")
    
    def _cleanup_temp_files(self, file_paths: List[str]) -> None:
        """Clean up temporary extracted files"""
        for file_path in file_paths:
            try:
                Path(file_path).unlink()
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp file {file_path}: {str(e)}")

