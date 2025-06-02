"""
Raw data loader for loading extracted data into the raw layer.
Supports Apache Iceberg tables with SCD Type 2 functionality, ZIP file processing, and comprehensive validation.
"""

import os
import zipfile
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import structlog
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    lit, current_timestamp, col, when, isnan, isnull, hash, concat_ws, input_file_name
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError
    ICEBERG_AVAILABLE = True
except ImportError:
    ICEBERG_AVAILABLE = False

from ..common.models import TableConfiguration
from ..common.exceptions import LoadError, ValidationError
from ..transformation.scd_processor import SCDProcessor

logger = structlog.get_logger(__name__)


class SchemaEvolutionError(Exception):
    """Raised when schema evolution cannot be handled"""
    pass


class RawDataLoader:
    """
    Comprehensive raw data loader for loading data into the raw layer with Iceberg support.
    
    Features:
    - DataFrame-based loading with SCD Type 2 functionality
    - ZIP file extraction and processing
    - CSV parsing with configurable delimiters
    - Schema enforcement and evolution
    - Technical metadata columns addition
    - Iceberg table creation and data writing
    - Comprehensive input validation
    - Error handling and dead letter queue
    """
    
    def __init__(self, spark: SparkSession, table_config: TableConfiguration = None,
                 metrics_collector: Optional[Any] = None, iceberg_catalog_name: str = None,
                 warehouse_path: str = None, temp_dir: str = "/tmp/etl"):
        """
        Initialize raw data loader.
        
        Args:
            spark: Spark session
            table_config: Table configuration (required for DataFrame-based operations)
            metrics_collector: Optional metrics collector
            iceberg_catalog_name: Name of Iceberg catalog (for ZIP-based operations)
            warehouse_path: Path to Iceberg warehouse (for ZIP-based operations)
            temp_dir: Temporary directory for file extraction
        """
        self.spark = spark
        self.table_config = table_config
        self.metrics_collector = metrics_collector
        self.iceberg_catalog_name = iceberg_catalog_name
        self.warehouse_path = warehouse_path
        
        # Setup temporary directory
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Iceberg catalog if parameters provided
        self.catalog = None
        if iceberg_catalog_name and warehouse_path and ICEBERG_AVAILABLE:
            try:
                self.catalog = load_catalog(
                    name=iceberg_catalog_name,
                    **{
                        "type": "rest",
                        "uri": f"http://nessie:19120/api/v1",
                        "warehouse": warehouse_path
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to initialize Iceberg catalog: {str(e)}")
        
        # Initialize logger
        if table_config:
            self.logger = logger.bind(table_id=table_config.table_id)
        else:
            self.logger = logger
        
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

    # DataFrame-based loading methods
    def load_data(self, df: DataFrame, target_table: str, write_mode: str = "append",
                  enable_scd: bool = False, **kwargs) -> Dict[str, Any]:
        """
        Load DataFrame into raw layer table.
        
        Args:
            df: DataFrame to load
            target_table: Target table name
            write_mode: Write mode (append, overwrite, merge)
            enable_scd: Enable SCD Type 2 processing
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with load metrics and metadata
        """
        if not self.table_config:
            raise ValidationError("Table configuration required for DataFrame-based loading")
            
        try:
            self.logger.info("Starting raw data load",
                           target_table=target_table,
                           write_mode=write_mode,
                           enable_scd=enable_scd,
                           record_count=df.count())
            
            # Validate input data
            self._validate_input_data(df)
            
            # Prepare data for loading
            prepared_df = self._prepare_data_for_load(df)
            
            # Load data based on mode
            if enable_scd and write_mode == "append":
                load_result = self._load_with_scd(prepared_df, target_table, **kwargs)
            elif write_mode == "merge":
                load_result = self._load_with_merge(prepared_df, target_table, **kwargs)
            else:
                load_result = self._load_direct(prepared_df, target_table, write_mode, **kwargs)
            
            # Collect metrics
            metrics = self._collect_load_metrics(load_result, target_table)
            
            # Optimize table if needed
            if kwargs.get('optimize_after_load', True):
                self._optimize_table(target_table)
            
            self.logger.info("Raw data load completed successfully",
                           target_table=target_table,
                           records_loaded=load_result.get('records_loaded', 0))
            
            return metrics
            
        except Exception as e:
            self.logger.error("Raw data load failed",
                            target_table=target_table,
                            error=str(e))
            raise LoadError(f"Failed to load data to {target_table}: {str(e)}") from e

    # ZIP-based processing methods
    def process_zip(self, zip_path: str, table_config: TableConfiguration, 
                   execution_date: str = None) -> Dict[str, Any]:
        """
        Process ZIP file containing CSV files and load to Iceberg.
        
        Args:
            zip_path: Path to ZIP file containing CSV files
            table_config: Table configuration with schema and rules
            execution_date: Execution date for partitioning
            
        Returns:
            Dict with processing results and statistics
        """
        if execution_date is None:
            execution_date = datetime.now().isoformat()
            
        self.logger.info(f"Starting ZIP processing for {table_config.table_id}")
        self.logger.info(f"ZIP file: {zip_path}, Execution date: {execution_date}")
        
        results = {
            "table_name": table_config.table_id,
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
            if not self._validate_zip_input(zip_path):
                raise ValueError(f"Invalid ZIP file: {zip_path}")
            
            # Step 2: Extract ZIP file
            extracted_files = self._extract_zip_file(zip_path, execution_date)
            self.logger.info(f"Extracted {len(extracted_files)} CSV files")
            
            # Step 3: Ensure Iceberg table exists
            if self.catalog:
                self._ensure_iceberg_table_exists(table_config)
            
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
            error_msg = f"ZIP processing failed: {str(e)}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            results["success"] = False
        
        finally:
            processing_time = (datetime.now() - start_time).total_seconds()
            results["processing_time_seconds"] = processing_time
            
            self.logger.info(f"ZIP processing completed. Success: {results['success']}")
            self.logger.info(f"Files processed: {results['files_processed']}, "
                           f"Rows loaded: {results['rows_loaded']}, "
                           f"Processing time: {processing_time:.2f}s")
        
        return results

    # Validation methods
    def _validate_input_data(self, df: DataFrame):
        """
        Validate input DataFrame.
        
        Args:
            df: DataFrame to validate
        """
        if df is None:
            raise ValidationError("Input DataFrame is None")
        
        if df.count() == 0:
            self.logger.warning("Input DataFrame is empty")
            return
        
        # Check for required columns
        required_columns = [col['name'] for col in self.table_config.columns 
                          if not col.get('nullable', True)]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValidationError(f"Missing required columns: {missing_columns}")
        
        # Check for data quality issues
        self._check_data_quality(df)
        
        self.logger.debug("Input data validation passed",
                         column_count=len(df.columns),
                         record_count=df.count())

    def _validate_zip_input(self, zip_path: str) -> bool:
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

    def _check_data_quality(self, df: DataFrame):
        """
        Perform basic data quality checks.
        
        Args:
            df: DataFrame to check
        """
        # Check for completely null rows
        all_columns = df.columns
        null_condition = None
        
        for col_name in all_columns:
            if null_condition is None:
                null_condition = col(col_name).isNull()
            else:
                null_condition = null_condition & col(col_name).isNull()
        
        null_rows = df.filter(null_condition).count()
        if null_rows > 0:
            self.logger.warning("Found completely null rows", null_row_count=null_rows)
        
        # Check for duplicate business keys if defined
        business_keys = self.table_config.business_keys
        if business_keys:
            total_rows = df.count()
            distinct_rows = df.select(*business_keys).distinct().count()
            
            if total_rows != distinct_rows:
                duplicate_count = total_rows - distinct_rows
                self.logger.warning("Found duplicate business keys",
                                  duplicate_count=duplicate_count,
                                  business_keys=business_keys)
    
    def _prepare_data_for_load(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for loading by adding technical columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with technical columns added
        """
        prepared_df = df
        
        # Add load timestamp if not exists
        if '_load_timestamp' not in prepared_df.columns:
            prepared_df = prepared_df.withColumn('_load_timestamp', current_timestamp())
        
        # Add batch ID if not exists
        if '_batch_id' not in prepared_df.columns:
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            prepared_df = prepared_df.withColumn('_batch_id', lit(batch_id))
        
        # Add record hash for change detection
        if '_record_hash' not in prepared_df.columns:
            prepared_df = self._add_record_hash(prepared_df)
        
        # Handle null values
        prepared_df = self._handle_null_values(prepared_df)
        
        self.logger.debug("Data prepared for load",
                         original_columns=len(df.columns),
                         prepared_columns=len(prepared_df.columns))
        
        return prepared_df
    
    def _add_record_hash(self, df: DataFrame) -> DataFrame:
        """
        Add record hash for change detection.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with record hash column
        """
        # Get business columns (exclude technical columns)
        business_columns = [col['name'] for col in self.table_config.columns
                          if not col['name'].startswith('_')]
        
        # Create hash from business columns
        hash_expr = hash(concat_ws('|', *[col(c) for c in business_columns if c in df.columns]))
        
        return df.withColumn('_record_hash', hash_expr)
    
    def _handle_null_values(self, df: DataFrame) -> DataFrame:
        """
        Handle null values according to column configuration.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with null values handled
        """
        handled_df = df
        
        for column_config in self.table_config.columns:
            col_name = column_config['name']
            
            if col_name not in df.columns:
                continue
            
            # Handle non-nullable columns
            if not column_config.get('nullable', True):
                default_value = column_config.get('default_value')
                if default_value is not None:
                    handled_df = handled_df.withColumn(
                        col_name,
                        when(col(col_name).isNull(), lit(default_value)).otherwise(col(col_name))
                    )
        
        return handled_df
    
    def _load_with_scd(self, df: DataFrame, target_table: str, **kwargs) -> Dict[str, Any]:
        """
        Load data with SCD Type 2 processing.
        
        Args:
            df: DataFrame to load
            target_table: Target table name
            **kwargs: Additional parameters
            
        Returns:
            Load result metrics
        """
        try:
            # Check if SCD is enabled in table configuration
            if not self.table_config.enable_scd:
                raise ValidationError("SCD is not enabled for this table configuration")
            
            # Get or create SCD configuration
            scd_config = getattr(self.table_config, 'scd_config', None)
            if not scd_config:
                # Create default SCD config if not present
                from ..common.models.scd_config import SCDConfig, SCDStrategy, HashAlgorithm
                
                scd_config = SCDConfig(
                    business_keys=self.table_config.business_keys,
                    tracked_columns=getattr(self.table_config, 'scd_columns', []),
                    valid_from_column='valid_from',
                    valid_to_column='valid_to',
                    is_current_column='is_current',
                    hash_column='row_hash',
                    strategy=SCDStrategy.MERGE_INTO,
                    hash_algorithm=HashAlgorithm.SHA256
                )
            
            # Initialize SCD processor
            scd_processor = SCDProcessor(
                spark=self.spark,
                table_config=self.table_config,
                scd_config=scd_config,
                metrics_collector=self.metrics_collector
            )
            
            # Generate batch ID for tracking
            batch_id = kwargs.get('batch_id', f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            
            # Process SCD
            scd_metrics = scd_processor.process_scd(
                source_df=df, 
                target_table=target_table,
                batch_id=batch_id,
                processing_timestamp=kwargs.get('processing_timestamp')
            )
            
            return {
                'records_loaded': scd_metrics.source_row_count,
                'new_records': scd_metrics.new_records,
                'updated_records': scd_metrics.updated_records,
                'unchanged_records': scd_metrics.unchanged_records,
                'load_type': 'scd_type2',
                'batch_id': batch_id,
                'scd_metrics': scd_metrics
            }
            
        except Exception as e:
            raise LoadError(f"SCD load failed: {str(e)}") from e
    
    def _load_with_merge(self, df: DataFrame, target_table: str, **kwargs) -> Dict[str, Any]:
        """
        Load data using merge operation.
        
        Args:
            df: DataFrame to load
            target_table: Target table name
            **kwargs: Additional parameters
            
        Returns:
            Load result metrics
        """
        try:
            # Get merge configuration
            merge_keys = kwargs.get('merge_keys', self.table_config.business_keys)
            if not merge_keys:
                raise ValidationError("Merge keys not specified for merge load")
            
            # Create temporary view for source data
            temp_view = f"temp_source_{target_table.replace('.', '_')}"
            df.createOrReplaceTempView(temp_view)
            
            # Build merge SQL
            merge_sql = self._build_merge_sql(target_table, temp_view, merge_keys, **kwargs)
            
            # Execute merge
            self.spark.sql(merge_sql)
            
            # Get metrics (simplified for now)
            record_count = df.count()
            
            return {
                'records_loaded': record_count,
                'load_type': 'merge'
            }
            
        except Exception as e:
            raise LoadError(f"Merge load failed: {str(e)}") from e
    
    def _load_direct(self, df: DataFrame, target_table: str, write_mode: str, **kwargs) -> Dict[str, Any]:
        """
        Load data directly without SCD or merge.
        
        Args:
            df: DataFrame to load
            target_table: Target table name
            write_mode: Write mode (append, overwrite)
            **kwargs: Additional parameters
            
        Returns:
            Load result metrics
        """
        try:
            # Configure write options
            writer = df.write.format("iceberg")
            
            if write_mode == "overwrite":
                writer = writer.mode("overwrite")
            else:
                writer = writer.mode("append")
            
            # Add partition configuration if specified
            partition_spec = self.table_config.partition_spec
            if partition_spec and partition_spec.get('columns'):
                partition_columns = partition_spec['columns']
                writer = writer.partitionBy(*partition_columns)
            
            # Write data
            writer.saveAsTable(target_table)
            
            record_count = df.count()
            
            return {
                'records_loaded': record_count,
                'load_type': 'direct',
                'write_mode': write_mode
            }
            
        except Exception as e:
            raise LoadError(f"Direct load failed: {str(e)}") from e
    
    def _build_merge_sql(self, target_table: str, source_view: str, 
                        merge_keys: List[str], **kwargs) -> str:
        """
        Build SQL for merge operation.
        
        Args:
            target_table: Target table name
            source_view: Source view name
            merge_keys: Keys for merge condition
            **kwargs: Additional parameters
            
        Returns:
            Merge SQL statement
        """
        # Build merge condition
        merge_conditions = [f"target.{key} = source.{key}" for key in merge_keys]
        merge_condition = " AND ".join(merge_conditions)
        
        # Get all columns except technical ones for update
        update_columns = [col['name'] for col in self.table_config.columns
                         if not col['name'].startswith('_') and col['name'] not in merge_keys]
        
        # Build update set clause
        update_sets = [f"target.{col} = source.{col}" for col in update_columns]
        update_sets.append("target._load_timestamp = source._load_timestamp")
        update_set = ", ".join(update_sets)
        
        # Build insert values
        all_columns = [col['name'] for col in self.table_config.columns]
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"source.{col}" for col in all_columns])
        
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
        
        return merge_sql
    
    def _collect_load_metrics(self, load_result: Dict[str, Any], target_table: str) -> Dict[str, Any]:
        """
        Collect load metrics.
        
        Args:
            load_result: Load result from operation
            target_table: Target table name
            
        Returns:
            Comprehensive metrics dictionary
        """
        metrics = {
            'table_name': target_table,
            'load_timestamp': datetime.now().isoformat(),
            'load_type': load_result.get('load_type', 'unknown'),
            'records_loaded': load_result.get('records_loaded', 0),
            'status': 'success'
        }
        
        # Add SCD-specific metrics if available
        if 'new_records' in load_result:
            metrics.update({
                'new_records': load_result['new_records'],
                'updated_records': load_result['updated_records'],
                'unchanged_records': load_result['unchanged_records']
            })
        
        # Add write mode if available
        if 'write_mode' in load_result:
            metrics['write_mode'] = load_result['write_mode']
        
        # Send metrics to collector if available
        if self.metrics_collector:
            self.metrics_collector.record_load_metrics(metrics)
        
        return metrics
    
    def _optimize_table(self, target_table: str):
        """
        Optimize table after load.
        
        Args:
            target_table: Target table name
        """
        try:
            # Run table optimization
            self.spark.sql(f"CALL system.rewrite_data_files(table => '{target_table}')")
            
            # Update table statistics
            self.spark.sql(f"ANALYZE TABLE {target_table} COMPUTE STATISTICS")
            
            self.logger.info("Table optimization completed", target_table=target_table)
            
        except Exception as e:
            self.logger.warning("Table optimization failed",
                              target_table=target_table,
                              error=str(e))
    
    def create_table_if_not_exists(self, target_table: str) -> bool:
        """
        Create table if it doesn't exist.
        
        Args:
            target_table: Target table name
            
        Returns:
            True if table was created, False if it already existed
        """
        try:
            # Check if table exists
            if self._table_exists(target_table):
                self.logger.debug("Table already exists", target_table=target_table)
                return False
            
            # Build CREATE TABLE SQL
            create_sql = self._build_create_table_sql(target_table)
            
            # Execute CREATE TABLE
            self.spark.sql(create_sql)
            
            self.logger.info("Table created successfully", target_table=target_table)
            return True
            
        except Exception as e:
            self.logger.error("Failed to create table",
                            target_table=target_table,
                            error=str(e))
            raise LoadError(f"Failed to create table {target_table}: {str(e)}") from e
    
    def _table_exists(self, target_table: str) -> bool:
        """
        Check if table exists.
        
        Args:
            target_table: Target table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE TABLE {target_table}")
            return True
        except Exception:
            return False
    
    def _build_create_table_sql(self, target_table: str) -> str:
        """
        Build CREATE TABLE SQL statement with comprehensive Iceberg properties.
        
        Args:
            target_table: Target table name
            
        Returns:
            CREATE TABLE SQL statement
        """
        # Build column definitions
        column_defs = []
        
        for column_config in self.table_config.columns:
            name = column_config['name']
            data_type = column_config['data_type']
            nullable = column_config.get('nullable', True)
            
            # Map data types to Iceberg/Spark SQL types
            if data_type.lower() in ['int', 'integer']:
                sql_type = 'INT'
            elif data_type.lower() in ['double', 'float']:
                sql_type = 'DOUBLE'
            elif data_type.lower() == 'decimal':
                precision = column_config.get('precision', 10)
                scale = column_config.get('scale', 2)
                sql_type = f'DECIMAL({precision},{scale})'
            elif data_type.lower() == 'boolean':
                sql_type = 'BOOLEAN'
            elif data_type.lower() == 'date':
                sql_type = 'DATE'
            elif data_type.lower() in ['timestamp', 'datetime']:
                sql_type = 'TIMESTAMP'
            else:
                sql_type = 'STRING'
            
            null_constraint = '' if nullable else ' NOT NULL'
            column_defs.append(f"{name} {sql_type}{null_constraint}")
        
        columns_sql = ",\n    ".join(column_defs)
        
        # Build partition clause
        partition_clause = ""
        partition_spec = self.table_config.partition_spec
        if partition_spec and partition_spec.get('columns'):
            partition_columns = partition_spec['columns']
            partition_clause = f"\nPARTITIONED BY ({', '.join(partition_columns)})"
        
        # Build table properties from configuration
        if hasattr(self.table_config, 'iceberg_properties'):
            # Use configured Iceberg properties
            iceberg_props = self.table_config.iceberg_properties.to_spark_properties()
            properties = [f"'{k}' = '{v}'" for k, v in iceberg_props.items()]
        else:
            # Fallback to default properties
            properties = [
                "'format-version' = '2'",
                "'write.delete.mode' = 'merge-on-read'",
                "'write.update.mode' = 'merge-on-read'",
                "'write.merge.mode' = 'merge-on-read'",
                "'write.target-file-size-bytes' = '134217728'",
                "'history.expire.max-snapshot-age-ms' = '432000000'"
            ]
        
        properties_clause = f"\nTBLPROPERTIES (\n    {','.join(properties)}\n)"
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {columns_sql}
        ) USING ICEBERG{partition_clause}{properties_clause}
        """
        
        return create_sql

    # ZIP processing implementation methods
    def _extract_zip_file(self, zip_path: str, execution_date: str) -> List[str]:
        """Extract ZIP file to temporary directory"""
        extract_dir = self.temp_dir / f"extract_{execution_date.replace(':', '-')}"
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        extracted_files = []
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for file_info in zf.infolist():
                if file_info.filename.endswith('.csv'):
                    # Extract the file
                    extracted_path = zf.extract(file_info, extract_dir)
                    extracted_files.append(extracted_path)
                    self.logger.debug(f"Extracted: {file_info.filename} -> {extracted_path}")
        
        return extracted_files

    def _ensure_iceberg_table_exists(self, table_config: TableConfiguration) -> None:
        """Ensure Iceberg table exists, create if not"""
        try:
            table_name = table_config.table_id
            namespace = table_config.database or "default"
            
            # Check if table exists
            try:
                table = self.catalog.load_table((namespace, table_name))
                self.logger.info(f"Table {namespace}.{table_name} already exists")
            except NoSuchTableError:
                # Create table
                self._create_iceberg_table(table_config)
                self.logger.info(f"Created Iceberg table: {namespace}.{table_name}")
                
        except Exception as e:
            raise SchemaEvolutionError(f"Failed to ensure table exists: {str(e)}")

    def _create_iceberg_table(self, table_config: TableConfiguration) -> None:
        """Create new Iceberg table"""
        try:
            table_name = table_config.table_id
            namespace = table_config.database or "default"
            
            # Build schema
            schema = self._build_iceberg_schema(table_config)
            
            # Create table
            table = self.catalog.create_table(
                identifier=(namespace, table_name),
                schema=schema
            )
            
            self.logger.info(f"Created Iceberg table: {namespace}.{table_name}")
            
        except Exception as e:
            raise SchemaEvolutionError(f"Failed to create Iceberg table: {str(e)}")

    def _build_iceberg_schema(self, table_config: TableConfiguration) -> pa.Schema:
        """Build PyArrow schema for Iceberg table"""
        fields = []
        
        for column in table_config.columns:
            name = column['name']
            data_type = column['data_type']
            nullable = column.get('nullable', True)
            
            pa_type = self._spark_type_to_pyarrow(data_type)
            
            field = pa.field(name, pa_type, nullable=nullable)
            fields.append(field)
        
        # Add technical columns
        fields.extend([
            pa.field('_load_timestamp', pa.timestamp('us'), nullable=False),
            pa.field('_batch_id', pa.string(), nullable=False),
            pa.field('_source_file', pa.string(), nullable=True)
        ])
        
        return pa.schema(fields)

    def _spark_type_to_pyarrow(self, spark_type: str) -> pa.DataType:
        """Convert Spark SQL type to PyArrow type"""
        type_map = {
            'string': pa.string(),
            'int': pa.int32(),
            'integer': pa.int32(),
            'long': pa.int64(),
            'double': pa.float64(),
            'float': pa.float32(),
            'boolean': pa.bool_(),
            'timestamp': pa.timestamp('us'),
            'date': pa.date32()
        }
        
        return type_map.get(spark_type.lower(), pa.string())

    def _process_csv_file(self, csv_path: str, table_config: TableConfiguration, 
                         execution_date: str) -> int:
        """Process single CSV file and load to Iceberg"""
        try:
            self.logger.info(f"Processing CSV file: {csv_path}")
            
            # Read CSV with schema enforcement
            df = self._read_csv_with_schema_enforcement(csv_path, table_config)
            
            if df.count() == 0:
                self.logger.warning(f"Empty CSV file: {csv_path}")
                return 0
            
            # Add technical columns
            df = self._add_technical_columns_zip(df, csv_path, execution_date)
            
            # Write to Iceberg
            self._write_to_iceberg(df, table_config)
            
            record_count = df.count()
            self.logger.info(f"Successfully processed {csv_path}: {record_count} records")
            
            return record_count
            
        except Exception as e:
            raise LoadError(f"Failed to process CSV file {csv_path}: {str(e)}")

    def _read_csv_with_schema_enforcement(self, csv_path: str, 
                                        table_config: TableConfiguration) -> DataFrame:
        """Read CSV file with schema enforcement"""
        try:
            # Try with defined schema first
            schema = self._build_spark_schema(table_config)
            
            df = self.spark.read \
                .format("csv") \
                .options(**self.csv_options) \
                .schema(schema) \
                .load(csv_path)
            
            return df
            
        except Exception as e:
            self.logger.warning(f"Schema enforcement failed for {csv_path}, trying fallback: {str(e)}")
            return self._read_csv_with_fallback(csv_path, table_config)

    def _read_csv_with_fallback(self, csv_path: str, table_config: TableConfiguration) -> DataFrame:
        """Read CSV with fallback (infer schema then cast)"""
        df = self.spark.read \
            .format("csv") \
            .options(**self.csv_options) \
            .option("inferSchema", "true") \
            .load(csv_path)
        
        # Cast to expected types where possible
        # This is a simplified implementation
        return df

    def _build_spark_schema(self, table_config: TableConfiguration) -> StructType:
        """Build Spark SQL schema from table configuration"""
        fields = []
        for column in table_config.columns:
            spark_type = self._get_spark_type(column['data_type'])
            field = StructField(column['name'], spark_type, column.get('nullable', True))
            fields.append(field)
        
        return StructType(fields)

    def _get_spark_type(self, type_string: str):
        """Convert type string to Spark SQL type"""
        from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType, DateType
        
        type_map = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType()
        }
        
        return type_map.get(type_string.lower(), StringType())

    def _add_technical_columns_zip(self, df: DataFrame, source_file: str, 
                             execution_date: str) -> DataFrame:
        """Add technical columns for ZIP processing"""
        return df \
            .withColumn('_load_timestamp', current_timestamp()) \
            .withColumn('_batch_id', lit(execution_date.replace(':', '-'))) \
            .withColumn('_source_file', lit(os.path.basename(source_file)))

    def _write_to_iceberg(self, df: DataFrame, table_config: TableConfiguration) -> None:
        """Write DataFrame to Iceberg table"""
        try:
            table_name = f"{table_config.database or 'default'}.{table_config.table_id}"
            
            df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(table_name)
                
        except Exception as e:
            raise LoadError(f"Failed to write to Iceberg table: {str(e)}")

    def _handle_schema_evolution(self, table, table_config: TableConfiguration) -> None:
        """Handle schema evolution for Iceberg table"""
        try:
            # This is a placeholder for schema evolution logic
            # In practice, you would compare current schema with target schema
            # and perform necessary ALTER TABLE operations
            
            current_schema = table.schema()
            target_schema = self._build_iceberg_schema(table_config)
            
            # Compare schemas and evolve if needed
            # Implementation depends on specific requirements
            
        except Exception as e:
            self.logger.warning(f"Schema evolution failed: {str(e)}")

    def _move_to_dlq(self, file_path: str, error_message: str) -> None:
        """Move file to dead letter queue"""
        try:
            dlq_dir = self.temp_dir / "dlq"
            dlq_dir.mkdir(parents=True, exist_ok=True)
            
            source_path = Path(file_path)
            dlq_path = dlq_dir / f"{source_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{source_path.suffix}"
            
            # Move file
            import shutil
            shutil.move(file_path, dlq_path)
            
            # Write error log
            error_log_path = dlq_path.with_suffix('.error')
            with open(error_log_path, 'w') as f:
                f.write(f"Error: {error_message}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Original file: {file_path}\n")
            
            self.logger.info(f"Moved file to DLQ: {dlq_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to move file to DLQ: {str(e)}")

    def _cleanup_temp_files(self, file_paths: List[str]) -> None:
        """Cleanup temporary files"""
        for file_path in file_paths:
            try:
                Path(file_path).unlink(missing_ok=True)
                self.logger.debug(f"Cleaned up: {file_path}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup {file_path}: {str(e)}") 