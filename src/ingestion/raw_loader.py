"""
Raw data loader for loading extracted data into the raw layer.
Supports Apache Iceberg tables with SCD Type 2 functionality.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, when, isnan, isnull

from ..common.models import TableConfiguration
from ..common.exceptions import LoadError, ValidationError
from ..transformation.scd_processor import SCDProcessor

logger = structlog.get_logger(__name__)


class RawDataLoader:
    """
    Raw data loader for loading data into the raw layer with Iceberg support.
    Handles both initial loads and incremental updates with SCD Type 2.
    """
    
    def __init__(self, spark: SparkSession, table_config: TableConfiguration,
                 metrics_collector: Optional[Any] = None):
        """
        Initialize raw data loader.
        
        Args:
            spark: Spark session
            table_config: Table configuration
            metrics_collector: Optional metrics collector
        """
        self.spark = spark
        self.table_config = table_config
        self.metrics_collector = metrics_collector
        self.logger = logger.bind(table_id=table_config.table_id)
        
    def load_data(self, df: DataFrame, target_table: str, write_mode: str = "append",
                  enable_scd: bool = False, **kwargs) -> Dict[str, Any]:
        """
        Load data into raw layer table.
        
        Args:
            df: DataFrame to load
            target_table: Target table name
            write_mode: Write mode (append, overwrite, merge)
            enable_scd: Enable SCD Type 2 processing
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with load metrics and metadata
        """
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
        from pyspark.sql.functions import hash, concat_ws
        
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
            # Get SCD configuration
            scd_config = self.table_config.scd_config
            if not scd_config:
                raise ValidationError("SCD configuration not found for SCD-enabled load")
            
            # Initialize SCD processor
            scd_processor = SCDProcessor(
                spark=self.spark,
                table_config=self.table_config,
                scd_config=scd_config,
                metrics_collector=self.metrics_collector
            )
            
            # Process SCD
            scd_result = scd_processor.process_scd(df, target_table)
            
            return {
                'records_loaded': scd_result.get('total_records', 0),
                'new_records': scd_result.get('new_records', 0),
                'updated_records': scd_result.get('updated_records', 0),
                'unchanged_records': scd_result.get('unchanged_records', 0),
                'load_type': 'scd_type2'
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
        Build CREATE TABLE SQL statement.
        
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
        
        # Build table properties
        properties = [
            "'format-version' = '2'",
            "'write.delete.mode' = 'merge-on-read'",
            "'write.update.mode' = 'merge-on-read'",
            "'write.merge.mode' = 'merge-on-read'"
        ]
        
        properties_clause = f"\nTBLPROPERTIES (\n    {','.join(properties)}\n)"
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {columns_sql}
        ) USING ICEBERG{partition_clause}{properties_clause}
        """
        
        return create_sql 