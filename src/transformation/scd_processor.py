"""
SCD Type 2 processor for slowly changing dimensions.
"""

import hashlib
from typing import Dict, List, Optional, Any
from datetime import datetime
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, 
    concat_ws, sha2, md5, max as spark_max, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, BooleanType

from ..common.models.scd_config import SCDConfig, SCDOperation, SCDMetrics
from ..common.models.table_config import TableConfig
from ..common.monitoring.metrics import MetricsCollector

logger = structlog.get_logger(__name__)


class SCDProcessor:
    """Processor for SCD Type 2 operations."""
    
    def __init__(self, 
                 spark: SparkSession,
                 table_config: TableConfig,
                 scd_config: SCDConfig,
                 metrics_collector: Optional[MetricsCollector] = None):
        self.spark = spark
        self.table_config = table_config
        self.scd_config = scd_config
        self.metrics_collector = metrics_collector
        self.logger = logger.bind(
            processor="SCDProcessor",
            table=table_config.identifier
        )
    
    def process_scd(self, source_df: DataFrame, 
                   target_table: str,
                   batch_id: str,
                   processing_timestamp: Optional[datetime] = None) -> SCDMetrics:
        """
        Process SCD Type 2 operation.
        
        Args:
            source_df: Source DataFrame with new/updated data
            target_table: Target table identifier
            batch_id: Batch identifier for tracking
            processing_timestamp: Processing timestamp (defaults to current time)
            
        Returns:
            SCDMetrics with operation results
        """
        if processing_timestamp is None:
            processing_timestamp = datetime.now()
        
        operation = SCDOperation(
            source_table="temp_source",
            target_table=target_table,
            scd_config=self.scd_config,
            batch_id=batch_id,
            processing_timestamp=processing_timestamp
        )
        
        metrics = SCDMetrics(
            operation_id=f"{batch_id}_{int(processing_timestamp.timestamp())}",
            batch_id=batch_id,
            table_name=target_table,
            start_time=processing_timestamp
        )
        
        try:
            # Prepare source data
            prepared_source = self._prepare_source_data(source_df, processing_timestamp)
            metrics.source_row_count = prepared_source.count()
            
            # Load existing target data
            target_df = self._load_target_data(target_table)
            if target_df is not None:
                metrics.target_row_count_before = target_df.count()
            
            # Perform SCD merge
            if target_df is None:
                # Initial load - no existing data
                result_df = self._initial_load(prepared_source)
                metrics.new_records = metrics.source_row_count
            else:
                # Incremental load with SCD logic
                result_df = self._incremental_scd_merge(prepared_source, target_df, operation)
                
                # Calculate change metrics
                self._calculate_change_metrics(metrics, prepared_source, target_df, result_df)
            
            # Write result back to target table
            self._write_result(result_df, target_table, operation)
            
            # Collect final metrics
            final_target_df = self._load_target_data(target_table)
            if final_target_df is not None:
                metrics.target_row_count_after = final_target_df.count()
            
            metrics.end_time = datetime.now()
            metrics.calculate_derived_metrics()
            
            self.logger.info("SCD processing completed successfully",
                           batch_id=batch_id,
                           source_rows=metrics.source_row_count,
                           new_records=metrics.new_records,
                           updated_records=metrics.updated_records)
            
            return metrics
            
        except Exception as e:
            metrics.end_time = datetime.now()
            self.logger.error("SCD processing failed", 
                            batch_id=batch_id, 
                            error=str(e))
            raise
    
    def _prepare_source_data(self, source_df: DataFrame, 
                           processing_timestamp: datetime) -> DataFrame:
        """Prepare source data for SCD processing."""
        
        # Add hash column for change detection
        if self.scd_config.hash_column:
            hash_columns = self.scd_config.tracked_columns
            
            # Create hash of tracked columns
            if self.scd_config.hash_algorithm.value == "md5":
                hash_expr = md5(concat_ws("|", *[coalesce(col(c), lit("NULL")) for c in hash_columns]))
            else:  # SHA256 or SHA1
                bits = 256 if self.scd_config.hash_algorithm.value == "sha256" else 160
                hash_expr = sha2(concat_ws("|", *[coalesce(col(c), lit("NULL")) for c in hash_columns]), bits)
            
            source_df = source_df.withColumn(self.scd_config.hash_column, hash_expr)
        
        # Add effective timestamp
        if self.scd_config.use_source_timestamp and self.scd_config.effective_timestamp_column:
            # Use source timestamp
            source_df = source_df.withColumn(
                self.scd_config.valid_from_column,
                col(self.scd_config.effective_timestamp_column)
            )
        else:
            # Use processing timestamp
            source_df = source_df.withColumn(
                self.scd_config.valid_from_column,
                lit(processing_timestamp).cast(TimestampType())
            )
        
        # Add default SCD columns
        source_df = source_df.withColumn(
            self.scd_config.valid_to_column,
            lit(self.scd_config.default_valid_to).cast(TimestampType())
        )
        source_df = source_df.withColumn(
            self.scd_config.is_current_column,
            lit(True).cast(BooleanType())
        )
        
        # Validate business keys
        if self.scd_config.validate_business_keys:
            for key in self.scd_config.business_keys:
                source_df = source_df.filter(col(key).isNotNull())
        
        # Handle duplicates in source
        if not self.scd_config.allow_duplicate_business_keys:
            # Keep latest record for each business key
            window_spec = Window.partitionBy(*self.scd_config.business_keys).orderBy(
                col(self.scd_config.valid_from_column).desc()
            )
            source_df = source_df.withColumn("rn", row_number().over(window_spec))
            source_df = source_df.filter(col("rn") == 1).drop("rn")
        
        return source_df
    
    def _load_target_data(self, target_table: str) -> Optional[DataFrame]:
        """Load existing target data."""
        try:
            # Try to read from Iceberg table
            target_df = self.spark.read.format("iceberg").table(target_table)
            return target_df
        except Exception as e:
            self.logger.info("Target table does not exist, will create new table",
                           table=target_table, error=str(e))
            return None
    
    def _initial_load(self, source_df: DataFrame) -> DataFrame:
        """Handle initial load when target table doesn't exist."""
        self.logger.info("Performing initial load")
        return source_df
    
    def _incremental_scd_merge(self, source_df: DataFrame, 
                             target_df: DataFrame,
                             operation: SCDOperation) -> DataFrame:
        """Perform incremental SCD Type 2 merge."""
        
        # Get current records from target
        current_target = target_df.filter(col(self.scd_config.is_current_column) == True)
        
        # Join source with current target on business keys
        join_condition = [col(f"source.{key}") == col(f"target.{key}") 
                         for key in self.scd_config.business_keys]
        
        joined_df = source_df.alias("source").join(
            current_target.alias("target"),
            join_condition,
            "full_outer"
        )
        
        # Identify different types of records
        new_records = self._identify_new_records(joined_df)
        changed_records = self._identify_changed_records(joined_df)
        unchanged_records = self._identify_unchanged_records(joined_df)
        
        # Process each type
        result_parts = []
        
        # 1. Keep all historical records (not current)
        historical_records = target_df.filter(col(self.scd_config.is_current_column) == False)
        if historical_records.count() > 0:
            result_parts.append(historical_records)
        
        # 2. Add new records
        if new_records.count() > 0:
            new_records_final = self._prepare_new_records(new_records)
            result_parts.append(new_records_final)
        
        # 3. Handle changed records (close old, add new)
        if changed_records.count() > 0:
            closed_records, new_versions = self._handle_changed_records(changed_records, operation)
            result_parts.extend([closed_records, new_versions])
        
        # 4. Keep unchanged records as current
        if unchanged_records.count() > 0:
            unchanged_final = self._prepare_unchanged_records(unchanged_records)
            result_parts.append(unchanged_final)
        
        # Union all parts
        if result_parts:
            result_df = result_parts[0]
            for part in result_parts[1:]:
                result_df = result_df.union(part)
        else:
            # No data to process
            result_df = target_df
        
        return result_df
    
    def _identify_new_records(self, joined_df: DataFrame) -> DataFrame:
        """Identify new records (exist in source but not in target)."""
        return joined_df.filter(
            col("source").isNotNull() & col("target").isNull()
        ).select("source.*")
    
    def _identify_changed_records(self, joined_df: DataFrame) -> DataFrame:
        """Identify changed records."""
        if self.scd_config.hash_column:
            # Use hash comparison
            condition = (
                col("source").isNotNull() & 
                col("target").isNotNull() &
                (col(f"source.{self.scd_config.hash_column}") != 
                 col(f"target.{self.scd_config.hash_column}"))
            )
        else:
            # Column-by-column comparison
            conditions = []
            for tracked_col in self.scd_config.tracked_columns:
                if self.scd_config.ignore_null_changes:
                    condition = (
                        (col(f"source.{tracked_col}") != col(f"target.{tracked_col}")) &
                        col(f"source.{tracked_col}").isNotNull() &
                        col(f"target.{tracked_col}").isNotNull()
                    )
                else:
                    condition = col(f"source.{tracked_col}") != col(f"target.{tracked_col}")
                conditions.append(condition)
            
            condition = (
                col("source").isNotNull() & 
                col("target").isNotNull()
            )
            if conditions:
                from functools import reduce
                from pyspark.sql.functions import expr
                condition = condition & reduce(lambda a, b: a | b, conditions)
        
        return joined_df.filter(condition)
    
    def _identify_unchanged_records(self, joined_df: DataFrame) -> DataFrame:
        """Identify unchanged records."""
        if self.scd_config.hash_column:
            # Use hash comparison
            condition = (
                col("source").isNotNull() & 
                col("target").isNotNull() &
                (col(f"source.{self.scd_config.hash_column}") == 
                 col(f"target.{self.scd_config.hash_column}"))
            )
        else:
            # Column-by-column comparison (all columns match)
            conditions = []
            for tracked_col in self.scd_config.tracked_columns:
                condition = col(f"source.{tracked_col}") == col(f"target.{tracked_col}")
                conditions.append(condition)
            
            condition = (
                col("source").isNotNull() & 
                col("target").isNotNull()
            )
            if conditions:
                from functools import reduce
                condition = condition & reduce(lambda a, b: a & b, conditions)
        
        return joined_df.filter(condition)
    
    def _prepare_new_records(self, new_records: DataFrame) -> DataFrame:
        """Prepare new records for insertion."""
        return new_records
    
    def _prepare_unchanged_records(self, unchanged_records: DataFrame) -> DataFrame:
        """Prepare unchanged records (keep target version)."""
        return unchanged_records.select("target.*")
    
    def _handle_changed_records(self, changed_records: DataFrame, 
                              operation: SCDOperation) -> tuple[DataFrame, DataFrame]:
        """Handle changed records by closing old and creating new versions."""
        
        # Close old records (set valid_to and is_current = false)
        closed_records = changed_records.select("target.*").withColumn(
            self.scd_config.valid_to_column,
            lit(operation.processing_timestamp).cast(TimestampType())
        ).withColumn(
            self.scd_config.is_current_column,
            lit(False).cast(BooleanType())
        )
        
        # Create new versions
        new_versions = changed_records.select("source.*")
        
        return closed_records, new_versions
    
    def _calculate_change_metrics(self, metrics: SCDMetrics,
                                source_df: DataFrame,
                                target_df: DataFrame,
                                result_df: DataFrame):
        """Calculate change metrics for the operation."""
        # This is a simplified calculation
        # In practice, you might want to track these during the merge process
        
        current_target_count = target_df.filter(
            col(self.scd_config.is_current_column) == True
        ).count()
        
        final_current_count = result_df.filter(
            col(self.scd_config.is_current_column) == True
        ).count()
        
        # Estimate metrics (simplified)
        metrics.new_records = max(0, final_current_count - current_target_count)
        metrics.updated_records = metrics.source_row_count - metrics.new_records
        metrics.unchanged_records = current_target_count - metrics.updated_records
    
    def _write_result(self, result_df: DataFrame, 
                     target_table: str,
                     operation: SCDOperation):
        """Write result DataFrame to target table."""
        
        # Write using Iceberg format
        writer = result_df.write.format("iceberg").mode("overwrite")
        
        # Add table properties for optimization
        if self.scd_config.enable_clustering:
            clustering_cols = self.scd_config.get_clustering_columns()
            # Note: Clustering configuration would be set at table creation time
            # This is just for reference
        
        writer.saveAsTable(target_table)
        
        # Post-write optimizations
        if operation.optimize_after_merge:
            self._optimize_table(target_table)
        
        if operation.analyze_after_merge:
            self._analyze_table(target_table)
    
    def _optimize_table(self, table_name: str):
        """Optimize table after merge."""
        try:
            # Run Iceberg table optimization
            self.spark.sql(f"CALL system.rewrite_data_files('{table_name}')")
            self.logger.info("Table optimization completed", table=table_name)
        except Exception as e:
            self.logger.warning("Table optimization failed", 
                              table=table_name, error=str(e))
    
    def _analyze_table(self, table_name: str):
        """Analyze table statistics."""
        try:
            self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
            self.logger.info("Table analysis completed", table=table_name)
        except Exception as e:
            self.logger.warning("Table analysis failed", 
                              table=table_name, error=str(e)) 