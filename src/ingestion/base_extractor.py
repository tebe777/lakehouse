"""
Base extractor class for data ingestion framework.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Iterator
from datetime import datetime
import structlog
from pyspark.sql import SparkSession, DataFrame

from ..common.models.table_config import TableConfig
from ..common.monitoring.metrics import MetricsCollector

logger = structlog.get_logger(__name__)


class ExtractionResult:
    """Result of data extraction operation."""
    
    def __init__(self, 
                 success: bool,
                 data: Optional[DataFrame] = None,
                 row_count: int = 0,
                 file_path: Optional[str] = None,
                 metadata: Optional[Dict[str, Any]] = None,
                 errors: Optional[List[str]] = None):
        self.success = success
        self.data = data
        self.row_count = row_count
        self.file_path = file_path
        self.metadata = metadata or {}
        self.errors = errors or []
        self.extraction_timestamp = datetime.now()


class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""
    
    def __init__(self, 
                 spark: SparkSession,
                 table_config: TableConfig,
                 metrics_collector: Optional[MetricsCollector] = None):
        self.spark = spark
        self.table_config = table_config
        self.metrics_collector = metrics_collector
        self.logger = logger.bind(
            extractor=self.__class__.__name__,
            table=table_config.identifier
        )
    
    @abstractmethod
    def extract(self, source_path: str, **kwargs) -> ExtractionResult:
        """
        Extract data from source.
        
        Args:
            source_path: Path to source data
            **kwargs: Additional extraction parameters
            
        Returns:
            ExtractionResult with extracted data
        """
        pass
    
    @abstractmethod
    def validate_source(self, source_path: str) -> bool:
        """
        Validate if source is accessible and readable.
        
        Args:
            source_path: Path to source data
            
        Returns:
            True if source is valid, False otherwise
        """
        pass
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported file formats."""
        return []
    
    def get_schema_mapping(self) -> Dict[str, str]:
        """Get mapping from source schema to target schema."""
        mapping = {}
        for column in self.table_config.columns:
            source_col = column.source_column or column.name
            mapping[source_col] = column.name
        return mapping
    
    def apply_schema_mapping(self, df: DataFrame) -> DataFrame:
        """Apply schema mapping to DataFrame."""
        mapping = self.get_schema_mapping()
        
        # Rename columns according to mapping
        for source_col, target_col in mapping.items():
            if source_col in df.columns and source_col != target_col:
                df = df.withColumnRenamed(source_col, target_col)
        
        return df
    
    def add_technical_columns(self, df: DataFrame, 
                            source_file: str,
                            batch_id: str) -> DataFrame:
        """Add technical columns to DataFrame."""
        from pyspark.sql.functions import lit, current_timestamp
        
        # Add technical columns
        df = df.withColumn("ingested_at", current_timestamp())
        df = df.withColumn("source_file", lit(source_file))
        df = df.withColumn("batch_id", lit(batch_id))
        
        # Add SCD columns if enabled
        if self.table_config.enable_scd:
            df = df.withColumn("valid_from", current_timestamp())
            df = df.withColumn("valid_to", lit(None).cast("timestamp"))
            df = df.withColumn("is_current", lit(True))
        
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> List[str]:
        """Validate extracted data against table configuration."""
        errors = []
        
        # Check required columns exist
        required_columns = [col.name for col in self.table_config.columns if not col.nullable]
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check data types (basic validation)
        for column in self.table_config.columns:
            if column.name in df.columns:
                df_type = dict(df.dtypes)[column.name]
                expected_type = self._spark_type_mapping(column.data_type.value)
                if not self._types_compatible(df_type, expected_type):
                    errors.append(f"Column {column.name} has type {df_type}, expected {expected_type}")
        
        return errors
    
    def _spark_type_mapping(self, iceberg_type: str) -> str:
        """Map Iceberg types to Spark types."""
        mapping = {
            "string": "string",
            "long": "bigint", 
            "integer": "int",
            "double": "double",
            "float": "float",
            "boolean": "boolean",
            "timestamp": "timestamp",
            "date": "date",
            "binary": "binary"
        }
        return mapping.get(iceberg_type, "string")
    
    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if data types are compatible."""
        # Simplified type compatibility check
        compatible_types = {
            "string": ["string"],
            "bigint": ["bigint", "long", "int"],
            "int": ["int", "bigint", "long"],
            "double": ["double", "float"],
            "float": ["float", "double"],
            "boolean": ["boolean"],
            "timestamp": ["timestamp"],
            "date": ["date"],
            "binary": ["binary"]
        }
        
        return actual in compatible_types.get(expected, [expected])
    
    def collect_metrics(self, result: ExtractionResult):
        """Collect extraction metrics."""
        if self.metrics_collector:
            metrics = {
                "extractor_type": self.__class__.__name__,
                "table_identifier": self.table_config.identifier,
                "success": result.success,
                "row_count": result.row_count,
                "file_path": result.file_path,
                "extraction_timestamp": result.extraction_timestamp,
                "errors_count": len(result.errors)
            }
            
            if result.metadata:
                metrics.update(result.metadata)
            
            self.metrics_collector.record_extraction_metrics(metrics)


class BatchExtractor(BaseExtractor):
    """Base class for batch extractors that process multiple files."""
    
    @abstractmethod
    def extract_batch(self, source_paths: List[str], **kwargs) -> List[ExtractionResult]:
        """
        Extract data from multiple sources.
        
        Args:
            source_paths: List of paths to source data
            **kwargs: Additional extraction parameters
            
        Returns:
            List of ExtractionResult objects
        """
        pass
    
    def extract_batch_parallel(self, source_paths: List[str], 
                             max_parallelism: int = 4,
                             **kwargs) -> List[ExtractionResult]:
        """Extract batch data with parallel processing."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_parallelism) as executor:
            # Submit extraction tasks
            future_to_path = {
                executor.submit(self.extract, path, **kwargs): path 
                for path in source_paths
            }
            
            # Collect results
            for future in as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.logger.info("Extraction completed", 
                                   source_path=path, 
                                   success=result.success,
                                   row_count=result.row_count)
                except Exception as e:
                    error_result = ExtractionResult(
                        success=False,
                        file_path=path,
                        errors=[str(e)]
                    )
                    results.append(error_result)
                    self.logger.error("Extraction failed", 
                                    source_path=path, 
                                    error=str(e))
        
        return results 