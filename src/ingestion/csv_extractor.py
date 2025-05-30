"""
CSV file extractor for processing CSV files from various sources.
Supports extraction from ZIP archives and direct CSV files.
"""

import os
import zipfile
import tempfile
from typing import Dict, Any, Optional, List
from pathlib import Path
import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

from .base_extractor import BaseExtractor, ExtractionResult
from ..common.models import TableConfiguration
from ..common.exceptions import ExtractionError, ValidationError

logger = structlog.get_logger(__name__)


class CSVExtractor(BaseExtractor):
    """
    CSV file extractor that can handle CSV files from ZIP archives or direct files.
    Supports various CSV formats and configurations.
    """
    
    def __init__(self, spark: SparkSession, table_config: TableConfiguration, 
                 metrics_collector: Optional[Any] = None):
        """
        Initialize CSV extractor.
        
        Args:
            spark: Spark session
            table_config: Table configuration
            metrics_collector: Optional metrics collector
        """
        super().__init__(spark, table_config, metrics_collector)
        self.temp_dir = None
        
    def extract_data(self, source_path: str, **kwargs) -> ExtractionResult:
        """
        Extract data from CSV files.
        
        Args:
            source_path: Path to source file or directory
            **kwargs: Additional parameters including csv_options, file_pattern
            
        Returns:
            ExtractionResult with extracted DataFrame and metadata
        """
        try:
            logger.info("Starting CSV extraction", 
                       source_path=source_path, 
                       table_id=self.table_config.table_id)
            
            # Get extraction parameters
            csv_options = kwargs.get('csv_options', {})
            file_pattern = kwargs.get('file_pattern', '*.csv')
            
            # Validate source
            self._validate_source(source_path)
            
            # Extract files if ZIP
            csv_files = self._prepare_csv_files(source_path, file_pattern)
            
            # Read CSV data
            df = self._read_csv_files(csv_files, csv_options)
            
            # Apply schema mapping
            df = self._apply_schema_mapping(df)
            
            # Add technical columns
            df = self._add_technical_columns(df, source_path)
            
            # Validate data
            self._validate_data(df)
            
            # Collect metrics
            metrics = self._collect_metrics(df, source_path)
            
            logger.info("CSV extraction completed successfully",
                       table_id=self.table_config.table_id,
                       record_count=df.count())
            
            return ExtractionResult(
                data=df,
                source_path=source_path,
                record_count=df.count(),
                metadata=metrics
            )
            
        except Exception as e:
            logger.error("CSV extraction failed", 
                        source_path=source_path,
                        table_id=self.table_config.table_id,
                        error=str(e))
            raise ExtractionError(f"Failed to extract CSV data: {str(e)}") from e
        finally:
            self._cleanup_temp_files()
    
    def _prepare_csv_files(self, source_path: str, file_pattern: str) -> List[str]:
        """
        Prepare CSV files for reading. Extract from ZIP if needed.
        
        Args:
            source_path: Source path (file or directory)
            file_pattern: Pattern to match CSV files
            
        Returns:
            List of CSV file paths
        """
        csv_files = []
        
        if source_path.lower().endswith('.zip'):
            # Extract ZIP file
            csv_files = self._extract_zip_file(source_path, file_pattern)
        elif os.path.isfile(source_path) and source_path.lower().endswith('.csv'):
            # Single CSV file
            csv_files = [source_path]
        elif os.path.isdir(source_path):
            # Directory with CSV files
            csv_files = self._find_csv_files(source_path, file_pattern)
        else:
            raise ValidationError(f"Unsupported source type: {source_path}")
        
        if not csv_files:
            raise ValidationError(f"No CSV files found matching pattern: {file_pattern}")
        
        logger.info("CSV files prepared", file_count=len(csv_files), files=csv_files)
        return csv_files
    
    def _extract_zip_file(self, zip_path: str, file_pattern: str) -> List[str]:
        """
        Extract CSV files from ZIP archive.
        
        Args:
            zip_path: Path to ZIP file
            file_pattern: Pattern to match files in ZIP
            
        Returns:
            List of extracted CSV file paths
        """
        extracted_files = []
        
        try:
            # Create temporary directory
            self.temp_dir = tempfile.mkdtemp(prefix="csv_extract_")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get list of files in ZIP
                zip_files = zip_ref.namelist()
                
                # Filter files by pattern
                target_files = [f for f in zip_files if self._matches_pattern(f, file_pattern)]
                
                if not target_files:
                    raise ValidationError(f"No files matching pattern '{file_pattern}' found in ZIP")
                
                # Extract matching files
                for file_name in target_files:
                    if not file_name.endswith('/'):  # Skip directories
                        extracted_path = zip_ref.extract(file_name, self.temp_dir)
                        extracted_files.append(extracted_path)
                        logger.debug("File extracted", file=file_name, path=extracted_path)
            
            logger.info("ZIP extraction completed", 
                       zip_path=zip_path, 
                       extracted_count=len(extracted_files))
            
        except zipfile.BadZipFile as e:
            raise ValidationError(f"Invalid ZIP file: {zip_path}") from e
        except Exception as e:
            raise ExtractionError(f"Failed to extract ZIP file: {str(e)}") from e
        
        return extracted_files
    
    def _find_csv_files(self, directory: str, file_pattern: str) -> List[str]:
        """
        Find CSV files in directory matching pattern.
        
        Args:
            directory: Directory to search
            file_pattern: File pattern to match
            
        Returns:
            List of CSV file paths
        """
        csv_files = []
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                if self._matches_pattern(file, file_pattern) and file.lower().endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        
        return csv_files
    
    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """
        Check if filename matches pattern.
        
        Args:
            filename: File name to check
            pattern: Pattern to match against
            
        Returns:
            True if matches, False otherwise
        """
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)
    
    def _read_csv_files(self, csv_files: List[str], csv_options: Dict[str, Any]) -> DataFrame:
        """
        Read CSV files into Spark DataFrame.
        
        Args:
            csv_files: List of CSV file paths
            csv_options: CSV reading options
            
        Returns:
            Spark DataFrame with combined data
        """
        # Default CSV options
        default_options = {
            'header': True,
            'inferSchema': False,  # We'll apply schema explicitly
            'delimiter': ',',
            'quote': '"',
            'escape': '\\',
            'encoding': 'utf-8',
            'multiLine': True,
            'timestampFormat': 'yyyy-MM-dd HH:mm:ss',
            'dateFormat': 'yyyy-MM-dd'
        }
        
        # Merge with provided options
        options = {**default_options, **csv_options}
        
        try:
            # Read first file to get structure
            reader = self.spark.read.options(**options)
            
            if len(csv_files) == 1:
                df = reader.csv(csv_files[0])
            else:
                # Read multiple files
                df = reader.csv(csv_files)
            
            # Add source file column
            if len(csv_files) > 1:
                from pyspark.sql.functions import input_file_name, regexp_extract
                df = df.withColumn("_source_file", 
                                 regexp_extract(input_file_name(), r"([^/\\]+)$", 1))
            else:
                from pyspark.sql.functions import lit
                df = df.withColumn("_source_file", lit(os.path.basename(csv_files[0])))
            
            logger.info("CSV files read successfully", 
                       file_count=len(csv_files),
                       column_count=len(df.columns),
                       estimated_rows=df.count())
            
            return df
            
        except Exception as e:
            raise ExtractionError(f"Failed to read CSV files: {str(e)}") from e
    
    def _apply_schema_mapping(self, df: DataFrame) -> DataFrame:
        """
        Apply schema mapping to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with mapped schema
        """
        try:
            # Get target schema from table configuration
            target_schema = self._build_target_schema()
            
            # Map columns
            mapped_df = df
            
            for column_config in self.table_config.columns:
                source_name = column_config.get('source_name', column_config['name'])
                target_name = column_config['name']
                target_type = column_config['data_type']
                
                if source_name in df.columns:
                    # Cast to target type
                    mapped_df = self._cast_column(mapped_df, source_name, target_name, target_type)
                elif not column_config.get('nullable', True):
                    raise ValidationError(f"Required column '{source_name}' not found in source data")
            
            # Select only target columns
            target_columns = [col['name'] for col in self.table_config.columns]
            available_columns = [col for col in target_columns if col in mapped_df.columns]
            
            mapped_df = mapped_df.select(*available_columns)
            
            logger.info("Schema mapping applied", 
                       source_columns=len(df.columns),
                       target_columns=len(mapped_df.columns))
            
            return mapped_df
            
        except Exception as e:
            raise ExtractionError(f"Schema mapping failed: {str(e)}") from e
    
    def _cast_column(self, df: DataFrame, source_name: str, target_name: str, target_type: str) -> DataFrame:
        """
        Cast column to target type.
        
        Args:
            df: DataFrame
            source_name: Source column name
            target_name: Target column name
            target_type: Target data type
            
        Returns:
            DataFrame with casted column
        """
        from pyspark.sql.functions import col, to_date, to_timestamp
        
        try:
            if target_type.lower() in ['int', 'integer']:
                df = df.withColumn(target_name, col(source_name).cast(IntegerType()))
            elif target_type.lower() in ['double', 'float', 'decimal']:
                df = df.withColumn(target_name, col(source_name).cast(DoubleType()))
            elif target_type.lower() == 'boolean':
                df = df.withColumn(target_name, col(source_name).cast(BooleanType()))
            elif target_type.lower() == 'date':
                df = df.withColumn(target_name, to_date(col(source_name)))
            elif target_type.lower() in ['timestamp', 'datetime']:
                df = df.withColumn(target_name, to_timestamp(col(source_name)))
            else:
                # Default to string
                df = df.withColumn(target_name, col(source_name).cast(StringType()))
            
            # Drop source column if different from target
            if source_name != target_name and source_name in df.columns:
                df = df.drop(source_name)
            
            return df
            
        except Exception as e:
            logger.warning("Column casting failed", 
                          source=source_name, 
                          target=target_name, 
                          type=target_type,
                          error=str(e))
            # Keep original column if casting fails
            return df.withColumnRenamed(source_name, target_name) if source_name != target_name else df
    
    def _build_target_schema(self) -> StructType:
        """
        Build target schema from table configuration.
        
        Returns:
            StructType representing target schema
        """
        fields = []
        
        for column_config in self.table_config.columns:
            name = column_config['name']
            data_type = column_config['data_type']
            nullable = column_config.get('nullable', True)
            
            if data_type.lower() in ['int', 'integer']:
                spark_type = IntegerType()
            elif data_type.lower() in ['double', 'float', 'decimal']:
                spark_type = DoubleType()
            elif data_type.lower() == 'boolean':
                spark_type = BooleanType()
            elif data_type.lower() == 'date':
                spark_type = DateType()
            elif data_type.lower() in ['timestamp', 'datetime']:
                spark_type = TimestampType()
            else:
                spark_type = StringType()
            
            fields.append(StructField(name, spark_type, nullable))
        
        return StructType(fields)
    
    def _cleanup_temp_files(self):
        """Clean up temporary files and directories."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
                logger.debug("Temporary directory cleaned up", temp_dir=self.temp_dir)
            except Exception as e:
                logger.warning("Failed to cleanup temporary directory", 
                             temp_dir=self.temp_dir, 
                             error=str(e))
            finally:
                self.temp_dir = None 