# src/ingestion/minio_extractor.py

import os
import tempfile
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import structlog

from src.common.connections.s3_connection import S3ConnectionManager
from src.common.utils.filename_parser import FileNameParser
from src.common.utils.validation_integration import FileProcessor, IntegratedDataValidator
from src.common.models.table_config import TableConfig
from .base_extractor import BaseExtractor, ExtractionResult, ExtractionError

logger = structlog.get_logger(__name__)


class MinIOExtractor(BaseExtractor):
    """
    Extractor for data stored in MinIO (S3-compatible storage).
    
    Features:
    - Downloads files from MinIO using boto3
    - Supports filename pattern matching and validation
    - Integrates with enhanced validation system
    - Handles ZIP file extraction
    - Temporary file management
    - Comprehensive error handling and logging
    """
    
    def __init__(self, spark, table_config: TableConfig, s3_config: Dict[str, Any]):
        """
        Initialize MinIO extractor.
        
        Args:
            spark: Spark session
            table_config: Table configuration
            s3_config: S3/MinIO connection configuration
        """
        super().__init__(spark, table_config)
        
        self.s3_config = s3_config
        self.s3_manager = S3ConnectionManager(s3_config)
        self.file_processor = FileProcessor(table_config)
        self.validator = IntegratedDataValidator(table_config)
        self.temp_dir = Path(tempfile.mkdtemp(prefix="minio_extract_"))
        
        self.logger = logger.bind(
            component="MinIOExtractor",
            table_id=table_config.identifier,
            endpoint=s3_config.get('endpoint', 'unknown')
        )
        
        self.logger.info("MinIO extractor initialized")
    
    def extract_data(self, source_path: str, **kwargs) -> ExtractionResult:
        """
        Extract data from MinIO.
        
        Args:
            source_path: S3 path in format 'bucket/prefix' or 's3://bucket/prefix'
            **kwargs: Additional parameters:
                - file_pattern: Pattern to match files (e.g., "*.csv.ZIP")
                - max_files: Maximum number of files to process
                - validate_filenames: Whether to validate filename patterns
                - extract_zip: Whether to extract ZIP files
                
        Returns:
            ExtractionResult with extracted DataFrame and metadata
        """
        try:
            self.logger.info("Starting MinIO data extraction", source_path=source_path)
            
            # Parse S3 path
            bucket, prefix = self._parse_s3_path(source_path)
            
            # Get extraction parameters
            file_pattern = kwargs.get('file_pattern', '*.csv.ZIP')
            max_files = kwargs.get('max_files', 100)
            validate_filenames = kwargs.get('validate_filenames', True)
            extract_zip = kwargs.get('extract_zip', True)
            
            # Test S3 connection
            if not self.s3_manager.test_connection():
                raise ExtractionError("Failed to connect to MinIO")
            
            # Step 1: List and filter files in MinIO
            available_files = self._list_source_files(bucket, prefix, file_pattern, max_files)
            
            if not available_files:
                raise ExtractionError(f"No files found matching pattern '{file_pattern}' in {bucket}/{prefix}")
            
            self.logger.info("Found source files", file_count=len(available_files))
            
            # Step 2: Validate filenames if requested
            if validate_filenames:
                available_files = self._validate_filenames(available_files)
            
            # Step 3: Download files to local temp directory
            local_files = self._download_files(bucket, available_files)
            
            # Step 4: Extract ZIP files if needed
            if extract_zip:
                local_files = self._extract_zip_files(local_files)
            
            # Step 5: Process CSV files into DataFrame
            df = self._process_csv_files(local_files)
            
            # Step 6: Apply table schema and transformations
            df = self._apply_table_schema(df)
            
            # Step 7: Add technical metadata columns
            df = self._add_technical_columns(df, source_path, available_files)
            
            # Step 8: Validate data quality
            validation_result = self._validate_data_quality(df)
            
            # Step 9: Collect extraction metrics
            metrics = self._collect_extraction_metrics(df, available_files, local_files, validation_result)
            
            self.logger.info("MinIO extraction completed successfully",
                           record_count=df.count(),
                           files_processed=len(local_files))
            
            return ExtractionResult(
                data=df,
                source_path=source_path,
                record_count=df.count(),
                metadata=metrics
            )
            
        except Exception as e:
            self.logger.error("MinIO extraction failed", 
                            source_path=source_path, 
                            error=str(e))
            raise ExtractionError(f"Failed to extract data from MinIO: {str(e)}") from e
        
        finally:
            # Cleanup temporary files
            self._cleanup_temp_files()
    
    def _parse_s3_path(self, source_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and prefix."""
        # Handle both formats: 'bucket/prefix' and 's3://bucket/prefix'
        if source_path.startswith('s3://'):
            path_parts = source_path[5:].split('/', 1)
        else:
            path_parts = source_path.split('/', 1)
        
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        self.logger.debug("Parsed S3 path", bucket=bucket, prefix=prefix)
        return bucket, prefix
    
    def _list_source_files(self, bucket: str, prefix: str, file_pattern: str, max_files: int) -> List[Dict[str, Any]]:
        """List files in MinIO bucket matching pattern."""
        try:
            # List objects with prefix
            objects = self.s3_manager.list_objects(bucket, prefix, max_files)
            
            # Filter by pattern
            import fnmatch
            filtered_objects = []
            
            for obj in objects:
                filename = Path(obj['key']).name
                if fnmatch.fnmatch(filename, file_pattern):
                    filtered_objects.append(obj)
            
            self.logger.info("Listed and filtered files", 
                           total_objects=len(objects),
                           filtered_count=len(filtered_objects))
            
            return filtered_objects
            
        except Exception as e:
            self.logger.error("Failed to list source files", 
                            bucket=bucket, 
                            prefix=prefix, 
                            error=str(e))
            raise
    
    def _validate_filenames(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate filenames using FileNameParser."""
        validated_files = []
        
        for file_obj in files:
            filename = Path(file_obj['key']).name
            
            try:
                # Use FileNameParser to validate filename format
                parsed_info = FileNameParser.parse(filename)
                file_obj['parsed_filename'] = parsed_info
                validated_files.append(file_obj)
                
                self.logger.debug("Filename validated", 
                                filename=filename,
                                prefix=parsed_info['prefix'],
                                file_type=parsed_info['file_type'])
                
            except ValueError as e:
                self.logger.warning("Filename validation failed", 
                                  filename=filename, 
                                  error=str(e))
                # Skip invalid filenames
                continue
        
        self.logger.info("Filename validation completed", 
                        valid_files=len(validated_files),
                        invalid_files=len(files) - len(validated_files))
        
        return validated_files
    
    def _download_files(self, bucket: str, files: List[Dict[str, Any]]) -> List[str]:
        """Download files from MinIO to local temp directory."""
        downloaded_files = []
        
        for file_obj in files:
            key = file_obj['key']
            filename = Path(key).name
            local_path = self.temp_dir / filename
            
            try:
                if self.s3_manager.download_file(bucket, key, str(local_path)):
                    downloaded_files.append(str(local_path))
                    
                    self.logger.debug("File downloaded", 
                                    key=key, 
                                    local_path=str(local_path),
                                    file_size=file_obj['size'])
                else:
                    self.logger.warning("Failed to download file", key=key)
                    
            except Exception as e:
                self.logger.error("Error downloading file", 
                                key=key, 
                                error=str(e))
                # Continue with other files
                continue
        
        self.logger.info("Files downloaded", downloaded_count=len(downloaded_files))
        return downloaded_files
    
    def _extract_zip_files(self, local_files: List[str]) -> List[str]:
        """Extract ZIP files and return list of extracted CSV files."""
        csv_files = []
        
        for file_path in local_files:
            if file_path.lower().endswith('.zip'):
                try:
                    import zipfile
                    
                    extract_dir = self.temp_dir / f"extracted_{Path(file_path).stem}"
                    extract_dir.mkdir(exist_ok=True)
                    
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        # Extract only CSV files
                        for member in zip_ref.namelist():
                            if member.lower().endswith('.csv'):
                                zip_ref.extract(member, extract_dir)
                                csv_files.append(str(extract_dir / member))
                                
                                self.logger.debug("Extracted CSV from ZIP", 
                                                zip_file=file_path,
                                                extracted_file=member)
                    
                except Exception as e:
                    self.logger.error("Failed to extract ZIP file", 
                                    file_path=file_path, 
                                    error=str(e))
                    continue
            else:
                # Non-ZIP file, add directly
                csv_files.append(file_path)
        
        self.logger.info("ZIP extraction completed", csv_files_count=len(csv_files))
        return csv_files
    
    def _process_csv_files(self, csv_files: List[str]) -> 'DataFrame':
        """Process CSV files into Spark DataFrame."""
        from pyspark.sql.functions import input_file_name, lit
        
        if not csv_files:
            raise ExtractionError("No CSV files to process")
        
        try:
            # CSV reading options
            csv_options = {
                'header': True,
                'inferSchema': False,  # We'll apply schema explicitly
                'delimiter': ',',
                'quote': '"',
                'escape': '\\',
                'encoding': 'utf-8',
                'multiLine': True
            }
            
            # Read CSV files
            if len(csv_files) == 1:
                df = self.spark.read.options(**csv_options).csv(csv_files[0])
            else:
                df = self.spark.read.options(**csv_options).csv(csv_files)
            
            # Add source file information
            df = df.withColumn("_source_file_name", 
                             input_file_name().alias("source_file"))
            
            self.logger.info("CSV files processed into DataFrame", 
                           file_count=len(csv_files),
                           column_count=len(df.columns))
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to process CSV files", error=str(e))
            raise
    
    def _validate_data_quality(self, df: 'DataFrame') -> Dict[str, Any]:
        """Validate data quality using integrated validator."""
        try:
            return self.validator.validate_dataframe(df)
        except Exception as e:
            self.logger.warning("Data quality validation failed", error=str(e))
            return {
                'status': 'failed',
                'error': str(e),
                'validation_timestamp': datetime.now().isoformat()
            }
    
    def _collect_extraction_metrics(self, df: 'DataFrame', source_files: List[Dict[str, Any]], 
                                  local_files: List[str], validation_result: Dict[str, Any]) -> Dict[str, Any]:
        """Collect comprehensive extraction metrics."""
        return {
            'extraction_timestamp': datetime.now().isoformat(),
            'source_info': {
                'endpoint': self.s3_config.get('endpoint'),
                'total_source_files': len(source_files),
                'total_source_size_bytes': sum(f['size'] for f in source_files),
                'local_files_processed': len(local_files)
            },
            'data_info': {
                'record_count': df.count(),
                'column_count': len(df.columns),
                'columns': df.columns
            },
            'validation_result': validation_result,
            'processing_stats': {
                'temp_directory': str(self.temp_dir),
                'files_downloaded': len(local_files)
            }
        }
    
    def _cleanup_temp_files(self):
        """Clean up temporary files and directories."""
        try:
            import shutil
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info("Temporary files cleaned up", temp_dir=str(self.temp_dir))
        except Exception as e:
            self.logger.warning("Failed to cleanup temp files", 
                              temp_dir=str(self.temp_dir), 
                              error=str(e))
    
    def __del__(self):
        """Ensure cleanup on object destruction."""
        self._cleanup_temp_files()
        if hasattr(self, 's3_manager'):
            self.s3_manager.close() 