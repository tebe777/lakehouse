# src/common/utils/validation_integration.py

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame
from .validator import DataValidator
from .filename_parser import FileNameParser
from .config import TableConfig

logger = logging.getLogger(__name__)


class IntegratedDataValidator:
    """
    Integration wrapper that combines the new DataValidator with the existing
    quality validation framework for seamless pipeline integration.
    """
    
    def __init__(self, table_config: TableConfig):
        self.table_config = table_config
        self.validation_rules = table_config.validation
        self.data_validator = DataValidator(self.validation_rules)
        
    def validate_dataframe(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame using the new DataValidator integrated with table config.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Validation result dictionary
        """
        logger.info(f"Starting integrated validation for table: {self.table_config.identifier}")
        
        try:
            # Use the new DataValidator
            self.data_validator.validate(df)
            
            result = {
                'table_identifier': self.table_config.identifier,
                'validation_timestamp': datetime.now().isoformat(),
                'status': 'passed',
                'row_count': df.count(),
                'column_count': len(df.columns),
                'validated_columns': list(self.validation_rules.get('null_check', [])),
                'date_range_columns': list(self.validation_rules.get('date_range', {}).keys()),
                'errors': [],
                'warnings': []
            }
            
            logger.info(f"Validation passed for table: {self.table_config.identifier}")
            return result
            
        except ValueError as e:
            logger.error(f"Validation failed for table {self.table_config.identifier}: {e}")
            
            result = {
                'table_identifier': self.table_config.identifier,
                'validation_timestamp': datetime.now().isoformat(),
                'status': 'failed',
                'row_count': df.count() if df else 0,
                'column_count': len(df.columns) if df else 0,
                'validated_columns': list(self.validation_rules.get('null_check', [])),
                'date_range_columns': list(self.validation_rules.get('date_range', {}).keys()),
                'errors': [str(e)],
                'warnings': []
            }
            
            return result


class FileProcessor:
    """
    File processing utility that integrates filename parsing with validation.
    """
    
    def __init__(self, table_config: TableConfig):
        self.table_config = table_config
        
    def process_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Process file and extract metadata using the FileNameParser.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File metadata dictionary
        """
        logger.info(f"Processing file metadata for: {file_path}")
        
        try:
            # Parse filename using the new FileNameParser
            filename_info = FileNameParser.parse(file_path)
            
            # Get file stats
            file_path_obj = Path(file_path)
            file_stats = {
                'file_size_bytes': file_path_obj.stat().st_size if file_path_obj.exists() else 0,
                'file_exists': file_path_obj.exists(),
                'file_extension': file_path_obj.suffix
            }
            
            result = {
                'table_identifier': self.table_config.identifier,
                'file_path': file_path,
                'processing_timestamp': datetime.now().isoformat(),
                'filename_info': filename_info,
                'file_stats': file_stats,
                'validation_status': 'pending'
            }
            
            # Basic file validation
            if not file_stats['file_exists']:
                result['validation_status'] = 'failed'
                result['errors'] = ['File does not exist']
            elif file_stats['file_size_bytes'] == 0:
                result['validation_status'] = 'failed'
                result['errors'] = ['File is empty']
            else:
                result['validation_status'] = 'passed'
                result['errors'] = []
            
            logger.info(f"File metadata processed successfully for: {file_path}")
            return result
            
        except ValueError as e:
            logger.error(f"Failed to process file metadata for {file_path}: {e}")
            
            result = {
                'table_identifier': self.table_config.identifier,
                'file_path': file_path,
                'processing_timestamp': datetime.now().isoformat(),
                'filename_info': None,
                'file_stats': {'file_exists': False, 'file_size_bytes': 0},
                'validation_status': 'failed',
                'errors': [str(e)]
            }
            
            return result


def load_validation_rules_from_file(rules_file_path: str) -> Dict[str, Any]:
    """
    Load validation rules from a JSON file.
    
    Args:
        rules_file_path: Path to the validation rules JSON file
        
    Returns:
        Validation rules dictionary
    """
    logger.info(f"Loading validation rules from: {rules_file_path}")
    
    try:
        with open(rules_file_path, 'r') as f:
            rules = json.load(f)
        
        logger.info(f"Successfully loaded validation rules: {rules}")
        return rules
        
    except Exception as e:
        logger.error(f"Failed to load validation rules from {rules_file_path}: {e}")
        raise


def create_integrated_validator(table_config: TableConfig) -> IntegratedDataValidator:
    """
    Factory function to create an IntegratedDataValidator instance.
    
    Args:
        table_config: Table configuration object
        
    Returns:
        IntegratedDataValidator instance
    """
    logger.info(f"Creating integrated validator for table: {table_config.identifier}")
    return IntegratedDataValidator(table_config)


def create_file_processor(table_config: TableConfig) -> FileProcessor:
    """
    Factory function to create a FileProcessor instance.
    
    Args:
        table_config: Table configuration object
        
    Returns:
        FileProcessor instance
    """
    logger.info(f"Creating file processor for table: {table_config.identifier}")
    return FileProcessor(table_config) 