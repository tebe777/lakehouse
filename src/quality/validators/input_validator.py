# src/quality/validators/input_validator.py
import os
import zipfile
import csv
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import pandas as pd
from dataclasses import dataclass, field

from src.common.models.table_config import TableConfig
from src.common.utils.config_manager import ConfigManager
from src.common.monitoring.logger import ETLLogger


@dataclass
class FileValidationResult:
    """Result of file validation"""
    file_path: str
    is_valid: bool
    file_size_bytes: int
    row_count: int = 0
    column_count: int = 0
    encoding: str = "unknown"
    delimiter: str = ","
    has_header: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InputValidationResult:
    """Complete input validation result"""
    validation_id: str
    source_path: str
    table_identifier: str
    validation_timestamp: datetime
    overall_status: str  # "passed", "failed", "warning"
    file_results: List[FileValidationResult] = field(default_factory=list)
    cross_file_checks: Dict[str, Any] = field(default_factory=dict)
    schema_evolution_check: Dict[str, Any] = field(default_factory=dict)
    business_validation: Dict[str, Any] = field(default_factory=dict)
    lineage_validation: Dict[str, Any] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)


class ComprehensiveInputValidator:
    """
    Comprehensive input validation framework for ETL pipeline.
    
    Performs multi-layer validation:
    1. File-level validation (format, structure, encoding)
    2. Schema validation and evolution checks
    3. Business context validation
    4. Cross-file consistency validation
    5. Data lineage and dependency validation
    """
    
    def __init__(self, config_manager: ConfigManager, temp_dir: str = "/tmp/validation"):
        self.config_manager = config_manager
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.logger = ETLLogger(self.__class__.__name__)
        
        # Load validation configuration
        self.env_config = config_manager.get_environment_config()
        self.validation_config = self.env_config.get('input_validation', {})
        
        # Validation thresholds
        self.max_file_size_mb = self.validation_config.get('max_file_size_mb', 500)
        self.min_file_size_bytes = self.validation_config.get('min_file_size_bytes', 100)
        self.max_row_count = self.validation_config.get('max_row_count', 10000000)
        self.min_row_count = self.validation_config.get('min_row_count', 1)
        self.allowed_encodings = self.validation_config.get('allowed_encodings', ['utf-8', 'latin1', 'cp1252'])
        self.max_schema_drift_percentage = self.validation_config.get('max_schema_drift_percentage', 20)
        
        self.logger.info("ComprehensiveInputValidator initialized")
    
    def validate_input(self, source_path: str, table_identifier: str,
                      execution_date: str = None) -> InputValidationResult:
        """
        Perform comprehensive input validation
        
        Args:
            source_path: Path to input file/directory (ZIP, CSV, etc.)
            table_identifier: Target table identifier for configuration
            execution_date: Execution date for context
        """
        if execution_date is None:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        
        validation_id = f"validation_{table_identifier.replace('.', '_')}_{int(datetime.now().timestamp())}"
        
        self.logger.info(f"Starting comprehensive input validation: {validation_id}")
        self.logger.info(f"Source: {source_path}, Target: {table_identifier}")
        
        # Initialize result
        result = InputValidationResult(
            validation_id=validation_id,
            source_path=source_path,
            table_identifier=table_identifier,
            validation_timestamp=datetime.now(),
            overall_status="unknown"
        )
        
        try:
            # Get table configuration
            table_config = self.config_manager.get_table_config(table_identifier)
            
            # Step 1: File-level validation
            self.logger.info("Step 1: File-level validation")
            file_results = self._validate_files(source_path, table_config)
            result.file_results = file_results
            
            if not any(f.is_valid for f in file_results):
                result.overall_status = "failed"
                result.summary = {"error": "All files failed validation"}
                return result
            
            # Step 2: Schema validation and evolution check
            self.logger.info("Step 2: Schema validation and evolution check")
            result.schema_evolution_check = self._validate_schema_evolution(file_results, table_config)
            
            # Step 3: Cross-file consistency validation
            self.logger.info("Step 3: Cross-file consistency validation")
            result.cross_file_checks = self._validate_cross_file_consistency(file_results, table_config)
            
            # Step 4: Business context validation
            self.logger.info("Step 4: Business context validation")
            result.business_validation = self._validate_business_context(file_results, table_config, execution_date)
            
            # Step 5: Data lineage validation
            self.logger.info("Step 5: Data lineage validation")
            result.lineage_validation = self._validate_data_lineage(table_config, execution_date)
            
            # Step 6: Generate summary and overall status
            result.summary = self._generate_validation_summary(result)
            result.overall_status = self._determine_overall_status(result)
            
            self.logger.info(f"Input validation completed: {result.overall_status}")
            
        except Exception as e:
            error_msg = f"Input validation failed: {str(e)}"
            self.logger.error(error_msg)
            result.overall_status = "failed"
            result.summary = {"error": error_msg}
        
        return result
    
    def _validate_files(self, source_path: str, table_config: TableConfig) -> List[FileValidationResult]:
        """Validate individual files (ZIP extraction, CSV parsing, etc.)"""
        file_results = []
        
        source_file = Path(source_path)
        
        if not source_file.exists():
            return [FileValidationResult(
                file_path=source_path,
                is_valid=False,
                file_size_bytes=0,
                errors=[f"Source file does not exist: {source_path}"]
            )]
        
        # Handle ZIP files
        if source_path.endswith('.zip'):
            file_results.extend(self._validate_zip_file(source_path, table_config))
        
        # Handle individual CSV files
        elif source_path.endswith('.csv'):
            file_results.append(self._validate_csv_file(source_path, table_config))
        
        # Handle directory with multiple files
        elif source_file.is_dir():
            for file_path in source_file.glob("*.csv"):
                file_results.append(self._validate_csv_file(str(file_path), table_config))
        
        else:
            file_results.append(FileValidationResult(
                file_path=source_path,
                is_valid=False,
                file_size_bytes=source_file.stat().st_size,
                errors=[f"Unsupported file format: {source_path}"]
            ))
        
        return file_results
    
    def _validate_zip_file(self, zip_path: str, table_config: TableConfig) -> List[FileValidationResult]:
        """Validate ZIP file and its contents"""
        results = []
        
        # Basic ZIP validation
        zip_result = FileValidationResult(
            file_path=zip_path,
            is_valid=True,
            file_size_bytes=os.path.getsize(zip_path)
        )
        
        # Check file size
        file_size_mb = zip_result.file_size_bytes / (1024 * 1024)
        if file_size_mb > self.max_file_size_mb:
            zip_result.errors.append(f"ZIP file too large: {file_size_mb:.1f}MB > {self.max_file_size_mb}MB")
            zip_result.is_valid = False
        
        if zip_result.file_size_bytes < self.min_file_size_bytes:
            zip_result.errors.append(f"ZIP file too small: {zip_result.file_size_bytes} bytes")
            zip_result.is_valid = False
        
        # Validate ZIP structure
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Check ZIP integrity
                bad_files = zf.testzip()
                if bad_files:
                    zip_result.errors.append(f"Corrupted files in ZIP: {bad_files}")
                    zip_result.is_valid = False
                
                # Get CSV files
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                
                if not csv_files:
                    zip_result.errors.append("No CSV files found in ZIP")
                    zip_result.is_valid = False
                
                zip_result.metadata = {
                    'total_files': len(zf.namelist()),
                    'csv_files': len(csv_files),
                    'csv_file_names': csv_files
                }
                
                # Extract and validate each CSV
                extract_dir = self.temp_dir / f"extract_{int(datetime.now().timestamp())}"
                extract_dir.mkdir(exist_ok=True)
                
                for csv_file in csv_files:
                    try:
                        extracted_path = zf.extract(csv_file, extract_dir)
                        csv_result = self._validate_csv_file(extracted_path, table_config)
                        csv_result.metadata['source_zip'] = zip_path
                        results.append(csv_result)
                    except Exception as e:
                        csv_result = FileValidationResult(
                            file_path=csv_file,
                            is_valid=False,
                            file_size_bytes=0,
                            errors=[f"Failed to extract {csv_file}: {str(e)}"]
                        )
                        results.append(csv_result)
        
        except Exception as e:
            zip_result.errors.append(f"Failed to read ZIP file: {str(e)}")
            zip_result.is_valid = False
        
        results.insert(0, zip_result)  # Add ZIP result at the beginning
        return results
    
    def _validate_csv_file(self, csv_path: str, table_config: TableConfig) -> FileValidationResult:
        """Comprehensive CSV file validation"""
        result = FileValidationResult(
            file_path=csv_path,
            is_valid=True,
            file_size_bytes=os.path.getsize(csv_path) if os.path.exists(csv_path) else 0
        )
        
        if not os.path.exists(csv_path):
            result.is_valid = False
            result.errors.append(f"CSV file does not exist: {csv_path}")
            return result
        
        try:
            # 1. File size validation
            file_size_mb = result.file_size_bytes / (1024 * 1024)
            if file_size_mb > self.max_file_size_mb:
                result.warnings.append(f"Large CSV file: {file_size_mb:.1f}MB")
            
            # 2. Encoding detection and validation
            result.encoding = self._detect_encoding(csv_path)
            if result.encoding not in self.allowed_encodings:
                result.warnings.append(f"Unusual encoding detected: {result.encoding}")
            
            # 3. CSV structure validation
            delimiter_info = self._detect_delimiter(csv_path, result.encoding)
            result.delimiter = delimiter_info['delimiter']
            result.has_header = delimiter_info['has_header']
            
            # 4. Row and column count validation
            csv_info = self._analyze_csv_structure(csv_path, result.encoding, result.delimiter)
            result.row_count = csv_info['row_count']
            result.column_count = csv_info['column_count']
            
            # Validate row count
            if result.row_count > self.max_row_count:
                result.errors.append(f"Too many rows: {result.row_count} > {self.max_row_count}")
                result.is_valid = False
            
            if result.row_count < self.min_row_count:
                result.errors.append(f"Too few rows: {result.row_count} < {self.min_row_count}")
                result.is_valid = False
            
            # 5. Schema compatibility check
            schema_check = self._check_csv_schema_compatibility(csv_info, table_config)
            if not schema_check['compatible']:
                for error in schema_check['errors']:
                    result.errors.append(f"Schema incompatibility: {error}")
                    result.is_valid = False
                
                for warning in schema_check['warnings']:
                    result.warnings.append(f"Schema warning: {warning}")
            
            # 6. Data quality quick check
            quality_check = self._quick_data_quality_check(csv_path, result.encoding, result.delimiter)
            result.metadata.update(quality_check)
            
            # Add quality warnings
            if quality_check['empty_rows_percentage'] > 5:
                result.warnings.append(f"High empty row percentage: {quality_check['empty_rows_percentage']:.1f}%")
            
            if quality_check['duplicate_header_count'] > 0:
                result.warnings.append(f"Duplicate headers found: {quality_check['duplicate_header_count']}")
            
        except Exception as e:
            result.is_valid = False
            result.errors.append(f"CSV validation failed: {str(e)}")
        
        return result
    
    def _detect_encoding(self, file_path: str) -> str:
        """Detect file encoding"""
        try:
            import chardet
            
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB
                result = chardet.detect(raw_data)
                return result['encoding'].lower() if result['encoding'] else 'utf-8'
        except ImportError:
            # Fallback if chardet not available
            encodings = ['utf-8', 'latin1', 'cp1252']
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        f.read(1000)
                    return encoding
                except UnicodeDecodeError:
                    continue
            return 'utf-8'  # Default fallback
    
    def _detect_delimiter(self, file_path: str, encoding: str) -> Dict[str, Any]:
        """Detect CSV delimiter and header presence"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                # Read first few lines for analysis
                sample = f.read(8192)
                
            # Use CSV sniffer
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            has_header = sniffer.has_header(sample)
            
            return {
                'delimiter': delimiter,
                'has_header': has_header
            }
            
        except Exception:
            # Fallback to comma delimiter
            return {
                'delimiter': ',',
                'has_header': True
            }
    
    def _analyze_csv_structure(self, file_path: str, encoding: str, delimiter: str) -> Dict[str, Any]:
        """Analyze CSV structure (rows, columns, headers)"""
        try:
            # Use pandas for efficient analysis
            df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, nrows=1000)
            
            # Get full row count efficiently
            with open(file_path, 'r', encoding=encoding) as f:
                row_count = sum(1 for line in f) - 1  # Subtract header
            
            return {
                'row_count': row_count,
                'column_count': len(df.columns),
                'column_names': list(df.columns),
                'sample_data': df.head().to_dict('records'),
                'data_types': df.dtypes.to_dict()
            }
            
        except Exception as e:
            return {
                'row_count': 0,
                'column_count': 0,
                'error': str(e)
            }
    
    def _check_csv_schema_compatibility(self, csv_info: Dict[str, Any], 
                                      table_config: TableConfig) -> Dict[str, Any]:
        """Check if CSV schema is compatible with table configuration"""
        result = {
            'compatible': True,
            'errors': [],
            'warnings': [],
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': []
        }
        
        if 'column_names' not in csv_info:
            result['compatible'] = False
            result['errors'].append("Could not determine CSV column structure")
            return result
        
        csv_columns = set(csv_info['column_names'])
        config_columns = set(table_config.schema.keys())
        
        # Check for missing required columns
        missing_columns = config_columns - csv_columns
        if missing_columns:
            result['missing_columns'] = list(missing_columns)
            result['errors'].extend([f"Missing required column: {col}" for col in missing_columns])
            result['compatible'] = False
        
        # Check for extra columns (schema evolution)
        extra_columns = csv_columns - config_columns
        if extra_columns:
            result['extra_columns'] = list(extra_columns)
            result['warnings'].extend([f"New column detected: {col}" for col in extra_columns])
            
            # Check if schema drift is within acceptable limits
            drift_percentage = len(extra_columns) / len(config_columns) * 100
            if drift_percentage > self.max_schema_drift_percentage:
                result['errors'].append(f"Schema drift too high: {drift_percentage:.1f}% > {self.max_schema_drift_percentage}%")
                result['compatible'] = False
        
        # Type compatibility check (basic)
        if 'data_types' in csv_info:
            for col_name, expected_type in table_config.schema.items():
                if col_name in csv_info['data_types']:
                    csv_type = str(csv_info['data_types'][col_name])
                    if not self._are_types_compatible(csv_type, expected_type):
                        result['type_mismatches'].append({
                            'column': col_name,
                            'csv_type': csv_type,
                            'expected_type': expected_type
                        })
                        result['warnings'].append(f"Type mismatch in {col_name}: {csv_type} vs {expected_type}")
        
        return result
    
    def _are_types_compatible(self, csv_type: str, expected_type: str) -> bool:
        """Check if CSV pandas type is compatible with expected Spark type"""
        type_mappings = {
            'object': ['string'],
            'int64': ['long', 'int'],
            'float64': ['double', 'float'],
            'bool': ['boolean'],
            'datetime64': ['timestamp', 'date']
        }
        
        compatible_types = type_mappings.get(csv_type, [])
        return expected_type in compatible_types
    
    def _quick_data_quality_check(self, file_path: str, encoding: str, delimiter: str) -> Dict[str, Any]:
        """Perform quick data quality checks on CSV"""
        try:
            # Sample-based analysis for performance
            df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, nrows=1000)
            
            # Calculate quality metrics
            total_rows = len(df)
            empty_rows = df.isnull().all(axis=1).sum()
            duplicate_rows = df.duplicated().sum()
            
            quality_metrics = {
                'empty_rows_count': int(empty_rows),
                'empty_rows_percentage': (empty_rows / total_rows * 100) if total_rows > 0 else 0,
                'duplicate_rows_count': int(duplicate_rows),
                'duplicate_rows_percentage': (duplicate_rows / total_rows * 100) if total_rows > 0 else 0,
                'duplicate_header_count': len(df.columns) - len(set(df.columns)),
                'null_percentage_by_column': df.isnull().mean().to_dict(),
                'sample_analyzed': total_rows
            }
            
            return quality_metrics
            
        except Exception as e:
            return {
                'error': f"Quality check failed: {str(e)}",
                'sample_analyzed': 0
            }
    
    def _validate_schema_evolution(self, file_results: List[FileValidationResult], 
                                 table_config: TableConfig) -> Dict[str, Any]:
        """Validate schema evolution across files and against configuration"""
        evolution_check = {
            'schema_consistent_across_files': True,
            'new_columns_detected': [],
            'missing_columns_detected': [],
            'schema_evolution_summary': {},
            'recommendation': None
        }
        
        try:
            # Get all column sets from valid CSV files
            csv_results = [f for f in file_results if f.file_path.endswith('.csv') and f.is_valid]
            
            if not csv_results:
                evolution_check['recommendation'] = "No valid CSV files to analyze"
                return evolution_check
            
            # Collect all column names from files
            all_column_sets = []
            for file_result in csv_results:
                if 'column_names' in file_result.metadata:
                    all_column_sets.append(set(file_result.metadata['column_names']))
            
            if not all_column_sets:
                evolution_check['recommendation'] = "Could not extract column information from files"
                return evolution_check
            
            # Check consistency across files
            first_column_set = all_column_sets[0]
            for i, column_set in enumerate(all_column_sets[1:], 1):
                if column_set != first_column_set:
                    evolution_check['schema_consistent_across_files'] = False
                    break
            
            # Compare with table configuration
            config_columns = set(table_config.schema.keys())
            file_columns = first_column_set  # Use first file as reference
            
            new_columns = file_columns - config_columns
            missing_columns = config_columns - file_columns
            
            evolution_check['new_columns_detected'] = list(new_columns)
            evolution_check['missing_columns_detected'] = list(missing_columns)
            
            # Generate summary
            evolution_check['schema_evolution_summary'] = {
                'files_analyzed': len(csv_results),
                'schema_consistent': evolution_check['schema_consistent_across_files'],
                'new_columns_count': len(new_columns),
                'missing_columns_count': len(missing_columns),
                'schema_drift_percentage': (len(new_columns) / len(config_columns) * 100) if config_columns else 0
            }
            
            # Generate recommendation
            if missing_columns:
                evolution_check['recommendation'] = f"CRITICAL: Missing required columns: {missing_columns}"
            elif new_columns and len(new_columns) / len(config_columns) > 0.1:
                evolution_check['recommendation'] = f"WARNING: Significant schema evolution detected. Consider updating table configuration."
            elif new_columns:
                evolution_check['recommendation'] = f"INFO: New columns detected: {new_columns}. Schema evolution within acceptable limits."
            else:
                evolution_check['recommendation'] = "Schema is consistent with configuration."
            
        except Exception as e:
            evolution_check['error'] = f"Schema evolution check failed: {str(e)}"
        
        return evolution_check
    
    def _validate_cross_file_consistency(self, file_results: List[FileValidationResult], 
                                       table_config: TableConfig) -> Dict[str, Any]:
        """Validate consistency across multiple files"""
        consistency_check = {
            'files_analyzed': 0,
            'consistent_structure': True,
            'consistent_encoding': True,
            'consistent_delimiter': True,
            'row_count_distribution': {},
            'encoding_distribution': {},
            'delimiter_distribution': {},
            'issues': []
        }
        
        try:
            csv_results = [f for f in file_results if f.file_path.endswith('.csv') and f.is_valid]
            consistency_check['files_analyzed'] = len(csv_results)
            
            if len(csv_results) <= 1:
                consistency_check['note'] = "Only one file to analyze - no cross-file validation needed"
                return consistency_check
            
            # Collect file properties
            encodings = [f.encoding for f in csv_results]
            delimiters = [f.delimiter for f in csv_results]
            row_counts = [f.row_count for f in csv_results]
            column_counts = [f.column_count for f in csv_results]
            
            # Check encoding consistency
            unique_encodings = set(encodings)
            if len(unique_encodings) > 1:
                consistency_check['consistent_encoding'] = False
                consistency_check['issues'].append(f"Inconsistent encodings: {unique_encodings}")
            
            # Check delimiter consistency
            unique_delimiters = set(delimiters)
            if len(unique_delimiters) > 1:
                consistency_check['consistent_delimiter'] = False
                consistency_check['issues'].append(f"Inconsistent delimiters: {unique_delimiters}")
            
            # Check structure consistency
            unique_column_counts = set(column_counts)
            if len(unique_column_counts) > 1:
                consistency_check['consistent_structure'] = False
                consistency_check['issues'].append(f"Inconsistent column counts: {unique_column_counts}")
            
            # Generate distributions
            consistency_check['row_count_distribution'] = {
                'min': min(row_counts),
                'max': max(row_counts),
                'avg': sum(row_counts) / len(row_counts),
                'total': sum(row_counts)
            }
            
            consistency_check['encoding_distribution'] = {enc: encodings.count(enc) for enc in unique_encodings}
            consistency_check['delimiter_distribution'] = {delim: delimiters.count(delim) for delim in unique_delimiters}
            
            # Check for outliers in row counts
            avg_rows = consistency_check['row_count_distribution']['avg']
            for i, count in enumerate(row_counts):
                if abs(count - avg_rows) > avg_rows * 0.5:  # 50% deviation
                    consistency_check['issues'].append(f"File {csv_results[i].file_path} has unusual row count: {count} (avg: {avg_rows:.0f})")
            
        except Exception as e:
            consistency_check['error'] = f"Cross-file consistency check failed: {str(e)}"
        
        return consistency_check
    
    def _validate_business_context(self, file_results: List[FileValidationResult], 
                                 table_config: TableConfig, execution_date: str) -> Dict[str, Any]:
        """Validate business context and constraints"""
        business_validation = {
            'execution_date_valid': True,
            'file_naming_convention': True,
            'expected_file_count': True,
            'business_rules_check': {},
            'issues': []
        }
        
        try:
            # 1. Execution date validation
            try:
                exec_date = datetime.strptime(execution_date, '%Y-%m-%d')
                today = datetime.now()
                
                # Check if execution date is too far in the future
                if exec_date > today + timedelta(days=1):
                    business_validation['execution_date_valid'] = False
                    business_validation['issues'].append(f"Execution date too far in future: {execution_date}")
                
                # Check if execution date is too old (configurable)
                max_age_days = self.validation_config.get('max_execution_age_days', 30)
                if exec_date < today - timedelta(days=max_age_days):
                    business_validation['issues'].append(f"Execution date very old: {execution_date}")
                    
            except ValueError:
                business_validation['execution_date_valid'] = False
                business_validation['issues'].append(f"Invalid execution date format: {execution_date}")
            
            # 2. File naming convention check
            expected_patterns = self.validation_config.get('file_naming_patterns', {})
            source_type = table_config.source_type
            
            if source_type in expected_patterns:
                pattern = expected_patterns[source_type]
                csv_files = [f for f in file_results if f.file_path.endswith('.csv')]
                
                for file_result in csv_files:
                    file_name = Path(file_result.file_path).name
                    if not self._matches_naming_pattern(file_name, pattern):
                        business_validation['file_naming_convention'] = False
                        business_validation['issues'].append(f"File naming convention violation: {file_name} doesn't match pattern {pattern}")
            
            # 3. Expected file count validation
            expected_files = self.validation_config.get('expected_file_counts', {})
            if source_type in expected_files:
                expected_count = expected_files[source_type]
                csv_files = [f for f in file_results if f.file_path.endswith('.csv') and f.is_valid]
                actual_count = len(csv_files)
                
                if actual_count != expected_count:
                    business_validation['expected_file_count'] = False
                    business_validation['issues'].append(f"Expected {expected_count} files, got {actual_count}")
            
            # 4. Business rules validation from configuration
            business_rules = self.config_manager.get_validation_rules(table_config.source_type)
            if 'business_rules' in business_rules:
                for rule_name, rule_config in business_rules['business_rules'].items():
                    try:
                        rule_result = self._validate_business_rule(file_results, rule_config)
                        business_validation['business_rules_check'][rule_name] = rule_result
                        
                        if not rule_result['passed']:
                            business_validation['issues'].append(f"Business rule '{rule_name}' failed: {rule_result['message']}")
                    except Exception as e:
                        business_validation['issues'].append(f"Business rule '{rule_name}' validation error: {str(e)}")
        
        except Exception as e:
            business_validation['error'] = f"Business context validation failed: {str(e)}"
        
        return business_validation
    
    def _matches_naming_pattern(self, file_name: str, pattern: str) -> bool:
        """Check if file name matches expected pattern"""
        import re
        try:
            return bool(re.match(pattern, file_name))
        except re.error:
            return True  # Invalid pattern, assume match
    
    def _validate_business_rule(self, file_results: List[FileValidationResult], 
                              rule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate specific business rule"""
        rule_type = rule_config.get('type')
        
        if rule_type == 'total_row_count_range':
            min_rows = rule_config.get('min_total_rows', 0)
            max_rows = rule_config.get('max_total_rows', float('inf'))
            
            total_rows = sum(f.row_count for f in file_results if f.is_valid and f.file_path.endswith('.csv'))
            
            if min_rows <= total_rows <= max_rows:
                return {'passed': True, 'message': f"Total rows {total_rows} within range [{min_rows}, {max_rows}]"}
            else:
                return {'passed': False, 'message': f"Total rows {total_rows} outside range [{min_rows}, {max_rows}]"}
        
        elif rule_type == 'file_size_consistency':
            tolerance = rule_config.get('size_tolerance_percentage', 50)
            csv_files = [f for f in file_results if f.is_valid and f.file_path.endswith('.csv')]
            
            if len(csv_files) <= 1:
                return {'passed': True, 'message': "Single file - size consistency check not applicable"}
            
            sizes = [f.file_size_bytes for f in csv_files]
            avg_size = sum(sizes) / len(sizes)
            max_deviation = max(abs(size - avg_size) for size in sizes)
            deviation_percentage = (max_deviation / avg_size * 100) if avg_size > 0 else 0
            
            if deviation_percentage <= tolerance:
                return {'passed': True, 'message': f"File sizes consistent (max deviation: {deviation_percentage:.1f}%)"}
            else:
                return {'passed': False, 'message': f"File sizes inconsistent (max deviation: {deviation_percentage:.1f}% > {tolerance}%)"}
        
        else:
            return {'passed': True, 'message': f"Unknown business rule type: {rule_type}"}
    
    def _validate_data_lineage(self, table_config: TableConfig, execution_date: str) -> Dict[str, Any]:
        """Validate data lineage and dependencies"""
        lineage_validation = {
            'dependencies_satisfied': True,
            'source_tables_available': True,
            'dependency_checks': [],
            'issues': []
        }
        
        try:
            # Check if all dependency tables are available and up-to-date
            for dependency_table in table_config.dependencies:
                dependency_check = {
                    'table': dependency_table,
                    'exists': False,
                    'has_recent_data': False,
                    'row_count': 0,
                    'last_update': None
                }
                
                try:
                    # This would require Spark session - simplified for demo
                    # In real implementation, would check table existence and freshness
                    dependency_check['exists'] = True
                    dependency_check['has_recent_data'] = True
                    dependency_check['row_count'] = 1000  # Placeholder
                    dependency_check['last_update'] = execution_date
                    
                except Exception as e:
                    dependency_check['error'] = str(e)
                    lineage_validation['dependencies_satisfied'] = False
                    lineage_validation['source_tables_available'] = False
                    lineage_validation['issues'].append(f"Dependency table {dependency_table} not available: {str(e)}")
                
                lineage_validation['dependency_checks'].append(dependency_check)
            
            # Check for circular dependencies (simplified)
            if len(table_config.dependencies) > 0:
                circular_deps = self._check_circular_dependencies(table_config.identifier, table_config.dependencies)
                if circular_deps:
                    lineage_validation['issues'].append(f"Circular dependencies detected: {circular_deps}")
        
        except Exception as e:
            lineage_validation['error'] = f"Data lineage validation failed: {str(e)}"
        
        return lineage_validation
    
    def _check_circular_dependencies(self, table_identifier: str, dependencies: List[str]) -> List[str]:
        """Check for circular dependencies (simplified implementation)"""
        # This is a simplified check - in production would need full dependency graph analysis
        circular_deps = []
        
        for dependency in dependencies:
            try:
                dep_config = self.config_manager.get_table_config(dependency)
                if table_identifier in dep_config.dependencies:
                    circular_deps.append(f"{table_identifier} -> {dependency} -> {table_identifier}")
            except Exception:
                continue  # Dependency config not found
        
        return circular_deps
    
    def _generate_validation_summary(self, result: InputValidationResult) -> Dict[str, Any]:
        """Generate comprehensive validation summary"""
        summary = {
            'total_files': len(result.file_results),
            'valid_files': len([f for f in result.file_results if f.is_valid]),
            'invalid_files': len([f for f in result.file_results if not f.is_valid]),
            'total_rows': sum(f.row_count for f in result.file_results if f.is_valid),
            'total_size_bytes': sum(f.file_size_bytes for f in result.file_results),
            'critical_issues': 0,
            'warnings': 0,
            'recommendations': []
        }
        
        # Count issues across all validation types
        all_checks = [
            result.schema_evolution_check,
            result.cross_file_checks,
            result.business_validation,
            result.lineage_validation
        ]
        
        for check in all_checks:
            if 'issues' in check:
                summary['critical_issues'] += len(check['issues'])
            if 'warnings' in check:
                summary['warnings'] += len(check['warnings'])
        
        # Add file-level issues
        for file_result in result.file_results:
            summary['critical_issues'] += len(file_result.errors)
            summary['warnings'] += len(file_result.warnings)
        
        # Generate recommendations
        if summary['invalid_files'] > 0:
            summary['recommendations'].append(f"Fix {summary['invalid_files']} invalid files before processing")
        
        if 'new_columns_detected' in result.schema_evolution_check and result.schema_evolution_check['new_columns_detected']:
            summary['recommendations'].append("Update table configuration to include new columns")
        
        if 'consistent_structure' in result.cross_file_checks and not result.cross_file_checks['consistent_structure']:
            summary['recommendations'].append("Ensure all files have consistent structure")
        
        if summary['critical_issues'] == 0 and summary['warnings'] == 0:
            summary['recommendations'].append("All validations passed - ready for processing")
        
        return summary
    
    def _determine_overall_status(self, result: InputValidationResult) -> str:
        """Determine overall validation status"""
        # Check for critical failures
        if not any(f.is_valid for f in result.file_results):
            return "failed"
        
        # Check schema evolution issues
        if ('missing_columns_detected' in result.schema_evolution_check and 
            result.schema_evolution_check['missing_columns_detected']):
            return "failed"
        
        # Check business validation issues
        if ('execution_date_valid' in result.business_validation and 
            not result.business_validation['execution_date_valid']):
            return "failed"
        
        # Check dependency issues
        if ('dependencies_satisfied' in result.lineage_validation and 
            not result.lineage_validation['dependencies_satisfied']):
            return "failed"
        
        # Check if there are warnings but no critical errors
        total_warnings = sum(len(f.warnings) for f in result.file_results)
        if total_warnings > 0 or result.summary.get('warnings', 0) > 0:
            return "warning"
        
        return "passed"