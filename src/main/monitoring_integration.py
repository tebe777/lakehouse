# src/main/monitoring_integration.py
"""
Example of integrating monitoring and alerting with ETL pipeline using
the new validation and filename parsing utilities.
"""

from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path

from src.catalog.config_manager import ConfigManager
from src.common.monitoring.logger import ETLLogger
from src.quality.validators.input_validator import ComprehensiveInputValidator
from src.ingestion.raw_loader import RawDataLoader
from src.common.utils.validation_integration import (
    IntegratedDataValidator, 
    FileProcessor,
    load_validation_rules_from_file
)
from src.airflow.utils import create_data_validator, parse_filename


class MonitoredETLPipeline:
    """ETL Pipeline with comprehensive monitoring, alerting, and new validation features"""
    
    def __init__(self, config_path: str, environment: str = "dev"):
        self.config_manager = ConfigManager(config_path, environment)
        self.logger = ETLLogger(self.__class__.__name__)
        
        # Initialize ETL components
        self.input_validator = ComprehensiveInputValidator(
            self.config_manager, 
            temp_dir="/tmp/validation"
        )
        
        # Raw loader would be initialized with Spark session
        # self.raw_loader = RawDataLoader(spark, table_config, ...)
    
    def process_with_enhanced_validation(self, source_path: str, table_identifier: str, 
                                       execution_date: str = None,
                                       validation_rules_file: str = None) -> Dict[str, Any]:
        """
        Process data with enhanced validation using new utilities
        
        Args:
            source_path: Path to source data files
            table_identifier: Table identifier for configuration lookup
            execution_date: Date of execution (ISO format)
            validation_rules_file: Optional path to custom validation rules
        """
        
        if execution_date is None:
            execution_date = datetime.now().isoformat()
            
        try:
            self.logger.info(f"Starting enhanced ETL pipeline for {table_identifier}")
            
            # Step 1: Get table configuration
            table_config = self.config_manager.get_table_config(table_identifier)
            if not table_config:
                raise ValueError(f"Table configuration not found for {table_identifier}")
            
            # Step 2: Enhanced filename parsing and validation
            self.logger.info("Step 2: Enhanced filename parsing and validation")
            file_results = self._process_files_with_enhanced_parsing(source_path, table_config)
            
            # Step 3: Load custom validation rules if provided
            validation_rules = table_config.validation
            if validation_rules_file:
                self.logger.info(f"Loading custom validation rules from {validation_rules_file}")
                try:
                    custom_rules = load_validation_rules_from_file(validation_rules_file)
                    # Merge with table config validation rules
                    validation_rules.update(custom_rules)
                except Exception as e:
                    self.logger.warning(f"Failed to load custom validation rules: {e}")
            
            # Step 4: Comprehensive input validation with new utilities
            self.logger.info("Step 4: Comprehensive input validation")
            validation_result = self.input_validator.validate_input(
                source_path, table_identifier, execution_date
            )
            
            # Step 5: Enhanced data validation using new DataValidator
            self.logger.info("Step 5: Enhanced data validation")
            data_validation_results = []
            
            if validation_result.overall_status != "failed":
                for file_result in file_results:
                    if file_result['validation_status'] == 'passed':
                        try:
                            # Create enhanced validator for this file's data
                            data_validator = create_data_validator(validation_rules)
                            
                            # Note: In real implementation, you would load the file as DataFrame
                            # For demo purposes, we'll simulate the validation result
                            file_validation = {
                                'file_path': file_result['file_path'],
                                'filename_info': file_result['filename_info'],
                                'validation_status': 'passed',
                                'validation_details': {
                                    'null_checks': 'passed',
                                    'date_range_checks': 'passed',
                                    'custom_rules': 'passed'
                                }
                            }
                            data_validation_results.append(file_validation)
                            
                        except Exception as e:
                            self.logger.error(f"Data validation failed for {file_result['file_path']}: {e}")
                            file_validation = {
                                'file_path': file_result['file_path'],
                                'validation_status': 'failed',
                                'error': str(e)
                            }
                            data_validation_results.append(file_validation)
            
            # Step 6: Data processing simulation
            self.logger.info("Step 6: Data processing with enhanced monitoring")
            
            valid_files = [f for f in file_results if f['validation_status'] == 'passed']
            processing_result = {
                'success': True,
                'files_processed': len(valid_files),
                'total_files_found': len(file_results),
                'processing_time_seconds': 45.2,
                'enhanced_validations': len(data_validation_results),
                'errors': [],
                'warnings': []
            }
            
            # Step 7: Final assessment and reporting
            overall_success = (
                validation_result.overall_status != "failed" and
                all(v['validation_status'] == 'passed' for v in data_validation_results)
            )
            
            if not overall_success:
                failed_validations = [v for v in data_validation_results if v['validation_status'] == 'failed']
                processing_result['errors'].extend([f"Validation failed: {v.get('error', 'Unknown error')}" for v in failed_validations])
            
            # Log comprehensive results
            self.logger.info(f"Enhanced ETL pipeline completed")
            self.logger.info(f"Overall success: {overall_success}")
            self.logger.info(f"Files processed: {processing_result['files_processed']}")
            self.logger.info(f"Enhanced validations: {processing_result['enhanced_validations']}")
            
            return {
                'success': overall_success,
                'validation_result': validation_result.dict() if hasattr(validation_result, 'dict') else validation_result,
                'file_parsing_results': file_results,
                'data_validation_results': data_validation_results,
                'processing_result': processing_result,
                'execution_date': execution_date,
                'table_identifier': table_identifier
            }
            
        except Exception as e:
            # Log critical error with enhanced context
            error_msg = f"CRITICAL ERROR during enhanced ETL processing: {str(e)}"
            self.logger.error(error_msg)
            
            return {
                'success': False,
                'error': str(e),
                'execution_date': execution_date,
                'table_identifier': table_identifier,
                'pipeline_stage': 'enhanced_processing'
            }
    
    def _process_files_with_enhanced_parsing(self, source_path: str, table_config) -> List[Dict[str, Any]]:
        """
        Process files using enhanced filename parsing
        
        Args:
            source_path: Path to source files
            table_config: Table configuration object
            
        Returns:
            List of file processing results
        """
        self.logger.info(f"Processing files with enhanced parsing from: {source_path}")
        
        results = []
        source_path_obj = Path(source_path)
        
        if source_path_obj.is_file():
            # Single file
            file_paths = [source_path_obj]
        elif source_path_obj.is_dir():
            # Directory - find relevant files
            file_paths = list(source_path_obj.glob("*.csv.ZIP")) + list(source_path_obj.glob("*.csv"))
        else:
            self.logger.warning(f"Source path does not exist: {source_path}")
            return results
        
        # Process each file
        file_processor = FileProcessor(table_config)
        
        for file_path in file_paths:
            try:
                self.logger.info(f"Processing file: {file_path}")
                
                # Use enhanced filename parsing
                file_result = file_processor.process_file_metadata(str(file_path))
                
                # Additional filename parsing using parse_filename utility
                try:
                    enhanced_filename_info = parse_filename(str(file_path.name))
                    file_result['enhanced_filename_info'] = enhanced_filename_info
                except ValueError as e:
                    file_result['enhanced_filename_info'] = None
                    file_result.setdefault('warnings', []).append(f"Enhanced filename parsing failed: {e}")
                
                results.append(file_result)
                
            except Exception as e:
                self.logger.error(f"Failed to process file {file_path}: {e}")
                results.append({
                    'file_path': str(file_path),
                    'validation_status': 'failed',
                    'error': str(e)
                })
        
        self.logger.info(f"Processed {len(results)} files with enhanced parsing")
        return results
    
    def get_enhanced_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status with enhanced monitoring capabilities"""
        return {
            'pipeline_name': 'EnhancedMonitoredETLPipeline',
            'status': 'healthy',
            'last_execution': datetime.now().isoformat(),
            'components': {
                'config_manager': 'initialized',
                'input_validator': 'initialized',
                'enhanced_validator': 'available',
                'filename_parser': 'available',
                'raw_loader': 'pending_spark_session'
            },
            'capabilities': {
                'enhanced_filename_parsing': True,
                'custom_validation_rules': True,
                'integrated_monitoring': True,
                'comprehensive_file_validation': True
            }
        }