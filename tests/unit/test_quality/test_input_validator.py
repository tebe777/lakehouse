# tests/unit/test_quality/test_input_validator.py
import unittest
import tempfile
import zipfile
import csv
import os
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.quality.validators.input_validator import (
    ComprehensiveInputValidator, FileValidationResult, InputValidationResult
)
from src.common.utils.config_manager import ConfigManager
from src.common.models.table_config import TableConfig, LayerType


class TestComprehensiveInputValidator(unittest.TestCase):
    """Unit tests for ComprehensiveInputValidator"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = tempfile.mkdtemp()
        
        # Create mock configuration
        self.mock_config_manager = Mock(spec=ConfigManager)
        self.mock_table_config = Mock(spec=TableConfig)
        self.mock_table_config.identifier = "raw.test.customers"
        self.mock_table_config.source_type = "test"
        self.mock_table_config.schema = {
            "customer_id": "long",
            "first_name": "string",
            "last_name": "string",
            "email": "string"
        }
        self.mock_table_config.dependencies = []
        
        # Mock environment config
        self.mock_env_config = {
            'input_validation': {
                'max_file_size_mb': 100,
                'min_file_size_bytes': 10,
                'max_row_count': 1000000,
                'min_row_count': 1,
                'allowed_encodings': ['utf-8', 'latin1'],
                'max_schema_drift_percentage': 20,
                'file_naming_patterns': {
                    'test': r'^test_.*\.csv$'
                },
                'expected_file_counts': {
                    'test': 2
                }
            }
        }
        
        self.mock_config_manager.get_environment_config.return_value = self.mock_env_config
        self.mock_config_manager.get_table_config.return_value = self.mock_table_config
        self.mock_config_manager.get_validation_rules.return_value = {
            'business_rules': {
                'total_volume': {
                    'type': 'total_row_count_range',
                    'min_total_rows': 100,
                    'max_total_rows': 10000
                }
            }
        }
        
        # Initialize validator
        self.validator = ComprehensiveInputValidator(
            self.mock_config_manager, 
            self.temp_dir
        )
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.config_dir, ignore_errors=True)
    
    def create_test_csv(self, filename: str, rows: int = 100, 
                       columns: list = None, encoding: str = 'utf-8') -> str:
        """Helper method to create test CSV file"""
        if columns is None:
            columns = ['customer_id', 'first_name', 'last_name', 'email']
        
        file_path = os.path.join(self.temp_dir, filename)
        
        with open(file_path, 'w', newline='', encoding=encoding) as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(columns)
            
            # Write data rows
            for i in range(rows):
                row_data = []
                for col in columns:
                    if 'id' in col.lower():
                        row_data.append(str(i + 1))
                    elif 'email' in col.lower():
                        row_data.append(f'user{i+1}@test.com')
                    else:
                        row_data.append(f'value_{col}_{i+1}')
                writer.writerow(row_data)
        
        return file_path
    
    def create_test_zip(self, filename: str, csv_files: list) -> str:
        """Helper method to create test ZIP file with CSV files"""
        zip_path = os.path.join(self.temp_dir, filename)
        
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for csv_file in csv_files:
                zipf.write(csv_file, os.path.basename(csv_file))
        
        return zip_path
    
    # ==========================================
    # FILE VALIDATION TESTS
    # ==========================================
    
    def test_validate_csv_file_valid(self):
        """Test validation of valid CSV file"""
        csv_file = self.create_test_csv("test_customers.csv", rows=50)
        
        result = self.validator._validate_csv_file(csv_file, self.mock_table_config)
        
        self.assertIsInstance(result, FileValidationResult)
        self.assertTrue(result.is_valid)
        self.assertEqual(result.row_count, 50)
        self.assertEqual(result.column_count, 4)
        self.assertEqual(result.encoding, 'utf-8')
        self.assertEqual(result.delimiter, ',')
        self.assertEqual(len(result.errors), 0)
    
    def test_validate_csv_file_missing_columns(self):
        """Test validation of CSV file with missing required columns"""
        csv_file = self.create_test_csv("test_incomplete.csv", 
                                       columns=['customer_id', 'first_name'])  # Missing columns
        
        result = self.validator._validate_csv_file(csv_file, self.mock_table_config)
        
        self.assertFalse(result.is_valid)
        self.assertTrue(any('Missing required column' in error for error in result.errors))
    
    def test_validate_csv_file_extra_columns(self):
        """Test validation of CSV file with extra columns (schema evolution)"""
        csv_file = self.create_test_csv("test_extra.csv", 
                                       columns=['customer_id', 'first_name', 'last_name', 
                                               'email', 'phone', 'address'])  # Extra columns
        
        result = self.validator._validate_csv_file(csv_file, self.mock_table_config)
        
        # Should be valid but with warnings about new columns
        self.assertTrue(result.is_valid)
        self.assertTrue(any('New column detected' in warning for warning in result.warnings))
    
    def test_validate_csv_file_too_many_rows(self):
        """Test validation of CSV file exceeding row limit"""
        # Set low limit for testing
        self.validator.max_row_count = 10
        
        csv_file = self.create_test_csv("test_large.csv", rows=20)
        
        result = self.validator._validate_csv_file(csv_file, self.mock_table_config)
        
        self.assertFalse(result.is_valid)
        self.assertTrue(any('Too many rows' in error for error in result.errors))
    
    def test_validate_csv_file_encoding_detection(self):
        """Test encoding detection for different file encodings"""
        csv_file = self.create_test_csv("test_latin1.csv", encoding='latin1')
        
        result = self.validator._validate_csv_file(csv_file, self.mock_table_config)
        
        # Should detect latin1 encoding
        self.assertEqual(result.encoding, 'latin1')
        self.assertTrue(result.is_valid)
    
    def test_validate_zip_file_valid(self):
        """Test validation of valid ZIP file"""
        # Create test CSV files
        csv1 = self.create_test_csv("test1.csv", rows=30)
        csv2 = self.create_test_csv("test2.csv", rows=40)
        
        # Create ZIP file
        zip_file = self.create_test_zip("test_archive.zip", [csv1, csv2])
        
        results = self.validator._validate_zip_file(zip_file, self.mock_table_config)
        
        # Should have 3 results: 1 ZIP + 2 CSV
        self.assertEqual(len(results), 3)
        
        # ZIP file result
        zip_result = results[0]
        self.assertTrue(zip_result.is_valid)
        self.assertTrue(zip_result.file_path.endswith('.zip'))
        
        # CSV file results
        csv_results = results[1:]
        self.assertTrue(all(r.is_valid for r in csv_results))
        self.assertEqual(sum(r.row_count for r in csv_results), 70)  # 30 + 40
    
    def test_validate_zip_file_corrupted(self):
        """Test validation of corrupted ZIP file"""
        # Create invalid ZIP file
        corrupted_zip = os.path.join(self.temp_dir, "corrupted.zip")
        with open(corrupted_zip, 'w') as f:
            f.write("This is not a valid ZIP file")
        
        results = self.validator._validate_zip_file(corrupted_zip, self.mock_table_config)
        
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].is_valid)
        self.assertTrue(any('Failed to read ZIP file' in error for error in results[0].errors))
    
    def test_validate_zip_file_no_csv_files(self):
        """Test validation of ZIP file with no CSV files"""
        # Create ZIP with non-CSV files
        text_file = os.path.join(self.temp_dir, "readme.txt")
        with open(text_file, 'w') as f:
            f.write("This is a text file")
        
        zip_file = self.create_test_zip("no_csv.zip", [text_file])
        
        results = self.validator._validate_zip_file(zip_file, self.mock_table_config)
        
        zip_result = results[0]
        self.assertFalse(zip_result.is_valid)
        self.assertTrue(any('No CSV files found' in error for error in zip_result.errors))
    
    # ==========================================
    # SCHEMA VALIDATION TESTS
    # ==========================================
    
    def test_schema_evolution_detection_new_columns(self):
        """Test detection of new columns in schema evolution"""
        # Create file results with extra columns
        file_result = FileValidationResult(
            file_path="test.csv",
            is_valid=True,
            file_size_bytes=1000,
            row_count=100,
            column_count=6
        )
        file_result.metadata = {
            'column_names': ['customer_id', 'first_name', 'last_name', 'email', 'phone', 'address']
        }
        
        evolution_check = self.validator._validate_schema_evolution([file_result], self.mock_table_config)
        
        self.assertEqual(len(evolution_check['new_columns_detected']), 2)  # phone, address
        self.assertIn('phone', evolution_check['new_columns_detected'])
        self.assertIn('address', evolution_check['new_columns_detected'])
        self.assertIn('New columns detected', evolution_check['recommendation'])
    
    def test_schema_evolution_missing_columns(self):
        """Test detection of missing required columns"""
        file_result = FileValidationResult(
            file_path="test.csv",
            is_valid=True,
            file_size_bytes=1000,
            row_count=100,
            column_count=2
        )
        file_result.metadata = {
            'column_names': ['customer_id', 'first_name']  # Missing last_name, email
        }
        
        evolution_check = self.validator._validate_schema_evolution([file_result], self.mock_table_config)
        
        self.assertEqual(len(evolution_check['missing_columns_detected']), 2)  # last_name, email
        self.assertIn('CRITICAL', evolution_check['recommendation'])
    
    def test_schema_consistency_across_files(self):
        """Test schema consistency across multiple files"""
        # File 1 with standard columns
        file1 = FileValidationResult("test1.csv", True, 1000, 100, 4)
        file1.metadata = {'column_names': ['customer_id', 'first_name', 'last_name', 'email']}
        
        # File 2 with different columns
        file2 = FileValidationResult("test2.csv", True, 1000, 100, 3)
        file2.metadata = {'column_names': ['customer_id', 'first_name', 'last_name']}
        
        evolution_check = self.validator._validate_schema_evolution([file1, file2], self.mock_table_config)
        
        self.assertFalse(evolution_check['schema_consistent_across_files'])
    
    # ==========================================
    # CROSS-FILE VALIDATION TESTS
    # ==========================================
    
    def test_cross_file_consistency_valid(self):
        """Test cross-file consistency with consistent files"""
        file1 = FileValidationResult("test1.csv", True, 1000, 100, 4, encoding='utf-8', delimiter=',')
        file2 = FileValidationResult("test2.csv", True, 1200, 120, 4, encoding='utf-8', delimiter=',')
        
        consistency_check = self.validator._validate_cross_file_consistency([file1, file2], self.mock_table_config)
        
        self.assertTrue(consistency_check['consistent_encoding'])
        self.assertTrue(consistency_check['consistent_delimiter'])
        self.assertTrue(consistency_check['consistent_structure'])
        self.assertEqual(len(consistency_check['issues']), 0)
    
    def test_cross_file_consistency_inconsistent_encoding(self):
        """Test cross-file consistency with inconsistent encodings"""
        file1 = FileValidationResult("test1.csv", True, 1000, 100, 4, encoding='utf-8')
        file2 = FileValidationResult("test2.csv", True, 1200, 120, 4, encoding='latin1')
        
        consistency_check = self.validator._validate_cross_file_consistency([file1, file2], self.mock_table_config)
        
        self.assertFalse(consistency_check['consistent_encoding'])
        self.assertTrue(any('Inconsistent encodings' in issue for issue in consistency_check['issues']))
    
    def test_cross_file_consistency_row_count_outlier(self):
        """Test detection of row count outliers"""
        file1 = FileValidationResult("test1.csv", True, 1000, 100, 4)
        file2 = FileValidationResult("test2.csv", True, 1200, 120, 4)
        file3 = FileValidationResult("test3.csv", True, 5000, 500, 4)  # Outlier
        
        consistency_check = self.validator._validate_cross_file_consistency([file1, file2, file3], self.mock_table_config)
        
        self.assertTrue(any('unusual row count' in issue for issue in consistency_check['issues']))
    
    # ==========================================
    # BUSINESS VALIDATION TESTS
    # ==========================================
    
    def test_business_validation_valid_execution_date(self):
        """Test business validation with valid execution date"""
        execution_date = datetime.now().strftime('%Y-%m-%d')
        
        business_validation = self.validator._validate_business_context([], self.mock_table_config, execution_date)
        
        self.assertTrue(business_validation['execution_date_valid'])
    
    def test_business_validation_invalid_execution_date(self):
        """Test business validation with invalid execution date"""
        # Date too far in future
        future_date = (datetime.now() + timedelta(days=10)).strftime('%Y-%m-%d')
        
        business_validation = self.validator._validate_business_context([], self.mock_table_config, future_date)
        
        self.assertFalse(business_validation['execution_date_valid'])
        self.assertTrue(any('too far in future' in issue for issue in business_validation['issues']))
    
    def test_business_validation_file_naming_convention(self):
        """Test file naming convention validation"""
        # Valid file name
        valid_file = FileValidationResult("test_customers.csv", True, 1000, 100, 4)
        
        # Invalid file name
        invalid_file = FileValidationResult("wrong_name.csv", True, 1000, 100, 4)
        
        business_validation = self.validator._validate_business_context([valid_file, invalid_file], self.mock_table_config, '2025-05-31')
        
        self.assertFalse(business_validation['file_naming_convention'])
        self.assertTrue(any('naming convention violation' in issue for issue in business_validation['issues']))
    
    def test_business_validation_expected_file_count(self):
        """Test expected file count validation"""
        # Only 1 file when 2 expected
        single_file = [FileValidationResult("test1.csv", True, 1000, 100, 4)]
        
        business_validation = self.validator._validate_business_context(single_file, self.mock_table_config, '2025-05-31')
        
        self.assertFalse(business_validation['expected_file_count'])
        self.assertTrue(any('Expected 2 files, got 1' in issue for issue in business_validation['issues']))
    
    @patch('src.quality.validators.input_validator.ComprehensiveInputValidator._validate_business_rule')
    def test_business_rule_validation(self, mock_validate_rule):
        """Test custom business rule validation"""
        mock_validate_rule.return_value = {'passed': False, 'message': 'Rule failed'}
        
        business_validation = self.validator._validate_business_context([], self.mock_table_config, '2025-05-31')
        
        self.assertTrue(any('Business rule' in issue and 'failed' in issue for issue in business_validation['issues']))
    
    # ==========================================
    # INTEGRATION TESTS
    # ==========================================
    
    def test_validate_input_complete_success(self):
        """Test complete input validation with successful result"""
        # Create valid test data
        csv1 = self.create_test_csv("test_file1.csv", rows=50)
        csv2 = self.create_test_csv("test_file2.csv", rows=60)
        zip_file = self.create_test_zip("test_data.zip", [csv1, csv2])
        
        # Update expected file count for this test
        self.mock_env_config['input_validation']['expected_file_counts']['test'] = 2
        
        result = self.validator.validate_input(zip_file, "raw.test.customers", "2025-05-31")
        
        self.assertIsInstance(result, InputValidationResult)
        self.assertEqual(result.overall_status, "passed")
        self.assertEqual(result.summary['valid_files'], 2)
        self.assertEqual(result.summary['total_rows'], 110)  # 50 + 60
        self.assertEqual(result.summary['critical_issues'], 0)
    
    def test_validate_input_schema_evolution_warning(self):
        """Test input validation with schema evolution warnings"""
        # Create CSV with extra columns
        csv_file = self.create_test_csv("test_extra.csv", 
                                       columns=['customer_id', 'first_name', 'last_name', 'email', 'phone'])
        zip_file = self.create_test_zip("test_data.zip", [csv_file])
        
        # Update expected file count
        self.mock_env_config['input_validation']['expected_file_counts']['test'] = 1
        
        result = self.validator.validate_input(zip_file, "raw.test.customers", "2025-05-31")
        
        self.assertEqual(result.overall_status, "warning")
        self.assertTrue(len(result.schema_evolution_check['new_columns_detected']) > 0)
    
    def test_validate_input_critical_failure(self):
        """Test input validation with critical failures"""
        # Create CSV with missing required columns
        csv_file = self.create_test_csv("test_missing.csv", 
                                       columns=['customer_id', 'first_name'])  # Missing required columns
        zip_file = self.create_test_zip("test_data.zip", [csv_file])
        
        result = self.validator.validate_input(zip_file, "raw.test.customers", "2025-05-31")
        
        self.assertEqual(result.overall_status, "failed")
        self.assertTrue(len(result.schema_evolution_check['missing_columns_detected']) > 0)
    
    def test_validate_input_nonexistent_file(self):
        """Test validation of non-existent input file"""
        result = self.validator.validate_input("/nonexistent/file.zip", "raw.test.customers", "2025-05-31")
        
        self.assertEqual(result.overall_status, "failed")
        self.assertTrue(any('does not exist' in f.errors[0] for f in result.file_results if f.errors))