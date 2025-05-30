# src/quality/validators/data_validator.py
from typing import Dict, Any, List, Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, min as spark_min, max as spark_max
import re

from src.common.models.interfaces import IValidator
from src.common.models.table_config import ValidationRule
from src.common.monitoring.logger import ETLLogger


class ValidationError(Exception):
    """Raised when critical validation rules fail"""
    pass


class DataValidator(IValidator):
    """
    Comprehensive data validation framework for data quality assurance.
    
    Features:
    - Configurable validation rules
    - Statistical profiling
    - Business rule validation
    - Custom SQL validation
    - Detailed validation reporting
    - Integration with alerting system
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = ETLLogger(self.__class__.__name__)
        
        # Register validation rule engines
        self.rule_engines = {
            'not_null': self._validate_not_null,
            'min_max': self._validate_min_max,
            'regex_pattern': self._validate_regex_pattern,
            'unique_values': self._validate_unique_values,
            'referential_integrity': self._validate_referential_integrity,
            'custom_sql': self._validate_custom_sql,
            'row_count': self._validate_row_count,
            'column_stats': self._validate_column_stats,
            'data_freshness': self._validate_data_freshness
        }
    
    def validate(self, df: DataFrame, rules: List[ValidationRule]) -> Dict[str, Any]:
        """
        Execute all validation rules against DataFrame
        
        Args:
            df: DataFrame to validate
            rules: List of validation rules to execute
            
        Returns:
            Comprehensive validation results dictionary
        """
        self.logger.info(f"Starting data validation with {len(rules)} rules")
        
        # Initialize results structure
        results = self._initialize_validation_results(df, rules)
        
        # Execute each validation rule
        for i, rule in enumerate(rules):
            try:
                self.logger.debug(f"Executing rule {i+1}/{len(rules)}: {rule.rule_type}")
                
                rule_result = self._execute_validation_rule(df, rule)
                self._process_rule_result(results, rule, rule_result)
                
            except Exception as e:
                error_msg = f"Rule execution failed: {rule.rule_type} - {str(e)}"
                self.logger.error(error_msg)
                results['errors'].append({
                    'rule_type': rule.rule_type,
                    'column': rule.column,
                    'error': error_msg,
                    'severity': rule.severity
                })
                if rule.severity == 'error':
                    results['has_errors'] = True
        
        # Calculate final statistics
        self._finalize_validation_results(results)
        
        self.logger.info(f"Validation completed. Passed: {results['passed_rules']}, "
                        f"Failed: {results['failed_rules']}, "
                        f"Warnings: {results['warnings']}")
        
        return results
    
    def _initialize_validation_results(self, df: DataFrame, rules: List[ValidationRule]) -> Dict[str, Any]:
        """Initialize validation results structure"""
        return {
            'table_name': self._get_table_name(df),
            'validation_timestamp': datetime.now().isoformat(),
            'total_rules': len(rules),
            'passed_rules': 0,
            'failed_rules': 0,
            'warnings': 0,
            'errors': [],
            'warnings_list': [],
            'rule_results': [],
            'has_errors': False,
            'has_warnings': False,
            'row_count': df.count(),
            'column_count': len(df.columns),
            'data_quality_score': 0.0,
            'profiling_stats': self._generate_basic_profiling(df)
        }
    
    def _get_table_name(self, df: DataFrame) -> str:
        """Extract table name from DataFrame if available"""
        try:
            if hasattr(df, 'sql_ctx') and hasattr(df.sql_ctx, 'table_name'):
                return df.sql_ctx.table_name
            return "unknown_table"
        except:
            return "unknown_table"
    
    def _generate_basic_profiling(self, df: DataFrame) -> Dict[str, Any]:
        """Generate basic statistical profiling of DataFrame"""
        try:
            stats = {}
            
            # Basic counts
            stats['row_count'] = df.count()
            stats['column_count'] = len(df.columns)
            
            # Null counts per column
            null_counts = {}
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
                null_counts[col_name] = {
                    'null_count': null_count,
                    'null_percentage': (null_count / stats['row_count']) * 100 if stats['row_count'] > 0 else 0
                }
            
            stats['null_analysis'] = null_counts
            
            return stats
            
        except Exception as e:
            self.logger.warning(f"Failed to generate profiling stats: {str(e)}")
            return {}
    
    def _execute_validation_rule(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Execute single validation rule"""
        if rule.rule_type not in self.rule_engines:
            raise ValueError(f"Unknown validation rule type: {rule.rule_type}")
        
        engine = self.rule_engines[rule.rule_type]
        return engine(df, rule)
    
    def _process_rule_result(self, results: Dict[str, Any], rule: ValidationRule, 
                           rule_result: Dict[str, Any]) -> None:
        """Process result of single validation rule"""
        rule_result['rule_config'] = {
            'rule_type': rule.rule_type,
            'column': rule.column,
            'parameters': rule.parameters,
            'severity': rule.severity
        }
        
        results['rule_results'].append(rule_result)
        
        if rule_result.get('passed', False):
            results['passed_rules'] += 1
        else:
            if rule.severity == 'error':
                results['failed_rules'] += 1
                results['errors'].append(rule_result)
                results['has_errors'] = True
            else:
                results['warnings'] += 1
                results['warnings_list'].append(rule_result)
                results['has_warnings'] = True
    
    def _finalize_validation_results(self, results: Dict[str, Any]) -> None:
        """Calculate final validation statistics"""
        total_rules = results['total_rules']
        if total_rules > 0:
            results['data_quality_score'] = (results['passed_rules'] / total_rules) * 100
        else:
            results['data_quality_score'] = 100.0
    
    # ==========================================
    # VALIDATION RULE IMPLEMENTATIONS
    # ==========================================
    
    def _validate_not_null(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate NOT NULL constraint"""
        if not rule.column:
            raise ValueError("Column name required for not_null validation")
        
        null_count = df.filter(col(rule.column).isNull()).count()
        total_count = df.count()
        
        return {
            'rule_type': 'not_null',
            'column': rule.column,
            'passed': null_count == 0,
            'null_count': null_count,
            'total_count': total_count,
            'null_percentage': (null_count / total_count * 100) if total_count > 0 else 0,
            'message': f"Found {null_count} null values in column '{rule.column}' ({null_count/total_count*100:.2f}%)"
        }
    
    def _validate_min_max(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate min/max value constraints"""
        if not rule.column:
            raise ValueError("Column name required for min_max validation")
        
        params = rule.parameters
        min_val = params.get('min')
        max_val = params.get('max')
        
        # Get actual min/max values
        stats = df.select(
            spark_min(col(rule.column)).alias('actual_min'),
            spark_max(col(rule.column)).alias('actual_max')
        ).collect()[0]
        
        actual_min = stats['actual_min']
        actual_max = stats['actual_max']
        
        violations = 0
        messages = []
        
        # Check min constraint
        if min_val is not None and actual_min is not None and actual_min < min_val:
            violations += df.filter(col(rule.column) < min_val).count()
            messages.append(f"Values below minimum {min_val}: actual min {actual_min}")
        
        # Check max constraint  
        if max_val is not None and actual_max is not None and actual_max > max_val:
            violations += df.filter(col(rule.column) > max_val).count()
            messages.append(f"Values above maximum {max_val}: actual max {actual_max}")
        
        return {
            'rule_type': 'min_max',
            'column': rule.column,
            'passed': violations == 0,
            'violations': violations,
            'actual_min': actual_min,
            'actual_max': actual_max,
            'expected_min': min_val,
            'expected_max': max_val,
            'message': '; '.join(messages) if messages else f"Min/max validation passed for column '{rule.column}'"
        }
    
    def _validate_regex_pattern(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate column values against regex pattern"""
        if not rule.column:
            raise ValueError("Column name required for regex_pattern validation")
        
        pattern = rule.parameters.get('pattern')
        if not pattern:
            raise ValueError("Pattern parameter required for regex_pattern validation")
        
        # Count rows that don't match pattern
        violations = df.filter(~col(rule.column).rlike(pattern)).count()
        total_count = df.count()
        
        return {
            'rule_type': 'regex_pattern',
            'column': rule.column,
            'passed': violations == 0,
            'violations': violations,
            'total_count': total_count,
            'pattern': pattern,
            'message': f"Found {violations} values not matching pattern '{pattern}' in column '{rule.column}'"
        }
    
    def _validate_unique_values(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate column has unique values (no duplicates)"""
        if not rule.column:
            raise ValueError("Column name required for unique_values validation")
        
        total_count = df.count()
        distinct_count = df.select(rule.column).distinct().count()
        duplicates = total_count - distinct_count
        
        return {
            'rule_type': 'unique_values',
            'column': rule.column,
            'passed': duplicates == 0,
            'total_count': total_count,
            'distinct_count': distinct_count,
            'duplicate_count': duplicates,
            'message': f"Found {duplicates} duplicate values in column '{rule.column}'"
        }
    
    def _validate_referential_integrity(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate referential integrity with another table"""
        params = rule.parameters
        ref_table = params.get('reference_table')
        ref_column = params.get('reference_column', rule.column)
        
        if not ref_table:
            raise ValueError("reference_table parameter required for referential_integrity validation")
        
        # Load reference table
        ref_df = self.spark.table(ref_table)
        
        # Find orphaned records
        orphaned = df.join(
            ref_df.select(ref_column).distinct(),
            df[rule.column] == ref_df[ref_column],
            'left_anti'
        ).count()
        
        return {
            'rule_type': 'referential_integrity',
            'column': rule.column,
            'passed': orphaned == 0,
            'orphaned_records': orphaned,
            'reference_table': ref_table,
            'reference_column': ref_column,
            'message': f"Found {orphaned} orphaned records in column '{rule.column}' not found in {ref_table}.{ref_column}"
        }
    
    def _validate_custom_sql(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate using custom SQL query"""
        sql_query = rule.parameters.get('sql')
        if not sql_query:
            raise ValueError("sql parameter required for custom_sql validation")
        
        # Create temporary view for the DataFrame
        temp_view_name = f"temp_validation_view_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df.createOrReplaceTempView(temp_view_name)
        
        try:
            # Execute custom SQL - should return count of violations
            result_df = self.spark.sql(sql_query.replace('${table}', temp_view_name))
            violations = result_df.collect()[0][0]  # Assume first column, first row is violation count
            
            return {
                'rule_type': 'custom_sql',
                'column': rule.column,
                'passed': violations == 0,
                'violations': violations,
                'sql_query': sql_query,
                'message': f"Custom SQL validation found {violations} violations"
            }
        finally:
            # Clean up temporary view
            self.spark.catalog.dropTempView(temp_view_name)
    
    def _validate_row_count(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate row count is within expected range"""
        params = rule.parameters
        min_rows = params.get('min_rows', 0)
        max_rows = params.get('max_rows')
        
        actual_count = df.count()
        
        violations = []
        if actual_count < min_rows:
            violations.append(f"Row count {actual_count} below minimum {min_rows}")
        
        if max_rows is not None and actual_count > max_rows:
            violations.append(f"Row count {actual_count} above maximum {max_rows}")
        
        return {
            'rule_type': 'row_count',
            'column': None,
            'passed': len(violations) == 0,
            'actual_count': actual_count,
            'min_expected': min_rows,
            'max_expected': max_rows,
            'message': '; '.join(violations) if violations else f"Row count validation passed: {actual_count} rows"
        }
    
    def _validate_column_stats(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate column statistical properties"""
        if not rule.column:
            raise ValueError("Column name required for column_stats validation")
        
        params = rule.parameters
        expected_mean = params.get('expected_mean')
        expected_std = params.get('expected_std')
        tolerance = params.get('tolerance', 0.1)  # 10% tolerance by default
        
        # Calculate statistics
        stats = df.select(
            col(rule.column).alias('value')
        ).summary('mean', 'stddev').collect()
        
        actual_mean = float(stats[0]['value'])
        actual_std = float(stats[1]['value'])
        
        violations = []
        
        if expected_mean is not None:
            if abs(actual_mean - expected_mean) > (expected_mean * tolerance):
                violations.append(f"Mean {actual_mean} deviates from expected {expected_mean}")
        
        if expected_std is not None:
            if abs(actual_std - expected_std) > (expected_std * tolerance):
                violations.append(f"Std dev {actual_std} deviates from expected {expected_std}")
        
        return {
            'rule_type': 'column_stats',
            'column': rule.column,
            'passed': len(violations) == 0,
            'actual_mean': actual_mean,
            'actual_std': actual_std,
            'expected_mean': expected_mean,
            'expected_std': expected_std,
            'tolerance': tolerance,
            'message': '; '.join(violations) if violations else f"Statistical validation passed for column '{rule.column}'"
        }
    
    def _validate_data_freshness(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """Validate data freshness based on timestamp column"""
        if not rule.column:
            raise ValueError("Column name required for data_freshness validation")
        
        params = rule.parameters
        max_age_hours = params.get('max_age_hours', 24)
        
        from pyspark.sql.functions import max as spark_max, unix_timestamp, current_timestamp
        
        # Get latest timestamp in data
        latest_timestamp = df.select(spark_max(col(rule.column))).collect()[0][0]
        
        if latest_timestamp is None:
            return {
                'rule_type': 'data_freshness',
                'column': rule.column,
                'passed': False,
                'latest_timestamp': None,
                'max_age_hours': max_age_hours,
                'message': f"No valid timestamps found in column '{rule.column}'"
            }
        
        # Calculate age in hours
        current_time = datetime.now()
        data_age_hours = (current_time - latest_timestamp).total_seconds() / 3600
        
        return {
            'rule_type': 'data_freshness',
            'column': rule.column,
            'passed': data_age_hours <= max_age_hours,
            'latest_timestamp': latest_timestamp.isoformat(),
            'data_age_hours': data_age_hours,
            'max_age_hours': max_age_hours,
            'message': f"Data age {data_age_hours:.1f} hours {'exceeds' if data_age_hours > max_age_hours else 'within'} limit of {max_age_hours} hours"
        }
