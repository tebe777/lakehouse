"""
Data quality validator for comprehensive data validation.
Supports various validation rules and generates detailed reports.
"""

import re
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull, regexp_extract, length

from ..common.models import TableConfiguration
from ..common.exceptions import ValidationError

logger = structlog.get_logger(__name__)


class ValidationSeverity(Enum):
    """Validation rule severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationRule:
    """Data validation rule definition."""
    rule_id: str
    rule_name: str
    rule_type: str
    column_name: Optional[str]
    parameters: Dict[str, Any]
    severity: ValidationSeverity
    description: str
    enabled: bool = True


@dataclass
class ValidationResult:
    """Result of a validation rule execution."""
    rule_id: str
    rule_name: str
    column_name: Optional[str]
    severity: ValidationSeverity
    passed: bool
    failed_count: int
    total_count: int
    failure_rate: float
    message: str
    details: Dict[str, Any]
    execution_time_ms: float


class DataQualityValidator:
    """
    Comprehensive data quality validator.
    Executes validation rules and generates quality reports.
    """
    
    def __init__(self, spark: SparkSession, table_config: TableConfiguration,
                 metrics_collector: Optional[Any] = None):
        """
        Initialize data quality validator.
        
        Args:
            spark: Spark session
            table_config: Table configuration
            metrics_collector: Optional metrics collector
        """
        self.spark = spark
        self.table_config = table_config
        self.metrics_collector = metrics_collector
        self.logger = logger.bind(table_id=table_config.table_id)
        
    def validate_data(self, df: DataFrame, validation_config_id: str = None,
                     fail_on_error: bool = True, generate_report: bool = True) -> Dict[str, Any]:
        """
        Validate DataFrame against configured rules.
        
        Args:
            df: DataFrame to validate
            validation_config_id: Configuration ID for validation rules
            fail_on_error: Whether to fail on validation errors
            generate_report: Whether to generate detailed report
            
        Returns:
            Validation results and metrics
        """
        try:
            self.logger.info("Starting data quality validation",
                           validation_config_id=validation_config_id,
                           record_count=df.count())
            
            # Get validation rules
            validation_rules = self._get_validation_rules(validation_config_id)
            
            # Execute validation rules
            results = []
            for rule in validation_rules:
                if rule.enabled:
                    result = self._execute_validation_rule(df, rule)
                    results.append(result)
            
            # Analyze results
            validation_summary = self._analyze_validation_results(results)
            
            # Generate report if requested
            if generate_report:
                report = self._generate_validation_report(results, validation_summary)
                validation_summary['report'] = report
            
            # Check if validation passed
            has_errors = any(not r.passed and r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL] 
                           for r in results)
            
            if has_errors and fail_on_error:
                error_rules = [r for r in results if not r.passed and r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]]
                error_messages = [f"{r.rule_name}: {r.message}" for r in error_rules]
                raise ValidationError(f"Data validation failed: {'; '.join(error_messages)}")
            
            # Collect metrics
            if self.metrics_collector:
                self.metrics_collector.record_validation_metrics(validation_summary)
            
            self.logger.info("Data quality validation completed",
                           total_rules=len(results),
                           passed_rules=validation_summary['passed_rules'],
                           failed_rules=validation_summary['failed_rules'])
            
            return validation_summary
            
        except Exception as e:
            self.logger.error("Data quality validation failed", error=str(e))
            raise ValidationError(f"Validation failed: {str(e)}") from e
    
    def _get_validation_rules(self, validation_config_id: str = None) -> List[ValidationRule]:
        """
        Get validation rules from table configuration.
        
        Args:
            validation_config_id: Configuration ID for validation rules
            
        Returns:
            List of validation rules
        """
        rules = []
        
        # Get rules from table configuration
        if hasattr(self.table_config, 'validation_rules') and self.table_config.validation_rules:
            for rule_config in self.table_config.validation_rules:
                rule = ValidationRule(
                    rule_id=rule_config.get('rule_id', f"rule_{len(rules)}"),
                    rule_name=rule_config.get('rule_name', 'Unknown Rule'),
                    rule_type=rule_config.get('rule_type', 'custom'),
                    column_name=rule_config.get('column_name'),
                    parameters=rule_config.get('parameters', {}),
                    severity=ValidationSeverity(rule_config.get('severity', 'warning')),
                    description=rule_config.get('description', ''),
                    enabled=rule_config.get('enabled', True)
                )
                rules.append(rule)
        
        # Generate default rules from column configuration
        default_rules = self._generate_default_rules()
        rules.extend(default_rules)
        
        return rules
    
    def _generate_default_rules(self) -> List[ValidationRule]:
        """
        Generate default validation rules from column configuration.
        
        Returns:
            List of default validation rules
        """
        rules = []
        
        for column_config in self.table_config.columns:
            column_name = column_config['name']
            
            # Skip technical columns
            if column_name.startswith('_'):
                continue
            
            # Not null rule
            if not column_config.get('nullable', True):
                rule = ValidationRule(
                    rule_id=f"not_null_{column_name}",
                    rule_name=f"Not Null - {column_name}",
                    rule_type="not_null",
                    column_name=column_name,
                    parameters={},
                    severity=ValidationSeverity.ERROR,
                    description=f"Column {column_name} should not contain null values"
                )
                rules.append(rule)
            
            # Data type validation
            data_type = column_config.get('data_type', 'string')
            if data_type in ['int', 'integer', 'double', 'float', 'decimal']:
                rule = ValidationRule(
                    rule_id=f"numeric_{column_name}",
                    rule_name=f"Numeric - {column_name}",
                    rule_type="numeric",
                    column_name=column_name,
                    parameters={'data_type': data_type},
                    severity=ValidationSeverity.ERROR,
                    description=f"Column {column_name} should contain valid numeric values"
                )
                rules.append(rule)
            
            # Custom validation rules from column config
            if 'validation_rules' in column_config:
                for rule_config in column_config['validation_rules']:
                    rule = ValidationRule(
                        rule_id=f"{rule_config.get('type', 'custom')}_{column_name}",
                        rule_name=f"{rule_config.get('type', 'Custom').title()} - {column_name}",
                        rule_type=rule_config.get('type', 'custom'),
                        column_name=column_name,
                        parameters=rule_config.get('parameters', {}),
                        severity=ValidationSeverity(rule_config.get('severity', 'warning')),
                        description=rule_config.get('description', f"Custom validation for {column_name}")
                    )
                    rules.append(rule)
        
        return rules
    
    def _execute_validation_rule(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """
        Execute a single validation rule.
        
        Args:
            df: DataFrame to validate
            rule: Validation rule to execute
            
        Returns:
            Validation result
        """
        start_time = datetime.now()
        
        try:
            # Execute rule based on type
            if rule.rule_type == "not_null":
                result = self._validate_not_null(df, rule)
            elif rule.rule_type == "numeric":
                result = self._validate_numeric(df, rule)
            elif rule.rule_type == "regex":
                result = self._validate_regex(df, rule)
            elif rule.rule_type == "range":
                result = self._validate_range(df, rule)
            elif rule.rule_type == "allowed_values":
                result = self._validate_allowed_values(df, rule)
            elif rule.rule_type == "unique":
                result = self._validate_unique(df, rule)
            elif rule.rule_type == "length":
                result = self._validate_length(df, rule)
            elif rule.rule_type == "custom_sql":
                result = self._validate_custom_sql(df, rule)
            else:
                result = self._validate_custom(df, rule)
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            result.execution_time_ms = execution_time
            
            self.logger.debug("Validation rule executed",
                            rule_id=rule.rule_id,
                            passed=result.passed,
                            failed_count=result.failed_count,
                            execution_time_ms=execution_time)
            
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.error("Validation rule execution failed",
                            rule_id=rule.rule_id,
                            error=str(e))
            
            return ValidationResult(
                rule_id=rule.rule_id,
                rule_name=rule.rule_name,
                column_name=rule.column_name,
                severity=rule.severity,
                passed=False,
                failed_count=-1,
                total_count=df.count(),
                failure_rate=1.0,
                message=f"Rule execution failed: {str(e)}",
                details={'error': str(e)},
                execution_time_ms=execution_time
            )
    
    def _validate_not_null(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate not null constraint."""
        column_name = rule.column_name
        total_count = df.count()
        
        null_count = df.filter(col(column_name).isNull()).count()
        failed_count = null_count
        passed = failed_count == 0
        failure_rate = failed_count / total_count if total_count > 0 else 0
        
        message = f"Found {failed_count} null values" if not passed else "No null values found"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'null_count': null_count},
            execution_time_ms=0
        )
    
    def _validate_numeric(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate numeric data type."""
        column_name = rule.column_name
        total_count = df.count()
        
        # Try to cast to numeric and count failures
        try:
            from pyspark.sql.functions import col
            from pyspark.sql.types import DoubleType
            
            # Count non-null values that can't be cast to numeric
            non_numeric_count = df.filter(
                col(column_name).isNotNull() & 
                col(column_name).cast(DoubleType()).isNull()
            ).count()
            
            failed_count = non_numeric_count
            passed = failed_count == 0
            failure_rate = failed_count / total_count if total_count > 0 else 0
            
            message = f"Found {failed_count} non-numeric values" if not passed else "All values are numeric"
            
        except Exception as e:
            failed_count = total_count
            passed = False
            failure_rate = 1.0
            message = f"Numeric validation failed: {str(e)}"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'non_numeric_count': failed_count},
            execution_time_ms=0
        )
    
    def _validate_regex(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate regex pattern."""
        column_name = rule.column_name
        pattern = rule.parameters.get('pattern', '.*')
        total_count = df.count()
        
        # Count values that don't match the pattern
        non_matching_count = df.filter(
            col(column_name).isNotNull() & 
            (regexp_extract(col(column_name), pattern, 0) == "")
        ).count()
        
        failed_count = non_matching_count
        passed = failed_count == 0
        failure_rate = failed_count / total_count if total_count > 0 else 0
        
        message = f"Found {failed_count} values not matching pattern" if not passed else "All values match pattern"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'pattern': pattern, 'non_matching_count': failed_count},
            execution_time_ms=0
        )
    
    def _validate_range(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate value range."""
        column_name = rule.column_name
        min_value = rule.parameters.get('min_value')
        max_value = rule.parameters.get('max_value')
        total_count = df.count()
        
        # Build range condition
        conditions = []
        if min_value is not None:
            conditions.append(col(column_name) < min_value)
        if max_value is not None:
            conditions.append(col(column_name) > max_value)
        
        if conditions:
            from functools import reduce
            from pyspark.sql.functions import or_
            range_condition = reduce(or_, conditions)
            
            out_of_range_count = df.filter(
                col(column_name).isNotNull() & range_condition
            ).count()
        else:
            out_of_range_count = 0
        
        failed_count = out_of_range_count
        passed = failed_count == 0
        failure_rate = failed_count / total_count if total_count > 0 else 0
        
        range_desc = f"[{min_value}, {max_value}]"
        message = f"Found {failed_count} values outside range {range_desc}" if not passed else f"All values within range {range_desc}"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'min_value': min_value, 'max_value': max_value, 'out_of_range_count': failed_count},
            execution_time_ms=0
        )
    
    def _validate_allowed_values(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate allowed values."""
        column_name = rule.column_name
        allowed_values = rule.parameters.get('allowed_values', [])
        total_count = df.count()
        
        if allowed_values:
            invalid_count = df.filter(
                col(column_name).isNotNull() & 
                ~col(column_name).isin(allowed_values)
            ).count()
        else:
            invalid_count = 0
        
        failed_count = invalid_count
        passed = failed_count == 0
        failure_rate = failed_count / total_count if total_count > 0 else 0
        
        message = f"Found {failed_count} values not in allowed list" if not passed else "All values are allowed"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'allowed_values': allowed_values, 'invalid_count': failed_count},
            execution_time_ms=0
        )
    
    def _validate_unique(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate uniqueness."""
        column_name = rule.column_name
        total_count = df.count()
        
        distinct_count = df.select(column_name).distinct().count()
        duplicate_count = total_count - distinct_count
        
        failed_count = duplicate_count
        passed = failed_count == 0
        failure_rate = failed_count / total_count if total_count > 0 else 0
        
        message = f"Found {failed_count} duplicate values" if not passed else "All values are unique"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'distinct_count': distinct_count, 'duplicate_count': failed_count},
            execution_time_ms=0
        )
    
    def _validate_length(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate string length."""
        column_name = rule.column_name
        min_length = rule.parameters.get('min_length')
        max_length = rule.parameters.get('max_length')
        total_count = df.count()
        
        # Build length condition
        conditions = []
        if min_length is not None:
            conditions.append(length(col(column_name)) < min_length)
        if max_length is not None:
            conditions.append(length(col(column_name)) > max_length)
        
        if conditions:
            from functools import reduce
            from pyspark.sql.functions import or_
            length_condition = reduce(or_, conditions)
            
            invalid_length_count = df.filter(
                col(column_name).isNotNull() & length_condition
            ).count()
        else:
            invalid_length_count = 0
        
        failed_count = invalid_length_count
        passed = failed_count == 0
        failure_rate = failed_count / total_count if total_count > 0 else 0
        
        length_desc = f"[{min_length}, {max_length}]"
        message = f"Found {failed_count} values with invalid length" if not passed else f"All values have valid length {length_desc}"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'min_length': min_length, 'max_length': max_length, 'invalid_length_count': failed_count},
            execution_time_ms=0
        )
    
    def _validate_custom_sql(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate using custom SQL."""
        sql_query = rule.parameters.get('sql_query', '')
        total_count = df.count()
        
        try:
            # Create temporary view
            temp_view = f"temp_validation_{rule.rule_id}"
            df.createOrReplaceTempView(temp_view)
            
            # Replace table placeholder with temp view
            sql_query = sql_query.replace('{table}', temp_view)
            
            # Execute query - should return count of failed records
            result_df = self.spark.sql(sql_query)
            failed_count = result_df.collect()[0][0]
            
            passed = failed_count == 0
            failure_rate = failed_count / total_count if total_count > 0 else 0
            
            message = f"Custom validation found {failed_count} issues" if not passed else "Custom validation passed"
            
        except Exception as e:
            failed_count = total_count
            passed = False
            failure_rate = 1.0
            message = f"Custom SQL validation failed: {str(e)}"
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=rule.column_name,
            severity=rule.severity,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            message=message,
            details={'sql_query': sql_query},
            execution_time_ms=0
        )
    
    def _validate_custom(self, df: DataFrame, rule: ValidationRule) -> ValidationResult:
        """Validate using custom logic."""
        # Placeholder for custom validation logic
        total_count = df.count()
        
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            column_name=rule.column_name,
            severity=rule.severity,
            passed=True,
            failed_count=0,
            total_count=total_count,
            failure_rate=0.0,
            message="Custom validation not implemented",
            details={'rule_type': rule.rule_type},
            execution_time_ms=0
        )
    
    def _analyze_validation_results(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Analyze validation results and create summary.
        
        Args:
            results: List of validation results
            
        Returns:
            Validation summary
        """
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = total_rules - passed_rules
        
        # Group by severity
        severity_counts = {}
        for severity in ValidationSeverity:
            severity_counts[severity.value] = {
                'total': sum(1 for r in results if r.severity == severity),
                'passed': sum(1 for r in results if r.severity == severity and r.passed),
                'failed': sum(1 for r in results if r.severity == severity and not r.passed)
            }
        
        # Calculate overall quality score
        quality_score = (passed_rules / total_rules * 100) if total_rules > 0 else 100
        
        return {
            'validation_timestamp': datetime.now().isoformat(),
            'table_id': self.table_config.table_id,
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'failed_rules': failed_rules,
            'quality_score': quality_score,
            'severity_breakdown': severity_counts,
            'results': results
        }
    
    def _generate_validation_report(self, results: List[ValidationResult], 
                                  summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate detailed validation report.
        
        Args:
            results: List of validation results
            summary: Validation summary
            
        Returns:
            Detailed validation report
        """
        # Group results by status and severity
        failed_results = [r for r in results if not r.passed]
        critical_failures = [r for r in failed_results if r.severity == ValidationSeverity.CRITICAL]
        error_failures = [r for r in failed_results if r.severity == ValidationSeverity.ERROR]
        warning_failures = [r for r in failed_results if r.severity == ValidationSeverity.WARNING]
        
        # Create report sections
        report = {
            'summary': {
                'quality_score': summary['quality_score'],
                'total_rules': summary['total_rules'],
                'passed_rules': summary['passed_rules'],
                'failed_rules': summary['failed_rules']
            },
            'critical_issues': [self._format_result_for_report(r) for r in critical_failures],
            'errors': [self._format_result_for_report(r) for r in error_failures],
            'warnings': [self._format_result_for_report(r) for r in warning_failures],
            'execution_stats': {
                'total_execution_time_ms': sum(r.execution_time_ms for r in results),
                'average_execution_time_ms': sum(r.execution_time_ms for r in results) / len(results) if results else 0,
                'slowest_rule': max(results, key=lambda r: r.execution_time_ms).rule_name if results else None
            }
        }
        
        return report
    
    def _format_result_for_report(self, result: ValidationResult) -> Dict[str, Any]:
        """Format validation result for report."""
        return {
            'rule_name': result.rule_name,
            'column_name': result.column_name,
            'severity': result.severity.value,
            'message': result.message,
            'failed_count': result.failed_count,
            'total_count': result.total_count,
            'failure_rate': f"{result.failure_rate:.2%}",
            'details': result.details
        } 