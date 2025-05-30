from typing import Dict, Any, List
from pyspark.sql import DataFrame
from src.common.models.interfaces import IValidator
from src.common.models.table_config import ValidationRule

class DataValidator(IValidator):
    """"""Comprehensive data validation framework""""""
    
    def __init__(self, spark):
        self.spark = spark
        self.rule_engines = {
            'not_null': self._validate_not_null,
            'min_max': self._validate_min_max,
            'custom_sql': self._validate_custom_sql
        }
    
    def validate(self, df: DataFrame, rules: List[ValidationRule]) -> Dict[str, Any]:
        """"""Validate DataFrame against rules""""""
        results = {
            'total_rules': len(rules),
            'passed_rules': 0,
            'failed_rules': 0,
            'warnings': 0,
            'errors': [],
            'warnings_list': [],
            'has_errors': False,
            'row_count': df.count()
        }
        
        for rule in rules:
            try:
                rule_result = self.rule_engines[rule.rule_type](df, rule)
                if rule_result['passed']:
                    results['passed_rules'] += 1
                else:
                    if rule.severity == 'error':
                        results['failed_rules'] += 1
                        results['errors'].append(rule_result)
                        results['has_errors'] = True
                    else:
                        results['warnings'] += 1
                        results['warnings_list'].append(rule_result)
            except Exception as e:
                results['errors'].append({
                    'rule_type': rule.rule_type,
                    'error': str(e)
                })
                results['has_errors'] = True
        
        return results
    
    def _validate_not_null(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """"""Validate not null constraint""""""
        null_count = df.filter(df[rule.column].isNull()).count()
        return {
            'rule_type': rule.rule_type,
            'column': rule.column,
            'passed': null_count == 0,
            'null_count': null_count,
            'message': f"Found {null_count} null values in {rule.column}"
        }
    
    def _validate_min_max(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """"""Validate min/max values""""""
        # TODO: Implement min/max validation
        return {'passed': True}
    
    def _validate_custom_sql(self, df: DataFrame, rule: ValidationRule) -> Dict[str, Any]:
        """"""Validate using custom SQL""""""
        # TODO: Implement custom SQL validation
        return {'passed': True}