# src/common/utils/validator.py

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Validates a Spark DataFrame against given rules:
      - null_check: list of columns that must not contain nulls
      - date_range: dict mapping column -> {from, to} (both ISO strings)

    Example config:
      {
        'null_check': ['col1', 'col2'],
        'date_range': { 'date_col': {'from': '2020-01-01', 'to': '2030-12-31'} }
      }
    """
    def __init__(self, rules: Dict[str, Any]):
        self.null_check = rules.get('null_check', [])
        self.date_range = rules.get('date_range', {})

    def validate(self, df: DataFrame) -> None:
        """
        Perform validations on df. Raises ValueError if any check fails.
        """
        # Null checks
        for col_name in self.null_check:
            null_count = df.filter(col(col_name).isNull()).count()
            logger.info(f"Validation null_check on '{col_name}': found {null_count} null(s)")
            if null_count > 0:
                raise ValueError(f"Column '{col_name}' contains {null_count} null(s)")

        # Date range checks
        for col_name, bounds in self.date_range.items():
            try:
                lower = bounds.get('from')
                upper = bounds.get('to')
                cond = (col(col_name) < lower) | (col(col_name) > upper)
                out_of_range_count = df.filter(cond).count()
                logger.info(f"Validation date_range on '{col_name}': found {out_of_range_count} out-of-range row(s)")
                if out_of_range_count > 0:
                    raise ValueError(
                        f"Column '{col_name}' has {out_of_range_count} rows outside [{lower}, {upper}]")
            except Exception as e:
                raise ValueError(f"Error validating date_range for column '{col_name}': {e}")

        logger.info("Data validation passed successfully.")
