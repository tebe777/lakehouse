# src/airflow/utils.py

import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog
from src.common.utils.config import load_all_table_configs
from src.common.utils.validator import DataValidator
from src.common.utils.filename_parser import FileNameParser

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Initializing SparkSession('{app_name}')")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info("SparkSession initialized")
    return spark


def get_iceberg_catalog(name: str = 'default'):
    logger.info(f"Loading Iceberg catalog '{name}'")
    catalog = load_catalog(name)
    logger.info("Iceberg catalog loaded")
    return catalog


def get_table_configs(dir_path: str):
    logger.info(f"Loading table configs from {dir_path}")
    cfg_path = Path(dir_path)
    if not cfg_path.exists() or not cfg_path.is_dir():
        msg = f"Config directory not found: {dir_path}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    configs = load_all_table_configs(dir_path)
    logger.info(f"Loaded {len(configs)} table configs")
    return configs


def create_data_validator(validation_rules: dict) -> DataValidator:
    """
    Create a DataValidator instance with the specified rules.
    
    Args:
        validation_rules: Dictionary containing validation rules
        
    Returns:
        DataValidator instance
    """
    logger.info("Creating DataValidator with rules")
    return DataValidator(validation_rules)


def parse_filename(filename: str) -> dict:
    """
    Parse filename using the FileNameParser.
    
    Args:
        filename: Filename to parse
        
    Returns:
        Dictionary with parsed filename components
        
    Raises:
        ValueError: If filename doesn't match expected pattern
    """
    logger.info(f"Parsing filename: {filename}")
    try:
        result = FileNameParser.parse(filename)
        logger.info(f"Successfully parsed filename: {result}")
        return result
    except ValueError as e:
        logger.error(f"Failed to parse filename '{filename}': {e}")
        raise
