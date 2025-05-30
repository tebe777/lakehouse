# PowerShell script to create Lakehouse ETL project structure
# Run this script in PowerShell as Administrator or with appropriate permissions

$PROJECT_NAME = "lakehouse"

# Create main project directory
New-Item -ItemType Directory -Path $PROJECT_NAME -Force
Set-Location $PROJECT_NAME

# Create main source directories
$directories = @(
    "src",
    "tests\unit\test_ingestion",
    "tests\unit\test_transformation", 
    "tests\unit\test_semantic",
    "tests\unit\test_common",
    "tests\unit\test_orchestration",
    "tests\unit\test_catalog",
    "tests\unit\test_quality",
    "tests\integration\test_e2e_pipeline",
    "tests\integration\test_iceberg_integration",
    "configs\environments",
    "configs\tables",
    "configs\pipelines",
    "configs\validation",
    "configs\transformations",
    "sql\raw_to_silver",
    "sql\silver_to_gold", 
    "sql\views",
    "logs",
    "scripts",
    "docker",
    "src\airflow\dags",
    "src\airflow\plugins",
    "src\airflow\operators",
    "src\ingestion\raw_loader",
    "src\ingestion\zip_extractor",
    "src\transformation\silver_transformer",
    "src\transformation\validator",
    "src\semantic\views",
    "src\semantic\olap",
    "src\orchestration\workflows",
    "src\orchestration\dependencies",
    "src\catalog\metadata",
    "src\catalog\schema_registry",
    "src\quality\validators",
    "src\quality\profilers",
    "src\quality\alerting",
    "src\common\models",
    "src\common\utils",
    "src\common\connections",
    "src\common\security",
    "src\common\monitoring"
)

foreach ($dir in $directories) {
    New-Item -ItemType Directory -Path $dir -Force
    Write-Host "Created directory: $dir" -ForegroundColor Green
}

# Create __init__.py files for Python packages
$python_packages = @(
    "src",
    "src\airflow",
    "src\airflow\dags",
    "src\airflow\plugins", 
    "src\airflow\operators",
    "src\ingestion",
    "src\ingestion\raw_loader",
    "src\ingestion\zip_extractor",
    "src\transformation",
    "src\transformation\silver_transformer",
    "src\transformation\validator",
    "src\semantic",
    "src\semantic\views",
    "src\semantic\olap",
    "src\orchestration",
    "src\orchestration\workflows",
    "src\orchestration\dependencies",
    "src\catalog",
    "src\catalog\metadata",
    "src\catalog\schema_registry",
    "src\quality",
    "src\quality\validators",
    "src\quality\profilers",
    "src\quality\alerting",
    "src\common",
    "src\common\models",
    "src\common\utils",
    "src\common\connections",
    "src\common\security",
    "src\common\monitoring",
    "tests",
    "tests\unit",
    "tests\integration"
)

foreach ($package in $python_packages) {
    New-Item -ItemType File -Path "$package\__init__.py" -Force
    Write-Host "Created __init__.py in: $package" -ForegroundColor Yellow
}

# Create main Python files with basic class structure

# Common Models
$content = @"
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame

class IDataProcessor(ABC):
    """"""Base interface for all data processors""""""
    
    @abstractmethod
    def process(self, input_data: Any, config: Dict[str, Any]) -> Any:
        pass
    
    @abstractmethod
    def validate_input(self, input_data: Any) -> bool:
        pass

class IValidator(ABC):
    """"""Interface for data validation""""""
    
    @abstractmethod
    def validate(self, df: DataFrame, rules: Dict[str, Any]) -> Dict[str, Any]:
        pass

class ITransformer(ABC):
    """"""Interface for data transformation""""""
    
    @abstractmethod
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        pass

class IMetadataManager(ABC):
    """"""Interface for metadata management""""""
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def update_table_schema(self, table_name: str, schema: Dict[str, Any]) -> bool:
        pass
"@
New-Item -ItemType File -Path "src\common\models\interfaces.py" -Value $content -Force

# TableConfig
$content = @"
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
import json
from pathlib import Path

class LayerType(Enum):
    RAW = "raw"
    NORMALISED = "normalised" 
    SEMANTIC = "semantic"

class SCDType(Enum):
    TYPE_1 = "scd1"
    TYPE_2 = "scd2"

@dataclass
class PartitionSpec:
    columns: List[str]
    partition_type: str = "monthly"  # daily, monthly, yearly

@dataclass
class SCDConfig:
    type: SCDType
    business_keys: List[str]
    compare_columns: Optional[List[str]] = None
    effective_date_column: str = "_valid_from"
    end_date_column: str = "_valid_to"
    current_flag_column: str = "_is_current"

@dataclass
class ValidationRule:
    rule_type: str  # "not_null", "min_max", "custom_sql"
    column: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: str = "error"  # "error", "warning"

@dataclass
class TransformationRule:
    source_columns: List[str]
    target_column: str
    transformation_type: str  # "mapping", "calculation", "lookup"
    parameters: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TableConfig:
    """"""Enhanced table configuration""""""
    identifier: str
    layer: LayerType
    source_type: str
    schema: Dict[str, str]
    business_keys: List[str]
    partition_spec: Optional[PartitionSpec] = None
    scd_config: Optional[SCDConfig] = None
    validation_rules: List[ValidationRule] = field(default_factory=list)
    transformation_rules: List[TransformationRule] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableConfig':
        # Validation logic
        required = ['identifier', 'layer', 'source_type', 'schema', 'business_keys']
        missing = [r for r in required if r not in data]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
            
        # Parse complex objects
        partition_spec = None
        if 'partition_spec' in data:
            partition_spec = PartitionSpec(**data['partition_spec'])
            
        scd_config = None
        if 'scd_config' in data:
            scd_data = data['scd_config'].copy()
            scd_data['type'] = SCDType(scd_data['type'])
            scd_config = SCDConfig(**scd_data)
            
        validation_rules = [
            ValidationRule(**rule) for rule in data.get('validation_rules', [])
        ]
        
        transformation_rules = [
            TransformationRule(**rule) for rule in data.get('transformation_rules', [])
        ]
        
        return cls(
            identifier=data['identifier'],
            layer=LayerType(data['layer']),
            source_type=data['source_type'],
            schema=data['schema'],
            business_keys=data['business_keys'],
            partition_spec=partition_spec,
            scd_config=scd_config,
            validation_rules=validation_rules,
            transformation_rules=transformation_rules,
            dependencies=data.get('dependencies', []),
            metadata=data.get('metadata', {})
        )

def load_table_configs(path: str) -> List[TableConfig]:
    """"""Load table configurations from a JSON file""""""
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with file.open('r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Table config file must contain a list of configurations.")
    configs: List[TableConfig] = []
    for entry in data:
        config = TableConfig.from_dict(entry)
        configs.append(config)
    return configs
"@
New-Item -ItemType File -Path "src\common\models\table_config.py" -Value $content -Force

# Pipeline Config
$content = @"
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import timedelta

@dataclass
class DependencyConfig:
    source_tables: List[str]
    target_table: str
    wait_timeout: timedelta = timedelta(hours=2)
    required_files: Optional[List[str]] = None
    optional_files: Optional[List[str]] = None

@dataclass
class PipelineConfig:
    name: str
    description: str
    schedule: str  # Cron expression
    source_type: str
    dependencies: List[DependencyConfig] = field(default_factory=list)
    retry_count: int = 3
    retry_delay: timedelta = timedelta(minutes=5)
    sla: Optional[timedelta] = None
    alerts: List[str] = field(default_factory=list)  # email addresses
    parameters: Dict[str, Any] = field(default_factory=dict)
"@
New-Item -ItemType File -Path "src\common\models\pipeline_config.py" -Value $content -Force

# Raw Loader
$content = @"
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from src.common.models.interfaces import IDataProcessor
from src.common.models.table_config import TableConfig

class RawDataLoader(IDataProcessor):
    """"""Loads raw data from ZIP files to Iceberg Raw layer""""""
    
    def __init__(self, spark: SparkSession, iceberg_catalog: str):
        self.spark = spark
        self.iceberg_catalog = iceberg_catalog
    
    def process(self, zip_path: str, table_config: TableConfig) -> Dict[str, Any]:
        """"""Process ZIP file and load to raw layer""""""
        # TODO: Implement ZIP extraction and CSV processing
        return {"status": "success", "message": "Raw data loaded"}
    
    def validate_input(self, input_data: Any) -> bool:
        """"""Validate input ZIP file""""""
        # TODO: Implement input validation
        return True
    
    def _read_csv_with_schema(self, csv_path: str, config: TableConfig) -> DataFrame:
        """"""Read CSV with schema enforcement""""""
        # TODO: Implement schema enforcement and evolution
        pass
    
    def _add_technical_columns(self, df: DataFrame) -> DataFrame:
        """"""Add technical columns like _ingested_at""""""
        # TODO: Add technical metadata columns
        pass
"@
New-Item -ItemType File -Path "src\ingestion\raw_loader\raw_loader.py" -Value $content -Force

# ZIP Extractor
$content = @"
import zipfile
from pathlib import Path
from typing import List

class ZipExtractor:
    """"""Extracts ZIP files and processes CSV contents""""""
    
    def extract(self, zip_path: str, extract_to: str = None) -> List[str]:
        """"""Extract ZIP file and return list of extracted CSV files""""""
        if extract_to is None:
            extract_to = Path(zip_path).parent / "extracted"
        
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                extracted_files = [
                    str(Path(extract_to) / file) 
                    for file in zip_ref.namelist() 
                    if file.endswith('.csv')
                ]
        except Exception as e:
            raise Exception(f"Failed to extract ZIP file {zip_path}: {str(e)}")
        
        return extracted_files
"@
New-Item -ItemType File -Path "src\ingestion\zip_extractor\zip_extractor.py" -Value $content -Force

# Silver Transformer
$content = @"
from pyspark.sql import SparkSession, DataFrame
from src.common.models.interfaces import ITransformer
from src.common.models.table_config import TableConfig, SCDType

class SilverTransformer(ITransformer):
    """"""Transforms raw data to normalized silver layer with SCD Type 2""""""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def transform(self, df: DataFrame, config: TableConfig) -> DataFrame:
        """"""Apply transformations and SCD processing""""""
        # Apply business transformations
        transformed_df = self._apply_transformations(df, config)
        
        # Apply SCD Type 2 if configured
        if config.scd_config and config.scd_config.type == SCDType.TYPE_2:
            final_df = self._process_scd2(transformed_df, config)
        else:
            final_df = transformed_df
            
        return final_df
    
    def _apply_transformations(self, df: DataFrame, config: TableConfig) -> DataFrame:
        """"""Apply transformation rules from configuration""""""
        # TODO: Implement transformation rules
        return df
    
    def _process_scd2(self, df: DataFrame, config: TableConfig) -> DataFrame:
        """"""Process SCD Type 2 logic""""""
        # TODO: Implement SCD Type 2 processing
        return df
"@
New-Item -ItemType File -Path "src\transformation\silver_transformer\silver_transformer.py" -Value $content -Force

# Data Validator
$content = @"
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
"@
New-Item -ItemType File -Path "src\quality\validators\data_validator.py" -Value $content -Force

# Workflow Engine
$content = @"
from typing import Dict, Any, List
from src.common.models.pipeline_config import PipelineConfig

class WorkflowEngine:
    """"""Manages complex dependencies and workflow execution""""""
    
    def __init__(self, config_manager):
        self.config_manager = config_manager
    
    def execute_pipeline(self, pipeline_name: str, execution_date: str) -> Dict[str, Any]:
        """"""Execute pipeline with dependency resolution""""""
        pipeline_config = self.config_manager.get_pipeline_config(pipeline_name)
        
        # TODO: Implement dependency resolution and execution
        
        return {
            'pipeline': pipeline_name,
            'execution_date': execution_date,
            'success': True
        }
    
    def _execute_step(self, step: Dict[str, Any], execution_date: str) -> Dict[str, Any]:
        """"""Execute individual pipeline step""""""
        # TODO: Implement step execution
        return {'success': True}
"@
New-Item -ItemType File -Path "src\orchestration\workflows\workflow_engine.py" -Value $content -Force

# Airflow DAG
$content = @"
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for lakehouse',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1
)

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# TODO: Add actual ETL tasks
start_task >> end_task
"@
New-Item -ItemType File -Path "src\airflow\dags\daily_etl_dag.py" -Value $content -Force

# Configuration files
$config_content = @"
[
  {
    "identifier": "raw.crm.customers",
    "layer": "raw",
    "source_type": "crm", 
    "schema": {
      "customer_id": "long",
      "first_name": "string",
      "last_name": "string",
      "email": "string",
      "created_date": "timestamp"
    },
    "business_keys": ["customer_id"],
    "partition_spec": {
      "columns": ["created_date"],
      "partition_type": "monthly"
    },
    "validation_rules": [
      {
        "rule_type": "not_null",
        "column": "customer_id",
        "severity": "error"
      }
    ]
  }
]
"@
New-Item -ItemType File -Path "configs\tables\table_definitions.json" -Value $config_content -Force

$env_config = @"
spark:
  app_name: "lakehouse_etl_dev"
  master: "local[*]"
  executor:
    instances: 2
    cores: 2
    memory: "4g"
  driver:
    cores: 1
    memory: "2g"
  
iceberg:
  catalog_name: "lakehouse_catalog"
  warehouse_path: "file:///tmp/warehouse"
  
minio:
  endpoint: "http://localhost:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  
security:
  log_level: "DEBUG"
  
monitoring:
  metrics_enabled: false
  alerts_enabled: false
"@
New-Item -ItemType File -Path "configs\environments\dev.yaml" -Value $env_config -Force

# Requirements.txt
$requirements = @"
pyspark==3.5.0
pyarrow==14.0.0
pyiceberg==0.5.1
pandas==2.1.4
polars==0.20.0
s3fs==2023.12.0
boto3==1.34.0
SQLAlchemy==2.0.23
apache-airflow==2.8.0
pyyaml==6.0.1
"@
New-Item -ItemType File -Path "requirements.txt" -Value $requirements -Force

# Docker files
$dockerfile = @"
FROM apache/spark:3.5.0-python3

# Install Python dependencies
COPY requirements.txt /opt/requirements.txt
RUN pip install -r /opt/requirements.txt

# Copy application code
COPY src/ /opt/lakehouse_etl/src/
COPY configs/ /opt/lakehouse_etl/configs/
COPY sql/ /opt/lakehouse_etl/sql/

# Set environment variables
ENV PYTHONPATH="/opt/lakehouse_etl:`$PYTHONPATH"
ENV SPARK_HOME="/opt/spark"

WORKDIR /opt/lakehouse_etl
"@
New-Item -ItemType File -Path "docker\Dockerfile.spark" -Value $dockerfile -Force

# Scripts
$deploy_script = @"
@echo off
echo Starting Lakehouse ETL deployment...

echo Building Docker images...
docker build -f docker/Dockerfile.spark -t lakehouse-etl:latest .

echo Deployment completed!
pause
"@
New-Item -ItemType File -Path "scripts\deploy.bat" -Value $deploy_script -Force

# README
$readme = @"
# Lakehouse ETL Project

Enterprise-grade ETL solution for data lakehouse architecture using Apache Iceberg, Spark, and Airflow.

## Project Structure

- `/src` - Main application source code
- `/tests` - Unit and integration tests  
- `/configs` - Configuration files
- `/sql` - SQL templates and transformations
- `/docker` - Docker configuration
- `/scripts` - Deployment and utility scripts

## Setup

1. Install Python 3.11+
2. Install requirements: `pip install -r requirements.txt`
3. Configure environment in `configs/environments/`
4. Run tests: `python -m pytest tests/`

## Architecture

- **Raw Layer**: Original data from source systems
- **Normalised Layer**: 3NF normalized data with SCD Type 2
- **Semantic Layer**: Analytics-ready views and OLAP cubes

## Technologies

- Apache Spark (PySpark)
- Apache Iceberg
- Apache Airflow
- MinIO (S3-compatible storage)
- Dremio (SQL engine)
"@
New-Item -ItemType File -Path "README.md" -Value $readme -Force

# Test files
$test_content = @"
import unittest
from src.common.models.table_config import TableConfig, LayerType

class TestTableConfig(unittest.TestCase):
    
    def test_table_config_creation(self):
        config_data = {
            "identifier": "test.table",
            "layer": "raw",
            "source_type": "test",
            "schema": {"id": "long", "name": "string"},
            "business_keys": ["id"]
        }
        
        config = TableConfig.from_dict(config_data)
        self.assertEqual(config.identifier, "test.table")
        self.assertEqual(config.layer, LayerType.RAW)

if __name__ == '__main__':
    unittest.main()
"@
New-Item -ItemType File -Path "tests\unit\test_common\test_table_config.py" -Value $test_content -Force

Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host "Project structure created successfully!" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "1. cd $PROJECT_NAME" -ForegroundColor White
Write-Host "2. python -m venv venv" -ForegroundColor White
Write-Host "3. venv\Scripts\activate" -ForegroundColor White
Write-Host "4. pip install -r requirements.txt" -ForegroundColor White
Write-Host "5. python -m pytest tests\" -ForegroundColor White
Write-Host "`nProject ready for development!" -ForegroundColor Green