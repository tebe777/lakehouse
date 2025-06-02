"""
Table configuration models using Pydantic for validation.
"""

from typing import Dict, List, Optional, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class DataType(str, Enum):
    """Supported data types for Iceberg tables."""
    STRING = "string"
    LONG = "long"
    INTEGER = "integer"
    DOUBLE = "double"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BINARY = "binary"
    DECIMAL = "decimal"


class PartitionType(str, Enum):
    """Supported partition types."""
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    HOUR = "hour"
    IDENTITY = "identity"


class ValidationSeverity(str, Enum):
    """Validation rule severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ValidationRule(BaseModel):
    """Configuration for data validation rules."""
    rule_type: str = Field(..., description="Type of validation rule")
    column: Optional[str] = Field(None, description="Column to validate")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Rule parameters")
    severity: ValidationSeverity = Field(ValidationSeverity.ERROR, description="Severity level")
    description: Optional[str] = Field(None, description="Human readable description")
    
    @validator('rule_type')
    def validate_rule_type(cls, v):
        allowed_types = [
            'not_null', 'unique', 'min_value', 'max_value', 
            'min_length', 'max_length', 'regex_match', 'in_set',
            'row_count', 'column_count', 'freshness', 'referential_integrity'
        ]
        if v not in allowed_types:
            raise ValueError(f"rule_type must be one of {allowed_types}")
        return v


class ColumnConfig(BaseModel):
    """Configuration for table columns."""
    name: str = Field(..., description="Column name")
    data_type: DataType = Field(..., description="Column data type")
    nullable: bool = Field(True, description="Whether column can be null")
    description: Optional[str] = Field(None, description="Column description")
    business_name: Optional[str] = Field(None, description="Business friendly name")
    source_column: Optional[str] = Field(None, description="Source column name if different")
    default_value: Optional[Any] = Field(None, description="Default value")
    precision: Optional[int] = Field(None, description="Precision for decimal types")
    scale: Optional[int] = Field(None, description="Scale for decimal types")


class PartitionSpec(BaseModel):
    """Partition specification for tables."""
    columns: List[str] = Field(..., description="Columns to partition by")
    partition_type: PartitionType = Field(..., description="Type of partitioning")
    
    @validator('columns')
    def validate_columns(cls, v):
        if not v:
            raise ValueError("At least one partition column must be specified")
        return v


class IcebergProperties(BaseModel):
    """Iceberg table properties configuration."""
    
    # Format and compatibility
    format_version: str = Field("2", description="Iceberg format version")
    
    # Write performance
    write_delete_mode: str = Field("merge-on-read", description="Delete operation mode")
    write_update_mode: str = Field("merge-on-read", description="Update operation mode") 
    write_merge_mode: str = Field("merge-on-read", description="Merge operation mode")
    write_target_file_size_bytes: int = Field(134217728, description="Target file size (128MB default)")
    
    # Compaction and optimization
    commit_retry_num_retries: int = Field(4, description="Number of commit retries")
    commit_retry_min_wait_ms: int = Field(100, description="Min wait between retries")
    commit_retry_max_wait_ms: int = Field(60000, description="Max wait between retries")
    
    # Data lifecycle
    history_expire_min_snapshots_to_keep: int = Field(1, description="Min snapshots to keep")
    history_expire_max_snapshot_age_ms: int = Field(432000000, description="Max snapshot age (5 days)")
    
    # Metadata optimization  
    metadata_delete_after_commit_enabled: bool = Field(True, description="Delete metadata after commit")
    metadata_previous_versions_max: int = Field(100, description="Max previous metadata versions")
    
    # Performance optimization
    split_size: int = Field(134217728, description="Split size for reading (128MB)")
    split_lookback: int = Field(10, description="Lookback for split planning")
    split_open_file_cost: int = Field(4194304, description="Cost of opening file (4MB)")
    
    # Security and compliance  
    encryption_key_id: Optional[str] = Field(None, description="Encryption key ID")
    
    # Custom properties
    custom_properties: Dict[str, str] = Field(default_factory=dict, description="Custom table properties")
    
    def to_spark_properties(self) -> Dict[str, str]:
        """Convert to Spark table properties format."""
        props = {
            "format-version": self.format_version,
            "write.delete.mode": self.write_delete_mode,
            "write.update.mode": self.write_update_mode,
            "write.merge.mode": self.write_merge_mode,
            "write.target-file-size-bytes": str(self.write_target_file_size_bytes),
            "commit.retry.num-retries": str(self.commit_retry_num_retries),
            "commit.retry.min-wait-ms": str(self.commit_retry_min_wait_ms),
            "commit.retry.max-wait-ms": str(self.commit_retry_max_wait_ms),
            "history.expire.min-snapshots-to-keep": str(self.history_expire_min_snapshots_to_keep),
            "history.expire.max-snapshot-age-ms": str(self.history_expire_max_snapshot_age_ms),
            "metadata.delete-after-commit.enabled": str(self.metadata_delete_after_commit_enabled).lower(),
            "metadata.previous-versions.max": str(self.metadata_previous_versions_max),
            "read.split.target-size": str(self.split_size),
            "read.split.planning-lookback": str(self.split_lookback),
            "read.split.open-file-cost": str(self.split_open_file_cost)
        }
        
        if self.encryption_key_id:
            props["encryption.key-id"] = self.encryption_key_id
            
        # Add custom properties
        props.update(self.custom_properties)
        
        return props


class TableConfig(BaseModel):
    """Complete table configuration."""
    identifier: str = Field(..., description="Full table identifier (layer.source.table)")
    layer: Literal["raw", "normalised", "semantic"] = Field(..., description="Data layer")
    source_type: str = Field(..., description="Source system type")
    table_name: str = Field(..., description="Table name")
    description: Optional[str] = Field(None, description="Table description")
    
    # Schema definition
    columns: List[ColumnConfig] = Field(..., description="Column definitions")
    business_keys: List[str] = Field(..., description="Business key columns")
    
    # Partitioning
    partition_spec: Optional[PartitionSpec] = Field(None, description="Partition specification")
    
    # SCD configuration
    enable_scd: bool = Field(False, description="Enable SCD Type 2")
    scd_columns: List[str] = Field(default_factory=list, description="Columns to track for SCD")
    
    # Validation rules
    validation_rules: List[ValidationRule] = Field(default_factory=list, description="Validation rules")
    
    # Iceberg properties
    iceberg_properties: IcebergProperties = Field(default_factory=IcebergProperties, description="Iceberg table properties")
    
    # Technical columns (auto-added)
    technical_columns: Dict[str, str] = Field(
        default_factory=lambda: {
            "ingested_at": "timestamp",
            "valid_from": "timestamp", 
            "valid_to": "timestamp",
            "is_current": "boolean",
            "source_file": "string",
            "batch_id": "string"
        },
        description="Technical columns added automatically"
    )
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field("1.0.0", description="Schema version")
    
    @validator('identifier')
    def validate_identifier(cls, v):
        parts = v.split('.')
        if len(parts) != 3:
            raise ValueError("Identifier must be in format 'layer.source.table'")
        return v
    
    @validator('business_keys')
    def validate_business_keys(cls, v, values):
        if 'columns' in values:
            column_names = [col.name for col in values['columns']]
            for key in v:
                if key not in column_names:
                    raise ValueError(f"Business key '{key}' not found in columns")
        return v
    
    @validator('scd_columns')
    def validate_scd_columns(cls, v, values):
        if v and 'columns' in values:
            column_names = [col.name for col in values['columns']]
            for col in v:
                if col not in column_names:
                    raise ValueError(f"SCD column '{col}' not found in columns")
        return v
    
    def get_full_schema(self) -> List[ColumnConfig]:
        """Get complete schema including technical columns."""
        schema = self.columns.copy()
        
        # Add technical columns
        for tech_col, data_type in self.technical_columns.items():
            schema.append(ColumnConfig(
                name=tech_col,
                data_type=DataType(data_type),
                nullable=False if tech_col in ['ingested_at', 'is_current'] else True,
                description=f"Technical column: {tech_col}"
            ))
        
        return schema
    
    def get_iceberg_schema(self) -> Dict[str, str]:
        """Get schema in format suitable for Iceberg table creation."""
        schema = {}
        for col in self.get_full_schema():
            schema[col.name] = col.data_type.value
        return schema