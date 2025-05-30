"""
SCD (Slowly Changing Dimension) Type 2 configuration models.
"""

from typing import Dict, List, Optional, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class SCDStrategy(str, Enum):
    """SCD implementation strategies."""
    MERGE_INTO = "merge_into"
    INSERT_OVERWRITE = "insert_overwrite"
    DELTA_MERGE = "delta_merge"


class HashAlgorithm(str, Enum):
    """Hash algorithms for change detection."""
    MD5 = "md5"
    SHA256 = "sha256"
    SHA1 = "sha1"


class SCDConfig(BaseModel):
    """Configuration for SCD Type 2 implementation."""
    
    # Basic SCD configuration
    business_keys: List[str] = Field(..., description="Business key columns for SCD")
    tracked_columns: List[str] = Field(..., description="Columns to track for changes")
    
    # Technical columns
    valid_from_column: str = Field("valid_from", description="Valid from timestamp column")
    valid_to_column: str = Field("valid_to", description="Valid to timestamp column")
    is_current_column: str = Field("is_current", description="Current record flag column")
    hash_column: str = Field("row_hash", description="Hash column for change detection")
    
    # Implementation strategy
    strategy: SCDStrategy = Field(SCDStrategy.MERGE_INTO, description="SCD implementation strategy")
    hash_algorithm: HashAlgorithm = Field(HashAlgorithm.SHA256, description="Hash algorithm")
    
    # Timestamp handling
    effective_timestamp_column: Optional[str] = Field(None, description="Source effective timestamp column")
    use_source_timestamp: bool = Field(False, description="Use source timestamp instead of processing time")
    default_valid_to: str = Field("9999-12-31 23:59:59", description="Default valid_to value for current records")
    
    # Change detection
    ignore_null_changes: bool = Field(True, description="Ignore changes from/to NULL values")
    case_sensitive_comparison: bool = Field(True, description="Case sensitive string comparison")
    trim_strings: bool = Field(True, description="Trim whitespace from strings before comparison")
    
    # Performance optimization
    enable_clustering: bool = Field(True, description="Enable table clustering on business keys")
    clustering_columns: Optional[List[str]] = Field(None, description="Custom clustering columns")
    partition_on_valid_from: bool = Field(True, description="Partition table on valid_from column")
    
    # Data quality
    validate_business_keys: bool = Field(True, description="Validate business keys are not null")
    allow_duplicate_business_keys: bool = Field(False, description="Allow duplicate business keys in source")
    handle_late_arriving_data: bool = Field(True, description="Handle late arriving data")
    
    @validator('business_keys')
    def validate_business_keys(cls, v):
        if not v:
            raise ValueError("At least one business key must be specified")
        return v
    
    @validator('tracked_columns')
    def validate_tracked_columns(cls, v):
        if not v:
            raise ValueError("At least one tracked column must be specified")
        return v
    
    @validator('clustering_columns')
    def validate_clustering_columns(cls, v, values):
        if v and 'business_keys' in values:
            # Clustering columns should include business keys
            business_keys = values['business_keys']
            for key in business_keys:
                if key not in v:
                    v.insert(0, key)
        return v
    
    def get_technical_columns(self) -> Dict[str, str]:
        """Get technical columns for SCD implementation."""
        return {
            self.valid_from_column: "timestamp",
            self.valid_to_column: "timestamp", 
            self.is_current_column: "boolean",
            self.hash_column: "string"
        }
    
    def get_clustering_columns(self) -> List[str]:
        """Get columns for table clustering."""
        if self.clustering_columns:
            return self.clustering_columns
        
        # Default clustering: business keys + valid_from
        columns = self.business_keys.copy()
        if self.valid_from_column not in columns:
            columns.append(self.valid_from_column)
        return columns


class SCDOperation(BaseModel):
    """Configuration for SCD operation execution."""
    
    # Source and target
    source_table: str = Field(..., description="Source table identifier")
    target_table: str = Field(..., description="Target table identifier")
    
    # SCD configuration
    scd_config: SCDConfig = Field(..., description="SCD configuration")
    
    # Operation settings
    batch_id: str = Field(..., description="Batch identifier")
    processing_timestamp: datetime = Field(default_factory=datetime.now, description="Processing timestamp")
    
    # Performance settings
    optimize_after_merge: bool = Field(True, description="Optimize table after merge")
    vacuum_after_merge: bool = Field(False, description="Vacuum table after merge")
    analyze_after_merge: bool = Field(True, description="Analyze table after merge")
    
    # Validation settings
    validate_before_merge: bool = Field(True, description="Validate data before merge")
    validate_after_merge: bool = Field(True, description="Validate data after merge")
    
    # Monitoring
    collect_metrics: bool = Field(True, description="Collect operation metrics")
    log_sample_changes: bool = Field(True, description="Log sample of detected changes")
    
    def get_merge_condition(self) -> str:
        """Generate merge condition for SCD operation."""
        conditions = []
        for key in self.scd_config.business_keys:
            conditions.append(f"source.{key} = target.{key}")
        
        # Add current record condition
        conditions.append(f"target.{self.scd_config.is_current_column} = true")
        
        return " AND ".join(conditions)
    
    def get_change_detection_condition(self) -> str:
        """Generate condition for change detection."""
        if self.scd_config.hash_column:
            return f"source.{self.scd_config.hash_column} != target.{self.scd_config.hash_column}"
        
        # Fallback to column-by-column comparison
        conditions = []
        for col in self.scd_config.tracked_columns:
            if self.scd_config.ignore_null_changes:
                condition = f"(source.{col} != target.{col} AND source.{col} IS NOT NULL AND target.{col} IS NOT NULL)"
            else:
                condition = f"source.{col} != target.{col}"
            conditions.append(condition)
        
        return " OR ".join(conditions)


class SCDMetrics(BaseModel):
    """Metrics collected during SCD operation."""
    
    # Operation metadata
    operation_id: str = Field(..., description="Operation identifier")
    batch_id: str = Field(..., description="Batch identifier")
    table_name: str = Field(..., description="Target table name")
    start_time: datetime = Field(..., description="Operation start time")
    end_time: Optional[datetime] = Field(None, description="Operation end time")
    
    # Data metrics
    source_row_count: int = Field(0, description="Number of rows in source")
    target_row_count_before: int = Field(0, description="Target rows before operation")
    target_row_count_after: int = Field(0, description="Target rows after operation")
    
    # Change metrics
    new_records: int = Field(0, description="Number of new records inserted")
    updated_records: int = Field(0, description="Number of records updated (closed)")
    unchanged_records: int = Field(0, description="Number of unchanged records")
    
    # Performance metrics
    processing_time_seconds: Optional[float] = Field(None, description="Processing time in seconds")
    rows_per_second: Optional[float] = Field(None, description="Processing rate")
    
    # Quality metrics
    duplicate_business_keys: int = Field(0, description="Number of duplicate business keys found")
    null_business_keys: int = Field(0, description="Number of null business keys found")
    validation_errors: int = Field(0, description="Number of validation errors")
    
    def calculate_derived_metrics(self):
        """Calculate derived metrics."""
        if self.end_time and self.start_time:
            self.processing_time_seconds = (self.end_time - self.start_time).total_seconds()
            
            if self.processing_time_seconds > 0:
                self.rows_per_second = self.source_row_count / self.processing_time_seconds 