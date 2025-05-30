"""
Pipeline configuration models for dependency management and orchestration.
"""

from typing import Dict, List, Optional, Any, Literal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field, validator
from enum import Enum


class DependencyType(str, Enum):
    """Types of dependencies between pipeline steps."""
    FILE_ARRIVAL = "file_arrival"
    TABLE_READY = "table_ready"
    BATCH_COMPLETE = "batch_complete"
    TIME_BASED = "time_based"
    EXTERNAL_TRIGGER = "external_trigger"


class PipelineStatus(str, Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"


class DependencyConfig(BaseModel):
    """Configuration for pipeline dependencies."""
    dependency_id: str = Field(..., description="Unique dependency identifier")
    dependency_type: DependencyType = Field(..., description="Type of dependency")
    source: str = Field(..., description="Source identifier (file pattern, table name, etc.)")
    timeout_minutes: int = Field(60, description="Timeout in minutes")
    retry_count: int = Field(3, description="Number of retries")
    retry_delay_minutes: int = Field(5, description="Delay between retries in minutes")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Additional parameters")
    
    @validator('timeout_minutes')
    def validate_timeout(cls, v):
        if v <= 0:
            raise ValueError("Timeout must be positive")
        return v


class FilePattern(BaseModel):
    """Configuration for file pattern matching."""
    pattern: str = Field(..., description="File pattern (glob or regex)")
    source_path: str = Field(..., description="Source directory path")
    expected_count: Optional[int] = Field(None, description="Expected number of files")
    min_count: int = Field(1, description="Minimum number of files required")
    max_age_hours: Optional[int] = Field(None, description="Maximum file age in hours")


class BatchConfig(BaseModel):
    """Configuration for batch processing."""
    batch_id_pattern: str = Field(..., description="Pattern for batch ID generation")
    file_patterns: List[FilePattern] = Field(..., description="File patterns in batch")
    completion_criteria: str = Field("all_files", description="Criteria for batch completion")
    max_wait_hours: int = Field(24, description="Maximum wait time for batch completion")


class PipelineStep(BaseModel):
    """Individual step in a pipeline."""
    step_id: str = Field(..., description="Unique step identifier")
    step_name: str = Field(..., description="Human readable step name")
    step_type: Literal["extract", "transform", "load", "validate", "notify"] = Field(..., description="Type of step")
    
    # Dependencies
    depends_on: List[str] = Field(default_factory=list, description="List of step IDs this step depends on")
    dependencies: List[DependencyConfig] = Field(default_factory=list, description="Detailed dependency configurations")
    
    # Execution configuration
    executor_class: str = Field(..., description="Python class to execute this step")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Step-specific parameters")
    
    # Resource requirements
    spark_config: Optional[Dict[str, str]] = Field(None, description="Spark configuration overrides")
    memory_gb: Optional[int] = Field(None, description="Memory requirement in GB")
    cpu_cores: Optional[int] = Field(None, description="CPU cores requirement")
    
    # Error handling
    retry_count: int = Field(3, description="Number of retries on failure")
    retry_delay_minutes: int = Field(5, description="Delay between retries")
    continue_on_failure: bool = Field(False, description="Continue pipeline if this step fails")
    
    # Monitoring
    sla_minutes: Optional[int] = Field(None, description="SLA for step completion")
    alert_on_failure: bool = Field(True, description="Send alert on failure")
    alert_on_sla_breach: bool = Field(True, description="Send alert on SLA breach")


class PipelineConfig(BaseModel):
    """Complete pipeline configuration."""
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    pipeline_name: str = Field(..., description="Human readable pipeline name")
    description: Optional[str] = Field(None, description="Pipeline description")
    
    # Pipeline metadata
    layer: Literal["raw", "normalised", "semantic"] = Field(..., description="Target data layer")
    source_type: str = Field(..., description="Source system type")
    
    # Steps configuration
    steps: List[PipelineStep] = Field(..., description="Pipeline steps")
    
    # Batch configuration
    batch_config: Optional[BatchConfig] = Field(None, description="Batch processing configuration")
    
    # Scheduling
    schedule_cron: Optional[str] = Field(None, description="Cron expression for scheduling")
    timezone: str = Field("UTC", description="Timezone for scheduling")
    
    # Global settings
    max_parallel_steps: int = Field(5, description="Maximum parallel steps")
    global_timeout_hours: int = Field(24, description="Global pipeline timeout")
    
    # Monitoring and alerting
    alert_channels: List[str] = Field(default_factory=list, description="Alert channels (email, slack, etc.)")
    monitoring_enabled: bool = Field(True, description="Enable monitoring")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field("1.0.0", description="Pipeline version")
    owner: Optional[str] = Field(None, description="Pipeline owner")
    tags: List[str] = Field(default_factory=list, description="Pipeline tags")
    
    @validator('steps')
    def validate_steps(cls, v):
        if not v:
            raise ValueError("Pipeline must have at least one step")
        
        step_ids = [step.step_id for step in v]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("Step IDs must be unique")
        
        # Validate dependencies
        for step in v:
            for dep_step_id in step.depends_on:
                if dep_step_id not in step_ids:
                    raise ValueError(f"Step {step.step_id} depends on non-existent step {dep_step_id}")
        
        return v
    
    @validator('schedule_cron')
    def validate_cron(cls, v):
        if v is not None:
            # Basic cron validation - could be enhanced with croniter
            parts = v.split()
            if len(parts) != 5:
                raise ValueError("Cron expression must have 5 parts")
        return v
    
    def get_step_by_id(self, step_id: str) -> Optional[PipelineStep]:
        """Get step by ID."""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None
    
    def get_dependency_graph(self) -> Dict[str, List[str]]:
        """Get dependency graph as adjacency list."""
        graph = {}
        for step in self.steps:
            graph[step.step_id] = step.depends_on.copy()
        return graph
    
    def get_execution_order(self) -> List[List[str]]:
        """Get steps grouped by execution level (topological sort)."""
        graph = self.get_dependency_graph()
        in_degree = {step_id: 0 for step_id in graph}
        
        # Calculate in-degrees
        for step_id in graph:
            for dep in graph[step_id]:
                in_degree[dep] += 1
        
        # Topological sort with levels
        levels = []
        remaining = set(graph.keys())
        
        while remaining:
            # Find nodes with no dependencies
            current_level = [step_id for step_id in remaining if in_degree[step_id] == 0]
            
            if not current_level:
                raise ValueError("Circular dependency detected in pipeline")
            
            levels.append(current_level)
            
            # Remove current level nodes and update in-degrees
            for step_id in current_level:
                remaining.remove(step_id)
                for dependent in graph:
                    if step_id in graph[dependent]:
                        in_degree[dependent] -= 1
        
        return levels