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