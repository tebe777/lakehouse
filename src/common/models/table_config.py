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