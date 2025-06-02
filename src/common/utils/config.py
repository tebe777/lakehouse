# src/common/utils/config.py

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

class TableConfig:
    """
    Holds configuration for a table.

    Attributes:
        identifier: full table name
        schema: mapping of column name to type
        key_columns: list of columns serving as business keys
        validation: dict of validation rules
        partition_spec: optional configuration for partitioning
        group: optional grouping for DAG
    """
    def __init__(
        self,
        identifier: str,
        schema: Dict[str, str],
        key_columns: List[str],
        validation: Dict[str, Any],
        partition_spec: Optional[Dict[str, Any]] = None,
        group: Optional[str] = None
    ):
        self.identifier = identifier
        self.schema = schema
        self.key_columns = key_columns
        self.validation = validation
        self.partition_spec = partition_spec
        self.group = group or 'default'

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableConfig':
        required = ['identifier', 'schema', 'key_columns', 'validation']
        missing = [r for r in required if r not in data]
        if missing:
            raise ValueError(f"Missing required table config fields: {missing}")
        return cls(
            identifier=data['identifier'],
            schema=data['schema'],
            key_columns=data['key_columns'],
            validation=data['validation'],
            partition_spec=data.get('partition_spec'),
            group=data.get('group')
        )


def load_all_table_configs(dir_path: str) -> List[TableConfig]:
    """
    Load all JSON configs from a directory and return list of TableConfig.
    """
    path = Path(dir_path)
    if not path.is_dir():
        raise FileNotFoundError(f"Config directory not found: {dir_path}")

    configs: List[TableConfig] = []
    for file in path.glob('*.json'):
        with file.open('r') as f:
            data = json.load(f)
        config = TableConfig.from_dict(data)
        configs.append(config)
    return configs
