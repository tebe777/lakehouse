"""
Configuration manager for loading and managing table and pipeline configurations.
Focused on configuration-as-code approach using files.
"""

import json
import yaml
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from abc import ABC, abstractmethod
import structlog

from ..common.models.table_config import TableConfig, ColumnConfig, ValidationRule
from ..common.models.pipeline_config import PipelineConfig
from ..common.connections.security_manager import SecurityManager

logger = structlog.get_logger(__name__)


class ConfigurationSource(ABC):
    """Abstract base class for configuration sources."""
    
    @abstractmethod
    def load_table_configs(self, environment: str = "default") -> List[TableConfig]:
        """Load table configurations."""
        pass
    
    @abstractmethod
    def load_pipeline_configs(self, environment: str = "default") -> List[PipelineConfig]:
        """Load pipeline configurations."""
        pass
    
    @abstractmethod
    def save_table_config(self, config: TableConfig, environment: str = "default") -> bool:
        """Save table configuration."""
        pass
    
    @abstractmethod
    def save_pipeline_config(self, config: PipelineConfig, environment: str = "default") -> bool:
        """Save pipeline configuration."""
        pass


class FileConfigurationSource(ConfigurationSource):
    """Configuration source using local files - primary approach for config-as-code."""
    
    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)
        self.tables_dir = self.config_dir / "tables"
        self.pipelines_dir = self.config_dir / "pipelines"
        self.environments_dir = self.config_dir / "environments"
        self.logger = logger.bind(source="FileConfigurationSource")
        
        # Ensure directories exist
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
        self.environments_dir.mkdir(parents=True, exist_ok=True)
    
    def load_table_configs(self, environment: str = "default") -> List[TableConfig]:
        """Load table configurations from files."""
        configs = []
        
        # Load base configurations
        configs.extend(self._load_configs_from_directory(self.tables_dir, "table"))
        
        # Load environment-specific overrides
        env_tables_dir = self.environments_dir / environment / "tables"
        if env_tables_dir.exists():
            env_configs = self._load_configs_from_directory(env_tables_dir, "table")
            configs = self._merge_environment_configs(configs, env_configs)
        
        self.logger.info("Loaded table configurations", 
                        count=len(configs), 
                        environment=environment)
        return configs
    
    def load_pipeline_configs(self, environment: str = "default") -> List[PipelineConfig]:
        """Load pipeline configurations from files."""
        configs = []
        
        # Load base configurations
        configs.extend(self._load_configs_from_directory(self.pipelines_dir, "pipeline"))
        
        # Load environment-specific overrides
        env_pipelines_dir = self.environments_dir / environment / "pipelines"
        if env_pipelines_dir.exists():
            env_configs = self._load_configs_from_directory(env_pipelines_dir, "pipeline")
            configs = self._merge_environment_configs(configs, env_configs)
        
        self.logger.info("Loaded pipeline configurations", 
                        count=len(configs), 
                        environment=environment)
        return configs
    
    def _load_configs_from_directory(self, directory: Path, config_type: str) -> List[Union[TableConfig, PipelineConfig]]:
        """Load configurations from a directory."""
        configs = []
        
        if not directory.exists():
            return configs
        
        # Load from JSON files
        for json_file in directory.glob("*.json"):
            try:
                if config_type == "table":
                    configs.extend(self._load_table_configs_from_file(json_file))
                else:
                    configs.extend(self._load_pipeline_configs_from_file(json_file))
            except Exception as e:
                self.logger.error("Failed to load config from file",
                                file=str(json_file), error=str(e))
        
        # Load from YAML files
        for yaml_file in directory.glob("*.yaml"):
            try:
                if config_type == "table":
                    configs.extend(self._load_table_configs_from_file(yaml_file))
                else:
                    configs.extend(self._load_pipeline_configs_from_file(yaml_file))
            except Exception as e:
                self.logger.error("Failed to load config from file",
                                file=str(yaml_file), error=str(e))
        
        return configs
    
    def _merge_environment_configs(self, base_configs: List, env_configs: List) -> List:
        """Merge environment-specific configurations with base configurations."""
        # Create lookup for base configs
        if not base_configs:
            return env_configs
        
        # Determine identifier field
        if hasattr(base_configs[0], 'identifier'):
            id_field = 'identifier'
        elif hasattr(base_configs[0], 'pipeline_id'):
            id_field = 'pipeline_id'
        else:
            return base_configs  # Can't merge without identifier
        
        base_lookup = {getattr(config, id_field): config for config in base_configs}
        
        # Apply environment overrides
        for env_config in env_configs:
            config_id = getattr(env_config, id_field)
            if config_id in base_lookup:
                # Replace base config with environment version
                base_lookup[config_id] = env_config
            else:
                # Add new environment-specific config
                base_lookup[config_id] = env_config
        
        return list(base_lookup.values())
    
    def _load_table_configs_from_file(self, file_path: Path) -> List[TableConfig]:
        """Load table configurations from a single file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.suffix.lower() == '.json':
                data = json.load(f)
            else:  # YAML
                data = yaml.safe_load(f)
        
        if isinstance(data, list):
            return [self._parse_table_config(item) for item in data]
        else:
            return [self._parse_table_config(data)]
    
    def _load_pipeline_configs_from_file(self, file_path: Path) -> List[PipelineConfig]:
        """Load pipeline configurations from a single file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.suffix.lower() == '.json':
                data = json.load(f)
            else:  # YAML
                data = yaml.safe_load(f)
        
        if isinstance(data, list):
            return [PipelineConfig(**item) for item in data]
        else:
            return [PipelineConfig(**data)]
    
    def _parse_table_config(self, data: Dict[str, Any]) -> TableConfig:
        """Parse table configuration from dictionary."""
        # Convert columns to ColumnConfig objects
        if 'columns' in data:
            columns = []
            for col_data in data['columns']:
                if isinstance(col_data, dict):
                    columns.append(ColumnConfig(**col_data))
                else:
                    # Handle legacy format where columns might be simple dict
                    columns.append(ColumnConfig(
                        name=col_data.get('name', ''),
                        data_type=col_data.get('data_type', 'string')
                    ))
            data['columns'] = columns
        
        # Convert validation rules
        if 'validation_rules' in data:
            rules = []
            for rule_data in data['validation_rules']:
                if isinstance(rule_data, dict):
                    rules.append(ValidationRule(**rule_data))
            data['validation_rules'] = rules
        
        return TableConfig(**data)
    
    def save_table_config(self, config: TableConfig, environment: str = "default") -> bool:
        """Save table configuration to file."""
        try:
            # Determine target directory
            if environment == "default":
                target_dir = self.tables_dir
            else:
                target_dir = self.environments_dir / environment / "tables"
                target_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = target_dir / f"{config.identifier.replace('.', '_')}.json"
            
            # Convert to dict for serialization
            config_dict = config.dict()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, default=str)
            
            self.logger.info("Saved table configuration", 
                           identifier=config.identifier, 
                           environment=environment,
                           file=str(file_path))
            return True
            
        except Exception as e:
            self.logger.error("Failed to save table configuration",
                            identifier=config.identifier, 
                            environment=environment,
                            error=str(e))
            return False
    
    def save_pipeline_config(self, config: PipelineConfig, environment: str = "default") -> bool:
        """Save pipeline configuration to file."""
        try:
            # Determine target directory
            if environment == "default":
                target_dir = self.pipelines_dir
            else:
                target_dir = self.environments_dir / environment / "pipelines"
                target_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = target_dir / f"{config.pipeline_id}.json"
            
            # Convert to dict for serialization
            config_dict = config.dict()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, default=str)
            
            self.logger.info("Saved pipeline configuration", 
                           pipeline_id=config.pipeline_id, 
                           environment=environment,
                           file=str(file_path))
            return True
            
        except Exception as e:
            self.logger.error("Failed to save pipeline configuration",
                            pipeline_id=config.pipeline_id, 
                            environment=environment,
                            error=str(e))
            return False
    
    def list_environments(self) -> List[str]:
        """List available environments."""
        environments = ["default"]
        
        if self.environments_dir.exists():
            for env_dir in self.environments_dir.iterdir():
                if env_dir.is_dir():
                    environments.append(env_dir.name)
        
        return environments
    
    def validate_configurations(self) -> Dict[str, List[str]]:
        """Validate all configurations and return any errors."""
        errors = {"table_configs": [], "pipeline_configs": []}
        
        # Validate table configurations
        try:
            table_configs = self.load_table_configs()
            for config in table_configs:
                try:
                    # Pydantic validation happens during loading
                    pass
                except Exception as e:
                    errors["table_configs"].append(f"{config.identifier}: {str(e)}")
        except Exception as e:
            errors["table_configs"].append(f"Failed to load table configs: {str(e)}")
        
        # Validate pipeline configurations
        try:
            pipeline_configs = self.load_pipeline_configs()
            for config in pipeline_configs:
                try:
                    # Pydantic validation happens during loading
                    pass
                except Exception as e:
                    errors["pipeline_configs"].append(f"{config.pipeline_id}: {str(e)}")
        except Exception as e:
            errors["pipeline_configs"].append(f"Failed to load pipeline configs: {str(e)}")
        
        return errors


class ConfigurationManager:
    """Main configuration manager focused on file-based configuration-as-code."""
    
    def __init__(self, 
                 config_dir: str,
                 environment: str = "default",
                 security_manager: Optional[SecurityManager] = None):
        self.config_source = FileConfigurationSource(config_dir)
        self.environment = environment
        self.security_manager = security_manager
        self.logger = logger.bind(manager="ConfigurationManager", environment=environment)
        
        # Cache for loaded configurations
        self._table_configs_cache: Optional[Dict[str, TableConfig]] = None
        self._pipeline_configs_cache: Optional[Dict[str, PipelineConfig]] = None
    
    def get_table_config(self, identifier: str) -> Optional[TableConfig]:
        """Get table configuration by identifier."""
        if self._table_configs_cache is None:
            self._load_table_configs()
        
        return self._table_configs_cache.get(identifier)
    
    def get_pipeline_config(self, pipeline_id: str) -> Optional[PipelineConfig]:
        """Get pipeline configuration by ID."""
        if self._pipeline_configs_cache is None:
            self._load_pipeline_configs()
        
        return self._pipeline_configs_cache.get(pipeline_id)
    
    def get_all_table_configs(self) -> List[TableConfig]:
        """Get all table configurations."""
        if self._table_configs_cache is None:
            self._load_table_configs()
        
        return list(self._table_configs_cache.values())
    
    def get_all_pipeline_configs(self) -> List[PipelineConfig]:
        """Get all pipeline configurations."""
        if self._pipeline_configs_cache is None:
            self._load_pipeline_configs()
        
        return list(self._pipeline_configs_cache.values())
    
    def get_configs_by_layer(self, layer: str) -> List[TableConfig]:
        """Get table configurations by layer."""
        all_configs = self.get_all_table_configs()
        return [config for config in all_configs if config.layer == layer]
    
    def get_configs_by_source_type(self, source_type: str) -> List[TableConfig]:
        """Get table configurations by source type."""
        all_configs = self.get_all_table_configs()
        return [config for config in all_configs if config.source_type == source_type]
    
    def refresh_cache(self):
        """Refresh configuration cache."""
        self._table_configs_cache = None
        self._pipeline_configs_cache = None
        self.logger.info("Configuration cache refreshed")
    
    def switch_environment(self, environment: str):
        """Switch to a different environment."""
        self.environment = environment
        self.refresh_cache()
        self.logger.info("Switched environment", new_environment=environment)
    
    def list_environments(self) -> List[str]:
        """List available environments."""
        return self.config_source.list_environments()
    
    def validate_all_configurations(self) -> Dict[str, List[str]]:
        """Validate all configurations."""
        return self.config_source.validate_configurations()
    
    def _load_table_configs(self):
        """Load table configurations from source."""
        self._table_configs_cache = {}
        
        try:
            configs = self.config_source.load_table_configs(self.environment)
            for config in configs:
                self._table_configs_cache[config.identifier] = config
        except Exception as e:
            self.logger.error("Failed to load table configurations", error=str(e))
            raise
    
    def _load_pipeline_configs(self):
        """Load pipeline configurations from source."""
        self._pipeline_configs_cache = {}
        
        try:
            configs = self.config_source.load_pipeline_configs(self.environment)
            for config in configs:
                self._pipeline_configs_cache[config.pipeline_id] = config
        except Exception as e:
            self.logger.error("Failed to load pipeline configurations", error=str(e))
            raise
    
    @classmethod
    def create_from_config(cls, config: Dict[str, Any]) -> 'ConfigurationManager':
        """Create configuration manager from configuration dictionary."""
        
        config_dir = config.get('config_dir', './configs')
        environment = config.get('environment', 'default')
        
        # Create security manager if configured
        security_manager = None
        if 'security' in config:
            security_manager = SecurityManager(config['security'])
        
        return cls(
            config_dir=config_dir,
            environment=environment,
            security_manager=security_manager
        ) 