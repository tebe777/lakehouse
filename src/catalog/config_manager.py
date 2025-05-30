"""
Configuration manager for loading and managing table and pipeline configurations.
"""

import json
import yaml
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from abc import ABC, abstractmethod
import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from ..common.models.table_config import TableConfig, ColumnConfig, ValidationRule
from ..common.models.pipeline_config import PipelineConfig
from ..common.connections.security_manager import SecurityManager

logger = structlog.get_logger(__name__)


class ConfigurationSource(ABC):
    """Abstract base class for configuration sources."""
    
    @abstractmethod
    def load_table_configs(self) -> List[TableConfig]:
        """Load table configurations."""
        pass
    
    @abstractmethod
    def load_pipeline_configs(self) -> List[PipelineConfig]:
        """Load pipeline configurations."""
        pass
    
    @abstractmethod
    def save_table_config(self, config: TableConfig) -> bool:
        """Save table configuration."""
        pass
    
    @abstractmethod
    def save_pipeline_config(self, config: PipelineConfig) -> bool:
        """Save pipeline configuration."""
        pass


class FileConfigurationSource(ConfigurationSource):
    """Configuration source using local files."""
    
    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)
        self.tables_dir = self.config_dir / "tables"
        self.pipelines_dir = self.config_dir / "pipelines"
        self.logger = logger.bind(source="FileConfigurationSource")
        
        # Ensure directories exist
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
    
    def load_table_configs(self) -> List[TableConfig]:
        """Load table configurations from files."""
        configs = []
        
        # Load from JSON files
        for json_file in self.tables_dir.glob("*.json"):
            try:
                configs.extend(self._load_table_configs_from_file(json_file))
            except Exception as e:
                self.logger.error("Failed to load table config from file",
                                file=str(json_file), error=str(e))
        
        # Load from YAML files
        for yaml_file in self.tables_dir.glob("*.yaml"):
            try:
                configs.extend(self._load_table_configs_from_file(yaml_file))
            except Exception as e:
                self.logger.error("Failed to load table config from file",
                                file=str(yaml_file), error=str(e))
        
        self.logger.info("Loaded table configurations", count=len(configs))
        return configs
    
    def load_pipeline_configs(self) -> List[PipelineConfig]:
        """Load pipeline configurations from files."""
        configs = []
        
        # Load from JSON files
        for json_file in self.pipelines_dir.glob("*.json"):
            try:
                configs.extend(self._load_pipeline_configs_from_file(json_file))
            except Exception as e:
                self.logger.error("Failed to load pipeline config from file",
                                file=str(json_file), error=str(e))
        
        # Load from YAML files
        for yaml_file in self.pipelines_dir.glob("*.yaml"):
            try:
                configs.extend(self._load_pipeline_configs_from_file(yaml_file))
            except Exception as e:
                self.logger.error("Failed to load pipeline config from file",
                                file=str(yaml_file), error=str(e))
        
        self.logger.info("Loaded pipeline configurations", count=len(configs))
        return configs
    
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
    
    def save_table_config(self, config: TableConfig) -> bool:
        """Save table configuration to file."""
        try:
            file_path = self.tables_dir / f"{config.identifier.replace('.', '_')}.json"
            
            # Convert to dict for serialization
            config_dict = config.dict()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, default=str)
            
            self.logger.info("Saved table configuration", 
                           identifier=config.identifier, 
                           file=str(file_path))
            return True
            
        except Exception as e:
            self.logger.error("Failed to save table configuration",
                            identifier=config.identifier, error=str(e))
            return False
    
    def save_pipeline_config(self, config: PipelineConfig) -> bool:
        """Save pipeline configuration to file."""
        try:
            file_path = self.pipelines_dir / f"{config.pipeline_id}.json"
            
            # Convert to dict for serialization
            config_dict = config.dict()
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, default=str)
            
            self.logger.info("Saved pipeline configuration", 
                           pipeline_id=config.pipeline_id, 
                           file=str(file_path))
            return True
            
        except Exception as e:
            self.logger.error("Failed to save pipeline configuration",
                            pipeline_id=config.pipeline_id, error=str(e))
            return False


class DatabaseConfigurationSource(ConfigurationSource):
    """Configuration source using database."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = logger.bind(source="DatabaseConfigurationSource")
        
        # Initialize database schema if needed
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Initialize database schema for configuration storage."""
        # This would create tables for storing configurations
        # For now, we'll assume tables exist
        pass
    
    def load_table_configs(self) -> List[TableConfig]:
        """Load table configurations from database."""
        configs = []
        
        try:
            with self.engine.connect() as conn:
                # Query table configurations
                result = conn.execute(text("""
                    SELECT config_data 
                    FROM table_configurations 
                    WHERE active = true
                """))
                
                for row in result:
                    config_data = json.loads(row.config_data)
                    configs.append(self._parse_table_config(config_data))
            
            self.logger.info("Loaded table configurations from database", count=len(configs))
            
        except Exception as e:
            self.logger.error("Failed to load table configurations from database", 
                            error=str(e))
        
        return configs
    
    def load_pipeline_configs(self) -> List[PipelineConfig]:
        """Load pipeline configurations from database."""
        configs = []
        
        try:
            with self.engine.connect() as conn:
                # Query pipeline configurations
                result = conn.execute(text("""
                    SELECT config_data 
                    FROM pipeline_configurations 
                    WHERE active = true
                """))
                
                for row in result:
                    config_data = json.loads(row.config_data)
                    configs.append(PipelineConfig(**config_data))
            
            self.logger.info("Loaded pipeline configurations from database", count=len(configs))
            
        except Exception as e:
            self.logger.error("Failed to load pipeline configurations from database", 
                            error=str(e))
        
        return configs
    
    def _parse_table_config(self, data: Dict[str, Any]) -> TableConfig:
        """Parse table configuration from dictionary."""
        # Similar to file source parsing
        if 'columns' in data:
            columns = []
            for col_data in data['columns']:
                columns.append(ColumnConfig(**col_data))
            data['columns'] = columns
        
        if 'validation_rules' in data:
            rules = []
            for rule_data in data['validation_rules']:
                rules.append(ValidationRule(**rule_data))
            data['validation_rules'] = rules
        
        return TableConfig(**data)
    
    def save_table_config(self, config: TableConfig) -> bool:
        """Save table configuration to database."""
        try:
            config_json = json.dumps(config.dict(), default=str)
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO table_configurations (identifier, config_data, active, created_at)
                    VALUES (:identifier, :config_data, true, NOW())
                    ON CONFLICT (identifier) 
                    DO UPDATE SET config_data = :config_data, updated_at = NOW()
                """), {
                    'identifier': config.identifier,
                    'config_data': config_json
                })
                conn.commit()
            
            self.logger.info("Saved table configuration to database", 
                           identifier=config.identifier)
            return True
            
        except Exception as e:
            self.logger.error("Failed to save table configuration to database",
                            identifier=config.identifier, error=str(e))
            return False
    
    def save_pipeline_config(self, config: PipelineConfig) -> bool:
        """Save pipeline configuration to database."""
        try:
            config_json = json.dumps(config.dict(), default=str)
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO pipeline_configurations (pipeline_id, config_data, active, created_at)
                    VALUES (:pipeline_id, :config_data, true, NOW())
                    ON CONFLICT (pipeline_id) 
                    DO UPDATE SET config_data = :config_data, updated_at = NOW()
                """), {
                    'pipeline_id': config.pipeline_id,
                    'config_data': config_json
                })
                conn.commit()
            
            self.logger.info("Saved pipeline configuration to database", 
                           pipeline_id=config.pipeline_id)
            return True
            
        except Exception as e:
            self.logger.error("Failed to save pipeline configuration to database",
                            pipeline_id=config.pipeline_id, error=str(e))
            return False


class ConfigurationManager:
    """Main configuration manager that can use multiple sources."""
    
    def __init__(self, 
                 primary_source: ConfigurationSource,
                 fallback_sources: Optional[List[ConfigurationSource]] = None,
                 security_manager: Optional[SecurityManager] = None):
        self.primary_source = primary_source
        self.fallback_sources = fallback_sources or []
        self.security_manager = security_manager
        self.logger = logger.bind(manager="ConfigurationManager")
        
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
    
    def _load_table_configs(self):
        """Load table configurations from sources."""
        self._table_configs_cache = {}
        
        # Try primary source first
        try:
            configs = self.primary_source.load_table_configs()
            for config in configs:
                self._table_configs_cache[config.identifier] = config
        except Exception as e:
            self.logger.error("Failed to load from primary source", error=str(e))
            
            # Try fallback sources
            for source in self.fallback_sources:
                try:
                    configs = source.load_table_configs()
                    for config in configs:
                        if config.identifier not in self._table_configs_cache:
                            self._table_configs_cache[config.identifier] = config
                except Exception as e:
                    self.logger.warning("Failed to load from fallback source", error=str(e))
    
    def _load_pipeline_configs(self):
        """Load pipeline configurations from sources."""
        self._pipeline_configs_cache = {}
        
        # Try primary source first
        try:
            configs = self.primary_source.load_pipeline_configs()
            for config in configs:
                self._pipeline_configs_cache[config.pipeline_id] = config
        except Exception as e:
            self.logger.error("Failed to load pipeline configs from primary source", error=str(e))
            
            # Try fallback sources
            for source in self.fallback_sources:
                try:
                    configs = source.load_pipeline_configs()
                    for config in configs:
                        if config.pipeline_id not in self._pipeline_configs_cache:
                            self._pipeline_configs_cache[config.pipeline_id] = config
                except Exception as e:
                    self.logger.warning("Failed to load pipeline configs from fallback source", error=str(e))
    
    @classmethod
    def create_from_config(cls, config: Dict[str, Any]) -> 'ConfigurationManager':
        """Create configuration manager from configuration dictionary."""
        
        # Create primary source
        primary_config = config.get('primary_source', {})
        source_type = primary_config.get('type', 'file')
        
        if source_type == 'file':
            primary_source = FileConfigurationSource(
                config_dir=primary_config.get('config_dir', './configs')
            )
        elif source_type == 'database':
            primary_source = DatabaseConfigurationSource(
                connection_string=primary_config.get('connection_string')
            )
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        # Create fallback sources
        fallback_sources = []
        for fallback_config in config.get('fallback_sources', []):
            source_type = fallback_config.get('type')
            if source_type == 'file':
                fallback_sources.append(FileConfigurationSource(
                    config_dir=fallback_config.get('config_dir')
                ))
            elif source_type == 'database':
                fallback_sources.append(DatabaseConfigurationSource(
                    connection_string=fallback_config.get('connection_string')
                ))
        
        # Create security manager if configured
        security_manager = None
        if 'security' in config:
            security_manager = SecurityManager(config['security'])
        
        return cls(
            primary_source=primary_source,
            fallback_sources=fallback_sources,
            security_manager=security_manager
        ) 