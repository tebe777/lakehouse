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