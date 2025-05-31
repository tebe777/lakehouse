"""
Data ingestion framework for lakehouse ETL.
"""

from .base_extractor import BaseExtractor
from .csv_extractor import CSVExtractor
from .raw_loader import RawDataLoader

__all__ = [
    "BaseExtractor",
    "CSVExtractor",
    "RawDataLoader"
]
