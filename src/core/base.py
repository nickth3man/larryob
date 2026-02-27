"""Base classes for ETL operations."""

from abc import ABC, abstractmethod
from typing import Any


class BaseETL(ABC):
    """Base class for ETL operations."""

    @abstractmethod
    def run(self) -> Any:
        """Execute the ETL operation."""
        pass


class BaseExtractor(ABC):
    """Base class for data extraction."""

    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Extract data from source."""
        pass


class BaseTransformer(ABC):
    """Base class for data transformation."""

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform extracted data."""
        pass


class BaseLoader(ABC):
    """Base class for data loading."""

    @abstractmethod
    def load(self, data: Any) -> None:
        """Load transformed data to destination."""
        pass
