"""
Enterprise ETL Platform - Abstract Base Extractor

Defines the plugin-based extractor interface. All extractors
must inherit from BaseExtractor and implement extract().
"""
import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

import pandas as pd

from backend.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ExtractionResult:
    """
    Result object returned by every extractor.

    Attributes:
        data: Extracted DataFrame.
        source_name: Name of the data source.
        records_count: Total records extracted.
        extraction_time: Seconds taken to extract.
        metadata: Additional extraction metadata.
        checkpoint_value: Latest watermark for incremental loads.
        errors: List of non-fatal errors encountered.
    """
    data: pd.DataFrame
    source_name: str
    records_count: int = 0
    extraction_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    checkpoint_value: Optional[str] = None
    errors: list = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.records_count == 0 and not self.data.empty:
            self.records_count = len(self.data)


class BaseExtractor(abc.ABC):
    """
    Abstract base class for all data extractors.

    Implements the Template Method pattern:
    1. validate_config()
    2. connect()
    3. extract() — must be implemented by subclasses
    4. disconnect()
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize extractor with configuration.

        Args:
            config: Source configuration dictionary loaded from YAML.
        """
        self.config = config
        self.source_name = config.get("name", self.__class__.__name__)
        self.logger = get_logger(f"extractor.{self.source_name}")
        self._connected = False

    @abc.abstractmethod
    def extract(
        self,
        checkpoint_value: Optional[str] = None,
    ) -> ExtractionResult:
        """
        Extract data from the source.

        Args:
            checkpoint_value: Last watermark value for incremental loads.

        Returns:
            ExtractionResult with extracted DataFrame.
        """
        ...

    def validate_config(self) -> None:
        """
        Validate extractor configuration.
        Subclasses should override to add source-specific validation.
        """
        required = self.get_required_config_keys()
        missing = [k for k in required if k not in self.config]
        if missing:
            raise ValueError(
                f"[{self.source_name}] Missing required config keys: {missing}"
            )

    def get_required_config_keys(self) -> list:
        """Return list of required configuration keys. Override in subclasses."""
        return []

    def connect(self) -> None:
        """Establish connection to the data source. Override if needed."""
        pass

    def disconnect(self) -> None:
        """Close connection to the data source. Override if needed."""
        self._connected = False

    def __enter__(self) -> "BaseExtractor":
        """Context manager entry — validates config and connects."""
        self.validate_config()
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit — always disconnects."""
        self.disconnect()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} source={self.source_name}>"
