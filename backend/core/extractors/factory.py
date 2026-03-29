"""
Enterprise ETL Platform - Extractor Factory

Plugin-based registry pattern. New extractors can be registered
without modifying existing code (Open/Closed Principle).
"""
from typing import Any, Dict, Type

from backend.core.extractors.base import BaseExtractor
from backend.core.extractors.csv_extractor import CSVExtractor
from backend.core.extractors.sql_extractor import SQLExtractor
from backend.core.extractors.api_extractor import APIExtractor
from backend.utils.logger import get_logger

logger = get_logger(__name__)

# ── Extractor Registry ───────────────────────────────────────────────────────
_EXTRACTOR_REGISTRY: Dict[str, Type[BaseExtractor]] = {
    "csv": CSVExtractor,
    "tsv": CSVExtractor,
    "json": CSVExtractor,
    "parquet": CSVExtractor,
    "excel": CSVExtractor,
    "xlsx": CSVExtractor,
    "postgresql": SQLExtractor,
    "mysql": SQLExtractor,
    "sqlite": SQLExtractor,
    "sql": SQLExtractor,
    "api": APIExtractor,
    "rest": APIExtractor,
}


def register_extractor(source_type: str, extractor_class: Type[BaseExtractor]) -> None:
    """
    Register a custom extractor plugin.

    Args:
        source_type: String key identifying the source type.
        extractor_class: Class inheriting from BaseExtractor.

    Example:
        register_extractor("sftp", SFTPExtractor)
    """
    if not issubclass(extractor_class, BaseExtractor):
        raise TypeError(f"{extractor_class} must inherit from BaseExtractor")
    _EXTRACTOR_REGISTRY[source_type] = extractor_class
    logger.info(f"Registered extractor: {source_type} → {extractor_class.__name__}")


def get_extractor(config: Dict[str, Any]) -> BaseExtractor:
    """
    Factory function to instantiate the correct extractor from config.

    Args:
        config: Source configuration dict with required key 'type'.

    Returns:
        BaseExtractor: Instantiated extractor.

    Raises:
        ValueError: If source type is not registered.

    Example:
        extractor = get_extractor({
            "type": "csv",
            "name": "customers",
            "path": "/data/customers.csv"
        })
    """
    source_type = config.get("type", "").lower()
    if source_type not in _EXTRACTOR_REGISTRY:
        available = list(_EXTRACTOR_REGISTRY.keys())
        raise ValueError(
            f"Unknown source type '{source_type}'. Available: {available}"
        )

    extractor_cls = _EXTRACTOR_REGISTRY[source_type]
    logger.debug(f"Instantiating extractor: {extractor_cls.__name__}", source_type=source_type)
    return extractor_cls(config)


def list_extractors() -> Dict[str, str]:
    """Return all registered extractor types and their class names."""
    return {k: v.__name__ for k, v in _EXTRACTOR_REGISTRY.items()}
