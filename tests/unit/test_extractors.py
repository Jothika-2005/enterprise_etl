"""
Enterprise ETL Platform - Unit Tests: Extractors

Tests for CSV extractor, extractor factory, and base extractor
contract enforcement.
"""
import os
import tempfile

import pandas as pd
import pytest

from backend.core.extractors.base import BaseExtractor, ExtractionResult
from backend.core.extractors.csv_extractor import CSVExtractor
from backend.core.extractors.factory import get_extractor, register_extractor, list_extractors


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_csv(tmp_path) -> str:
    """Write a temp CSV and return its path."""
    df = pd.DataFrame({
        "id":    [1, 2, 3, 4, 5],
        "name":  ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
        "age":   [25, 30, 28, 35, 42],
    })
    path = str(tmp_path / "test.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_json(tmp_path) -> str:
    """Write a temp JSON Lines file and return its path."""
    df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    path = str(tmp_path / "test.jsonl")
    df.to_json(path, orient="records", lines=True)
    return path


@pytest.fixture
def sample_parquet(tmp_path) -> str:
    """Write a temp Parquet file and return its path."""
    df = pd.DataFrame({"val": [10, 20, 30]})
    path = str(tmp_path / "test.parquet")
    df.to_parquet(path, index=False)
    return path


# ── CSVExtractor ─────────────────────────────────────────────────────────────

class TestCSVExtractor:
    def test_extracts_csv_file(self, sample_csv):
        ext = CSVExtractor({"path": sample_csv, "name": "test_csv"})
        with ext:
            result = ext.extract()
        assert isinstance(result, ExtractionResult)
        assert result.records_count == 5
        assert not result.data.empty

    def test_raises_on_missing_file(self):
        ext = CSVExtractor({"path": "/nonexistent/file.csv", "name": "bad"})
        with pytest.raises(FileNotFoundError):
            with ext:
                ext.extract()

    def test_config_validation_requires_path(self):
        ext = CSVExtractor({"name": "no_path"})
        with pytest.raises(ValueError, match="Missing required config keys"):
            ext.validate_config()

    def test_column_subset_selection(self, sample_csv):
        ext = CSVExtractor({
            "path": sample_csv,
            "name": "test",
            "columns": ["id", "email"],
        })
        with ext:
            result = ext.extract()
        assert list(result.data.columns) == ["id", "email"]

    def test_extracts_parquet_file(self, sample_parquet):
        ext = CSVExtractor({"path": sample_parquet, "name": "test_parquet"})
        with ext:
            result = ext.extract()
        assert result.records_count == 3

    def test_metadata_contains_file_path(self, sample_csv):
        ext = CSVExtractor({"path": sample_csv, "name": "test"})
        with ext:
            result = ext.extract()
        assert "file_path" in result.metadata

    def test_extraction_time_is_positive(self, sample_csv):
        ext = CSVExtractor({"path": sample_csv, "name": "test"})
        with ext:
            result = ext.extract()
        assert result.extraction_time > 0

    def test_context_manager_connects_and_disconnects(self, sample_csv):
        ext = CSVExtractor({"path": sample_csv, "name": "test"})
        assert not ext._connected
        with ext:
            pass  # enter/exit
        assert not ext._connected


# ── ExtractionResult ──────────────────────────────────────────────────────────

class TestExtractionResult:
    def test_auto_counts_records(self):
        df = pd.DataFrame({"a": range(10)})
        result = ExtractionResult(data=df, source_name="test")
        assert result.records_count == 10

    def test_empty_df_returns_zero_count(self):
        df = pd.DataFrame()
        result = ExtractionResult(data=df, source_name="test")
        assert result.records_count == 0


# ── Extractor Factory ─────────────────────────────────────────────────────────

class TestExtractorFactory:
    def test_csv_type_returns_csv_extractor(self, sample_csv):
        ext = get_extractor({"type": "csv", "path": sample_csv})
        assert isinstance(ext, CSVExtractor)

    def test_unknown_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            get_extractor({"type": "unknown_source_xyz"})

    def test_parquet_type_returns_csv_extractor(self, sample_parquet):
        ext = get_extractor({"type": "parquet", "path": sample_parquet})
        assert isinstance(ext, CSVExtractor)

    def test_register_custom_extractor(self, sample_csv):
        class DummyExtractor(BaseExtractor):
            def extract(self, checkpoint_value=None):
                return ExtractionResult(data=pd.DataFrame(), source_name="dummy")

        register_extractor("dummy_type", DummyExtractor)
        ext = get_extractor({"type": "dummy_type"})
        assert isinstance(ext, DummyExtractor)

    def test_list_extractors_returns_dict(self):
        result = list_extractors()
        assert isinstance(result, dict)
        assert "csv" in result
        assert "postgresql" in result
        assert "api" in result

    def test_register_non_extractor_raises_type_error(self):
        with pytest.raises(TypeError):
            register_extractor("bad", object)  # type: ignore


# ── BaseExtractor contract ────────────────────────────────────────────────────

class TestBaseExtractorContract:
    def test_cannot_instantiate_base_directly(self):
        with pytest.raises(TypeError):
            BaseExtractor({})  # type: ignore

    def test_concrete_subclass_must_implement_extract(self):
        class IncompleteExtractor(BaseExtractor):
            pass  # Missing extract()

        with pytest.raises(TypeError):
            IncompleteExtractor({})

    def test_repr_contains_class_name(self, sample_csv):
        ext = CSVExtractor({"path": sample_csv, "name": "my_source"})
        assert "CSVExtractor" in repr(ext)
        assert "my_source" in repr(ext)
