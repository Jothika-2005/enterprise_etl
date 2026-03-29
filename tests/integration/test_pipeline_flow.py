"""
Enterprise ETL Platform - Integration Tests: Full Pipeline Flow

End-to-end tests that run a complete Extract → Transform → Quality → Load
cycle using in-memory SQLite and temp files (no Docker required).

Run with: pytest tests/integration/ -v
"""
import os
import sqlite3
import tempfile
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from backend.core.extractors.csv_extractor import CSVExtractor
from backend.core.transformers.engine import TransformationEngine
from backend.core.transformers.quality import DataQualityScorer
from backend.core.loaders.loader import CSVLoader


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def customer_csv(tmp_path_factory) -> str:
    """Generate a realistic customer CSV for integration testing."""
    tmp = tmp_path_factory.mktemp("data")
    path = str(tmp / "customers.csv")
    df = pd.DataFrame({
        "customer_id": [f"C{i:04d}" for i in range(1, 201)],
        "first_name":  ["Alice", "Bob"] * 100,
        "last_name":   ["Smith", "Jones"] * 100,
        "email": [f"user{i}@example.com" for i in range(1, 201)],
        "age":   list(range(20, 70)) * 4,
        "city":  (["New York", "London", "Tokyo", "Paris"] * 50),
        "signup_date": ["2023-01-15"] * 200,
    })
    # Inject quality issues
    df.loc[5, "email"] = "INVALID-EMAIL"
    df.loc[10, "first_name"] = None
    df.loc[15, "age"] = -1
    # Duplicate row
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    df.to_csv(path, index=False)
    return path


@pytest.fixture(scope="module")
def output_dir(tmp_path_factory) -> str:
    return str(tmp_path_factory.mktemp("output"))


# ── Extract phase ─────────────────────────────────────────────────────────────

class TestExtractionIntegration:
    def test_full_csv_extraction(self, customer_csv):
        ext = CSVExtractor({
            "path": customer_csv,
            "name": "integration_customers",
        })
        with ext:
            result = ext.extract()

        assert result.records_count == 201   # 200 + 1 duplicate
        assert "email" in result.data.columns
        assert result.extraction_time > 0

    def test_extraction_with_column_subset(self, customer_csv):
        ext = CSVExtractor({
            "path": customer_csv,
            "name": "subset_test",
            "columns": ["customer_id", "email", "age"],
        })
        with ext:
            result = ext.extract()

        assert set(result.data.columns) == {"customer_id", "email", "age"}


# ── Transform phase ───────────────────────────────────────────────────────────

class TestTransformationIntegration:
    @pytest.fixture
    def raw_df(self, customer_csv) -> pd.DataFrame:
        return pd.read_csv(customer_csv)

    def test_full_transformation_pipeline(self, raw_df):
        engine = TransformationEngine("integration_test")
        rules = [
            {"name": "remove_duplicates", "config": {"subset": ["email"]}},
            {"name": "fill_missing", "config": {
                "strategy": "constant", "fill_value": "UNKNOWN",
                "columns": ["first_name", "city"],
            }},
            {"name": "normalize_text", "config": {"columns": ["first_name", "last_name"]}},
            {"name": "validate_email", "config": {"column": "email", "action": "nullify"}},
            {"name": "cast_types", "config": {
                "mappings": {"age": "int", "signup_date": "datetime"},
                "errors": "coerce",
            }},
        ]
        result = engine.apply(raw_df, rules)

        # Duplicates removed
        assert result.records_out < result.records_in
        # No nulls in first_name after fill
        assert result.data["first_name"].isna().sum() == 0
        # All 5 rules should have applied
        assert len(result.rules_applied) == 5
        # No warnings for valid rules
        assert not any("Unknown" in w for w in result.warnings)

    def test_invalid_email_nullified(self, raw_df):
        engine = TransformationEngine("email_test")
        result = engine.apply(raw_df, [
            {"name": "validate_email", "config": {"column": "email", "action": "nullify"}}
        ])
        # Row 5 had INVALID-EMAIL — should now be null
        assert result.data.loc[5, "email"] != "INVALID-EMAIL"


# ── Quality phase ─────────────────────────────────────────────────────────────

class TestQualityIntegration:
    @pytest.fixture
    def transformed_df(self, customer_csv) -> pd.DataFrame:
        df = pd.read_csv(customer_csv)
        engine = TransformationEngine("quality_test")
        result = engine.apply(df, [
            {"name": "remove_duplicates", "config": {"subset": ["email"]}},
            {"name": "fill_missing", "config": {"strategy": "constant", "fill_value": "UNK"}},
            {"name": "validate_email", "config": {"column": "email", "action": "nullify"}},
        ])
        return result.data

    def test_quality_score_reasonable_after_cleaning(self, transformed_df):
        config = {
            "max_null_ratio": 0.20,
            "max_duplicate_ratio": 0.02,
            "min_records": 100,
            "required_columns": ["email"],
            "unique_keys": ["customer_id"],
        }
        scorer = DataQualityScorer("integration", config)
        report = scorer.evaluate(transformed_df)

        # After cleaning, quality should be reasonable
        assert report.composite_score >= 0.50
        assert report.total_records >= 100

    def test_quality_report_has_all_categories(self, transformed_df):
        scorer = DataQualityScorer("integration", {})
        report = scorer.evaluate(transformed_df)
        categories = {c.category for c in report.checks}
        assert "completeness" in categories
        assert "uniqueness" in categories
        assert "consistency" in categories


# ── Load phase ────────────────────────────────────────────────────────────────

class TestLoaderIntegration:
    def test_csv_load_writes_file(self, output_dir):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        out_path = os.path.join(output_dir, "test_output.csv")
        loader = CSVLoader({"path": out_path, "name": "csv_test"})
        result = loader.load(df)

        assert result.success
        assert result.records_loaded == 3
        assert os.path.exists(out_path)
        loaded = pd.read_csv(out_path)
        assert len(loaded) == 3

    def test_csv_append_mode(self, output_dir):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        out_path = os.path.join(output_dir, "append_test.csv")

        loader = CSVLoader({"path": out_path, "name": "append_test", "mode": "write"})
        loader.load(df)

        loader2 = CSVLoader({"path": out_path, "name": "append_test", "mode": "append"})
        result = loader2.load(df)

        assert result.success
        loaded = pd.read_csv(out_path)
        assert len(loaded) == 4  # 2 + 2


# ── Full end-to-end pipeline ──────────────────────────────────────────────────

class TestFullPipeline:
    def test_extract_transform_quality_load(self, customer_csv, output_dir):
        """Simulate a complete ETL pipeline run without database or Docker."""

        # 1. Extract
        ext = CSVExtractor({"path": customer_csv, "name": "e2e_test"})
        with ext:
            extract_result = ext.extract()

        assert extract_result.records_count > 0

        # 2. Transform
        engine = TransformationEngine("e2e_pipeline")
        transform_result = engine.apply(
            extract_result.data,
            [
                {"name": "remove_duplicates", "config": {"subset": ["email"]}},
                {"name": "fill_missing", "config": {
                    "strategy": "constant", "fill_value": "UNKNOWN"
                }},
                {"name": "normalize_text"},
                {"name": "validate_email", "config": {"action": "nullify"}},
            ]
        )

        assert transform_result.records_out <= extract_result.records_count
        assert len(transform_result.rules_applied) == 4

        # 3. Quality
        scorer = DataQualityScorer("e2e_pipeline", {"min_records": 50})
        quality_report = scorer.evaluate(transform_result.data)

        assert 0.0 <= quality_report.composite_score <= 1.0

        # 4. Load
        out_path = os.path.join(output_dir, "e2e_output.csv")
        loader = CSVLoader({"path": out_path, "name": "e2e_loader"})
        load_result = loader.load(transform_result.data)

        assert load_result.success
        assert load_result.records_loaded == transform_result.records_out

        # Verify output file is correct
        loaded_df = pd.read_csv(out_path)
        assert len(loaded_df) == transform_result.records_out
        assert set(transform_result.data.columns) == set(loaded_df.columns)

        print(f"\n✅ E2E Pipeline complete:")
        print(f"   Extracted  : {extract_result.records_count}")
        print(f"   Transformed: {transform_result.records_out}")
        print(f"   Quality    : {quality_report.composite_score:.2%}")
        print(f"   Loaded     : {load_result.records_loaded}")
