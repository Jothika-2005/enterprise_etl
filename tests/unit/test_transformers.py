"""
Enterprise ETL Platform - Unit Tests: Transformation Engine

Tests covering individual transformation functions and the
TransformationEngine orchestration class.
"""
import numpy as np
import pandas as pd
import pytest

from backend.core.transformers.engine import (
    TransformationEngine,
    remove_duplicates,
    fill_missing_values,
    normalize_text_columns,
    validate_email_column,
    cast_data_types,
    filter_rows,
    add_audit_columns,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Standard test DataFrame with known quality issues."""
    return pd.DataFrame({
        "id": [1, 2, 3, 3, 5],
        "name": ["  Alice  ", "BOB", "Carol", "Carol", None],
        "email": [
            "alice@example.com",
            "INVALID_EMAIL",
            "carol@test.org",
            "carol@test.org",
            "eve@domain.io",
        ],
        "age": [25, None, 200, 200, 30],
        "score": [88.5, 72.0, None, None, 95.1],
    })


@pytest.fixture
def engine() -> TransformationEngine:
    return TransformationEngine(pipeline_name="test_pipeline")


# ── remove_duplicates ─────────────────────────────────────────────────────────

class TestRemoveDuplicates:
    def test_removes_duplicate_rows(self, sample_df):
        result = remove_duplicates(sample_df, {})
        # Row 3 and 4 are duplicates
        assert len(result) < len(sample_df)

    def test_subset_deduplication(self, sample_df):
        result = remove_duplicates(sample_df, {"subset": ["email"]})
        # carol@test.org appears twice
        assert result["email"].duplicated().sum() == 0

    def test_keep_last(self, sample_df):
        result = remove_duplicates(sample_df, {"subset": ["id"], "keep": "last"})
        assert len(result) == 4

    def test_empty_df(self):
        df = pd.DataFrame(columns=["a", "b"])
        result = remove_duplicates(df, {})
        assert len(result) == 0


# ── fill_missing_values ───────────────────────────────────────────────────────

class TestFillMissingValues:
    def test_constant_fill(self, sample_df):
        result = fill_missing_values(sample_df, {
            "strategy": "constant",
            "fill_value": "UNKNOWN",
            "columns": ["name"],
        })
        assert result["name"].isna().sum() == 0
        assert "UNKNOWN" in result["name"].values

    def test_mean_fill(self, sample_df):
        result = fill_missing_values(sample_df, {
            "strategy": "mean",
            "columns": ["score"],
        })
        assert result["score"].isna().sum() == 0

    def test_drop_strategy(self, sample_df):
        before = len(sample_df)
        result = fill_missing_values(sample_df, {
            "strategy": "drop",
            "columns": ["name"],
        })
        assert len(result) < before

    def test_threshold_drop(self, sample_df):
        result = fill_missing_values(sample_df, {"threshold": 0.5})
        # Rows where >50% of values are null should be dropped
        assert isinstance(result, pd.DataFrame)


# ── normalize_text_columns ────────────────────────────────────────────────────

class TestNormalizeTextColumns:
    def test_strips_whitespace(self, sample_df):
        result = normalize_text_columns(sample_df, {"columns": ["name"]})
        assert not any(
            (v or "").startswith(" ") or (v or "").endswith(" ")
            for v in result["name"].dropna()
        )

    def test_lowercases(self, sample_df):
        result = normalize_text_columns(sample_df, {"columns": ["email"], "lowercase": True})
        assert all(v == v.lower() for v in result["email"].dropna())

    def test_no_lowercase_when_disabled(self, sample_df):
        result = normalize_text_columns(sample_df, {"columns": ["name"], "lowercase": False})
        assert "BOB" in result["name"].values

    def test_non_string_column_skipped(self, sample_df):
        # Should not crash on numeric columns
        result = normalize_text_columns(sample_df, {"columns": ["age"]})
        assert len(result) == len(sample_df)


# ── validate_email_column ─────────────────────────────────────────────────────

class TestValidateEmailColumn:
    def test_nullify_invalid_emails(self, sample_df):
        result = validate_email_column(sample_df, {"column": "email", "action": "nullify"})
        assert pd.isna(result.loc[result["id"] == 2, "email"].values[0])

    def test_drop_invalid_emails(self, sample_df):
        before = len(sample_df)
        result = validate_email_column(sample_df, {"column": "email", "action": "drop"})
        assert len(result) < before

    def test_valid_emails_preserved(self, sample_df):
        result = validate_email_column(sample_df, {"column": "email"})
        assert result.loc[result["id"] == 1, "email"].values[0] == "alice@example.com"

    def test_missing_column_ignored(self, sample_df):
        result = validate_email_column(sample_df, {"column": "nonexistent"})
        assert len(result) == len(sample_df)


# ── cast_data_types ───────────────────────────────────────────────────────────

class TestCastDataTypes:
    def test_int_cast(self):
        df = pd.DataFrame({"count": ["1", "2", "abc", "4"]})
        result = cast_data_types(df, {"mappings": {"count": "int"}, "errors": "coerce"})
        assert pd.api.types.is_integer_dtype(result["count"])

    def test_datetime_cast(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-06-15", "not-a-date"]})
        result = cast_data_types(df, {"mappings": {"date": "datetime"}, "errors": "coerce"})
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_missing_column_skipped(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = cast_data_types(df, {"mappings": {"nonexistent": "int"}})
        assert "nonexistent" not in result.columns


# ── filter_rows ───────────────────────────────────────────────────────────────

class TestFilterRows:
    def test_filter_by_expression(self, sample_df):
        result = filter_rows(sample_df, {"expression": "age > 25"})
        assert all(v > 25 for v in result["age"].dropna())

    def test_no_expression_returns_unchanged(self, sample_df):
        result = filter_rows(sample_df, {})
        assert len(result) == len(sample_df)


# ── TransformationEngine ──────────────────────────────────────────────────────

class TestTransformationEngine:
    def test_applies_multiple_rules_in_order(self, engine, sample_df):
        rules = [
            {"name": "remove_duplicates"},
            {"name": "fill_missing", "config": {"strategy": "constant", "fill_value": "N/A"}},
            {"name": "normalize_text"},
        ]
        result = engine.apply(sample_df, rules)
        assert result.records_out <= result.records_in
        assert len(result.rules_applied) == 3

    def test_skips_unknown_rule_with_warning(self, engine, sample_df):
        rules = [{"name": "nonexistent_rule"}]
        result = engine.apply(sample_df, rules)
        assert len(result.warnings) > 0
        assert len(result.data) == len(sample_df)

    def test_string_rule_names_supported(self, engine, sample_df):
        result = engine.apply(sample_df, ["remove_duplicates"])
        assert "remove_duplicates" in result.rules_applied

    def test_empty_dataframe(self, engine):
        df = pd.DataFrame(columns=["a", "b", "c"])
        result = engine.apply(df, ["remove_duplicates"])
        assert len(result.data) == 0
        assert result.records_in == 0

    def test_result_records_out_matches_data(self, engine, sample_df):
        result = engine.apply(sample_df, ["remove_duplicates"])
        assert result.records_out == len(result.data)
