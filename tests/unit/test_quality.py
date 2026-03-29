"""
Enterprise ETL Platform - Unit Tests: Data Quality Scorer

Tests for completeness, uniqueness, validity, consistency checks
and the composite quality score computation.
"""
import numpy as np
import pandas as pd
import pytest

from backend.core.transformers.quality import DataQualityScorer, QualityReport, QualityCheck


@pytest.fixture
def clean_df() -> pd.DataFrame:
    """A high-quality DataFrame — should score close to 1.0."""
    return pd.DataFrame({
        "id":    [1, 2, 3, 4, 5],
        "email": [
            "a@ex.com", "b@ex.com", "c@ex.com", "d@ex.com", "e@ex.com"
        ],
        "age":   [25, 30, 22, 45, 38],
        "name":  ["Alice", "Bob", "Carol", "Dave", "Eve"],
    })


@pytest.fixture
def dirty_df() -> pd.DataFrame:
    """A low-quality DataFrame — should score much lower."""
    return pd.DataFrame({
        "id":    [1, 1, 3, None, 5],          # duplicate, null
        "email": ["a@ex.com", "a@ex.com", None, "d@ex.com", "e@ex.com"],
        "age":   [25, 25, -5, 999, 38],        # out-of-range
        "name":  [None, None, None, None, "Eve"],  # lots of nulls
    })


@pytest.fixture
def base_config() -> dict:
    return {
        "max_null_ratio": 0.20,
        "max_duplicate_ratio": 0.05,
        "min_records": 3,
        "required_columns": ["email"],
        "unique_keys": ["email"],
        "range_checks": [{"column": "age", "min": 0, "max": 120}],
    }


class TestCompletenessChecks:
    def test_low_null_ratio_passes(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        null_check = next(c for c in report.checks if c.name == "overall_null_ratio")
        assert null_check.passed

    def test_high_null_ratio_fails(self, dirty_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(dirty_df)
        null_check = next(c for c in report.checks if c.name == "overall_null_ratio")
        assert not null_check.passed

    def test_required_column_missing_fails(self, base_config):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})  # no 'email'
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(df)
        email_check = next(
            (c for c in report.checks if "email" in c.name and "exists" in c.name), None
        )
        assert email_check is not None
        assert not email_check.passed

    def test_required_column_present_passes(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        email_null_check = next(
            (c for c in report.checks if "required_column_not_null_email" in c.name), None
        )
        assert email_null_check is not None
        assert email_null_check.passed


class TestUniquenessChecks:
    def test_no_duplicates_passes(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        dup_check = next(c for c in report.checks if c.name == "duplicate_row_ratio")
        assert dup_check.passed

    def test_duplicates_above_threshold_fails(self, dirty_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(dirty_df)
        dup_check = next(c for c in report.checks if c.name == "duplicate_row_ratio")
        assert not dup_check.passed

    def test_unique_key_violation_detected(self, dirty_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(dirty_df)
        key_check = next(
            (c for c in report.checks if "unique_key_email" in c.name), None
        )
        assert key_check is not None
        assert not key_check.passed

    def test_unique_key_clean_data_passes(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        key_check = next(
            (c for c in report.checks if "unique_key_email" in c.name), None
        )
        assert key_check is not None
        assert key_check.passed


class TestValidityChecks:
    def test_range_check_clean_data_passes(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        range_check = next(
            (c for c in report.checks if "range_check_age" in c.name), None
        )
        assert range_check is not None
        assert range_check.passed

    def test_range_check_dirty_data_fails(self, dirty_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(dirty_df)
        range_check = next(
            (c for c in report.checks if "range_check_age" in c.name), None
        )
        assert range_check is not None
        assert not range_check.passed

    def test_regex_check_passes(self):
        df = pd.DataFrame({"zip": ["12345", "90210", "10001"]})
        config = {"regex_checks": [{"column": "zip", "pattern": r"^\d{5}$"}]}
        scorer = DataQualityScorer("test", config)
        report = scorer.evaluate(df)
        regex_check = next(
            (c for c in report.checks if "regex_check_zip" in c.name), None
        )
        assert regex_check is not None
        assert regex_check.passed

    def test_regex_check_fails_on_invalid(self):
        df = pd.DataFrame({"zip": ["12345", "ABCDE", "1234"]})
        config = {"regex_checks": [{"column": "zip", "pattern": r"^\d{5}$"}]}
        scorer = DataQualityScorer("test", config)
        report = scorer.evaluate(df)
        regex_check = next(c for c in report.checks if "regex_check_zip" in c.name)
        assert not regex_check.passed


class TestConsistencyChecks:
    def test_minimum_record_count_passes(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        count_check = next(c for c in report.checks if c.name == "minimum_record_count")
        assert count_check.passed

    def test_minimum_record_count_fails(self, base_config):
        df = pd.DataFrame({"email": ["a@b.com"]})  # only 1 record, min=3
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(df)
        count_check = next(c for c in report.checks if c.name == "minimum_record_count")
        assert not count_check.passed

    def test_expected_columns_check(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        config = {"expected_columns": ["a", "b", "c"]}
        scorer = DataQualityScorer("test", config)
        report = scorer.evaluate(df)
        schema_check = next(
            (c for c in report.checks if c.name == "expected_schema_columns"), None
        )
        assert schema_check is not None
        assert not schema_check.passed
        assert "c" in schema_check.details.get("missing_columns", [])


class TestCompositeScore:
    def test_clean_data_scores_high(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        assert report.composite_score >= 0.70

    def test_dirty_data_scores_low(self, dirty_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(dirty_df)
        assert report.composite_score < 0.70

    def test_score_between_0_and_1(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        assert 0.0 <= report.composite_score <= 1.0

    def test_empty_df_returns_zero_or_low_score(self, base_config):
        df = pd.DataFrame(columns=["email", "age", "name", "id"])
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(df)
        # Empty DF should fail minimum record count
        count_check = next(c for c in report.checks if c.name == "minimum_record_count")
        assert not count_check.passed

    def test_report_to_dict_structure(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        d = report.to_dict()
        assert "composite_score" in d
        assert "checks" in d
        assert isinstance(d["checks"], list)
        assert all("name" in c for c in d["checks"])

    def test_pass_fail_counts_sum_correctly(self, clean_df, base_config):
        scorer = DataQualityScorer("test", base_config)
        report = scorer.evaluate(clean_df)
        assert report.passed_checks + report.failed_checks == len(report.checks)
