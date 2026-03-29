"""
Enterprise ETL Platform - Data Quality Scorer

Evaluates a DataFrame against configurable quality rules and
produces a composite quality score (0.0–1.0).
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from backend.utils.logger import get_logger

logger = get_logger(__name__)

# Category weights for composite score
CATEGORY_WEIGHTS = {
    "completeness": 0.35,
    "uniqueness": 0.25,
    "validity": 0.25,
    "consistency": 0.15,
}


@dataclass
class QualityCheck:
    """Result of a single quality check."""
    name: str
    category: str
    passed: bool
    expected_value: Optional[float] = None
    actual_value: Optional[float] = None
    threshold: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)
    weight: float = 1.0


@dataclass
class QualityReport:
    """Aggregated quality report for a pipeline run."""
    pipeline_name: str
    total_records: int
    checks: List[QualityCheck] = field(default_factory=list)

    @property
    def composite_score(self) -> float:
        """
        Weighted composite quality score (0.0–1.0).

        Each category's score = pass_rate of checks in that category.
        Final score = weighted average across categories.
        """
        if not self.checks:
            return 0.0

        category_scores: Dict[str, List[float]] = {k: [] for k in CATEGORY_WEIGHTS}
        for check in self.checks:
            cat = check.category if check.category in category_scores else "validity"
            category_scores[cat].append(1.0 if check.passed else 0.0)

        weighted_sum = 0.0
        weight_total = 0.0
        for category, weight in CATEGORY_WEIGHTS.items():
            scores = category_scores[category]
            if scores:
                weighted_sum += np.mean(scores) * weight
                weight_total += weight

        return round(weighted_sum / weight_total, 4) if weight_total > 0 else 0.0

    @property
    def passed_checks(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def failed_checks(self) -> int:
        return sum(1 for c in self.checks if not c.passed)

    @property
    def pass_rate(self) -> float:
        total = len(self.checks)
        return round(self.passed_checks / total, 4) if total > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "total_records": self.total_records,
            "composite_score": self.composite_score,
            "pass_rate": self.pass_rate,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "checks": [
                {
                    "name": c.name,
                    "category": c.category,
                    "passed": c.passed,
                    "actual_value": c.actual_value,
                    "threshold": c.threshold,
                    "details": c.details,
                }
                for c in self.checks
            ],
        }


class DataQualityScorer:
    """
    Evaluates data quality across completeness, uniqueness,
    validity, and consistency dimensions.
    """

    def __init__(self, pipeline_name: str, config: Dict[str, Any]) -> None:
        self.pipeline_name = pipeline_name
        self.config = config
        self.logger = get_logger(f"quality.{pipeline_name}")

    def evaluate(self, df: pd.DataFrame) -> QualityReport:
        """
        Run all quality checks against the DataFrame.

        Args:
            df: Transformed DataFrame ready for evaluation.

        Returns:
            QualityReport with composite score and individual checks.
        """
        report = QualityReport(pipeline_name=self.pipeline_name, total_records=len(df))

        # ── Completeness ────────────────────────────────────────────────────
        report.checks.extend(self._check_completeness(df))

        # ── Uniqueness ──────────────────────────────────────────────────────
        report.checks.extend(self._check_uniqueness(df))

        # ── Validity ────────────────────────────────────────────────────────
        report.checks.extend(self._check_validity(df))

        # ── Consistency ─────────────────────────────────────────────────────
        report.checks.extend(self._check_consistency(df))

        self.logger.info(
            "Quality evaluation complete",
            composite_score=report.composite_score,
            passed=report.passed_checks,
            failed=report.failed_checks,
            total_records=len(df),
        )

        return report

    def _check_completeness(self, df: pd.DataFrame) -> List[QualityCheck]:
        checks = []
        max_null_ratio = self.config.get("max_null_ratio", 0.20)
        required_columns = self.config.get("required_columns", [])

        # Overall null ratio
        if len(df) > 0:
            overall_null = df.isnull().mean().mean()
            checks.append(QualityCheck(
                name="overall_null_ratio",
                category="completeness",
                passed=overall_null <= max_null_ratio,
                actual_value=round(float(overall_null), 4),
                threshold=max_null_ratio,
                details={"null_ratio_per_column": df.isnull().mean().round(4).to_dict()},
            ))

        # Required columns not null
        for col in required_columns:
            if col not in df.columns:
                checks.append(QualityCheck(
                    name=f"required_column_exists_{col}",
                    category="completeness",
                    passed=False,
                    details={"column": col, "reason": "Column missing from DataFrame"},
                ))
            else:
                null_ratio = float(df[col].isnull().mean())
                checks.append(QualityCheck(
                    name=f"required_column_not_null_{col}",
                    category="completeness",
                    passed=null_ratio == 0.0,
                    actual_value=round(null_ratio, 4),
                    threshold=0.0,
                    details={"column": col},
                ))

        return checks

    def _check_uniqueness(self, df: pd.DataFrame) -> List[QualityCheck]:
        checks = []
        max_dup_ratio = self.config.get("max_duplicate_ratio", 0.05)
        unique_keys = self.config.get("unique_keys", [])

        if len(df) > 0:
            dup_ratio = float(df.duplicated().mean())
            checks.append(QualityCheck(
                name="duplicate_row_ratio",
                category="uniqueness",
                passed=dup_ratio <= max_dup_ratio,
                actual_value=round(dup_ratio, 4),
                threshold=max_dup_ratio,
                details={"duplicate_count": int(df.duplicated().sum())},
            ))

        for key_cols in unique_keys:
            if isinstance(key_cols, str):
                key_cols = [key_cols]
            available = [c for c in key_cols if c in df.columns]
            if not available:
                continue
            dup_count = int(df.duplicated(subset=available).sum())
            checks.append(QualityCheck(
                name=f"unique_key_{'_'.join(available)}",
                category="uniqueness",
                passed=dup_count == 0,
                actual_value=float(dup_count),
                threshold=0,
                details={"columns": available, "duplicate_count": dup_count},
            ))

        return checks

    def _check_validity(self, df: pd.DataFrame) -> List[QualityCheck]:
        checks = []
        range_checks = self.config.get("range_checks", [])
        regex_checks = self.config.get("regex_checks", [])

        for rc in range_checks:
            col = rc.get("column")
            min_val = rc.get("min")
            max_val = rc.get("max")
            if col not in df.columns:
                continue
            numeric = pd.to_numeric(df[col], errors="coerce")
            mask = pd.Series([True] * len(df))
            if min_val is not None:
                mask &= numeric >= min_val
            if max_val is not None:
                mask &= numeric <= max_val
            invalid = (~mask & numeric.notna()).sum()
            checks.append(QualityCheck(
                name=f"range_check_{col}",
                category="validity",
                passed=int(invalid) == 0,
                actual_value=float(invalid),
                threshold=0,
                details={"column": col, "min": min_val, "max": max_val},
            ))

        for rxc in regex_checks:
            import re
            col = rxc.get("column")
            pattern = rxc.get("pattern")
            if col not in df.columns or not pattern:
                continue
            matches = df[col].astype(str).str.match(pattern)
            invalid = (~matches & df[col].notna()).sum()
            checks.append(QualityCheck(
                name=f"regex_check_{col}",
                category="validity",
                passed=int(invalid) == 0,
                actual_value=float(invalid),
                threshold=0,
                details={"column": col, "pattern": pattern},
            ))

        return checks

    def _check_consistency(self, df: pd.DataFrame) -> List[QualityCheck]:
        checks = []
        min_records = self.config.get("min_records", 1)
        expected_columns = self.config.get("expected_columns", [])

        # Minimum record count
        checks.append(QualityCheck(
            name="minimum_record_count",
            category="consistency",
            passed=len(df) >= min_records,
            actual_value=float(len(df)),
            threshold=float(min_records),
            details={"record_count": len(df)},
        ))

        # Expected columns present
        if expected_columns:
            missing = [c for c in expected_columns if c not in df.columns]
            checks.append(QualityCheck(
                name="expected_schema_columns",
                category="consistency",
                passed=len(missing) == 0,
                details={"missing_columns": missing},
            ))

        return checks
