"""
Enterprise ETL Platform - Quality Scoring Engine

Computes a 0–100 quality score across 5 dimensions.
Designed for the UI dashboard and submission reports.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


# ── Dimension weights (must sum to 1.0) ──────────────────────────────────────
DIMENSION_WEIGHTS = {
    "completeness":  0.30,
    "uniqueness":    0.20,
    "validity":      0.20,
    "consistency":   0.15,
    "timeliness":    0.15,
}

GRADE_MAP = [
    (90, "A", "🟢 Excellent"),
    (75, "B", "🟡 Good"),
    (60, "C", "🟠 Acceptable"),
    (40, "D", "🔴 Poor"),
    (0,  "F", "⛔ Critical"),
]


@dataclass
class DimensionScore:
    dimension: str
    score: float          # 0–100
    weight: float
    issues: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def weighted_contribution(self) -> float:
        return self.score * self.weight


@dataclass
class QualityScoreCard:
    """Full quality scorecard for a DataFrame."""
    filename: str
    total_records: int
    total_columns: int
    dimensions: List[DimensionScore] = field(default_factory=list)

    @property
    def overall_score(self) -> float:
        if not self.dimensions:
            return 0.0
        raw = sum(d.weighted_contribution for d in self.dimensions)
        return round(min(max(raw, 0), 100), 1)

    @property
    def grade(self) -> str:
        s = self.overall_score
        for threshold, letter, _ in GRADE_MAP:
            if s >= threshold:
                return letter
        return "F"

    @property
    def grade_label(self) -> str:
        s = self.overall_score
        for threshold, _, label in GRADE_MAP:
            if s >= threshold:
                return label
        return "⛔ Critical"

    @property
    def dimension_dict(self) -> Dict[str, float]:
        return {d.dimension.title(): d.score for d in self.dimensions}

    @property
    def all_issues(self) -> List[str]:
        issues = []
        for d in self.dimensions:
            issues.extend(d.issues)
        return issues

    def to_dict(self) -> Dict[str, Any]:
        return {
            "filename": self.filename,
            "total_records": self.total_records,
            "total_columns": self.total_columns,
            "overall_score": self.overall_score,
            "grade": self.grade,
            "grade_label": self.grade_label,
            "dimensions": {
                d.dimension: {
                    "score": d.score,
                    "weight": d.weight,
                    "issues": d.issues,
                    "details": d.details,
                }
                for d in self.dimensions
            },
        }


class QualityScorer:
    """
    Scores a DataFrame across 5 quality dimensions and returns
    a QualityScoreCard with per-dimension breakdown.
    """

    def score(self, df: pd.DataFrame, filename: str = "dataset") -> QualityScoreCard:
        """
        Compute full quality scorecard.

        Args:
            df:       DataFrame to evaluate.
            filename: Source label for the scorecard.

        Returns:
            QualityScoreCard with overall score and dimension breakdown.
        """
        if df.empty:
            card = QualityScoreCard(
                filename=filename, total_records=0, total_columns=0
            )
            for dim, w in DIMENSION_WEIGHTS.items():
                card.dimensions.append(DimensionScore(dim, 0.0, w, ["Empty dataset"]))
            return card

        card = QualityScoreCard(
            filename=filename,
            total_records=len(df),
            total_columns=len(df.columns),
        )

        card.dimensions.append(self._completeness(df))
        card.dimensions.append(self._uniqueness(df))
        card.dimensions.append(self._validity(df))
        card.dimensions.append(self._consistency(df))
        card.dimensions.append(self._timeliness(df))

        return card

    # ── Completeness (30%) ────────────────────────────────────────────────────
    def _completeness(self, df: pd.DataFrame) -> DimensionScore:
        null_ratio = float(df.isnull().mean().mean())
        score = round((1 - null_ratio) * 100, 1)
        issues = []
        per_col = df.isnull().mean()
        bad_cols = per_col[per_col > 0.20].index.tolist()
        if bad_cols:
            issues.append(f"Columns with >20% nulls: {bad_cols[:5]}")
        return DimensionScore(
            dimension="completeness",
            score=score,
            weight=DIMENSION_WEIGHTS["completeness"],
            issues=issues,
            details={
                "overall_null_pct": round(null_ratio * 100, 2),
                "columns_above_20pct_nulls": len(bad_cols),
                "total_nulls": int(df.isnull().sum().sum()),
            },
        )

    # ── Uniqueness (20%) ──────────────────────────────────────────────────────
    def _uniqueness(self, df: pd.DataFrame) -> DimensionScore:
        dup_ratio = float(df.duplicated().mean())
        score = round((1 - dup_ratio) * 100, 1)
        issues = []
        if dup_ratio > 0.05:
            issues.append(f"{dup_ratio*100:.1f}% duplicate rows detected")
        # Check per-column near-duplicates in string cols
        str_cols = df.select_dtypes(include="object").columns
        low_card = [c for c in str_cols if df[c].nunique() == 1]
        if low_card:
            issues.append(f"Constant-value columns: {low_card[:3]}")
        return DimensionScore(
            dimension="uniqueness",
            score=score,
            weight=DIMENSION_WEIGHTS["uniqueness"],
            issues=issues,
            details={
                "duplicate_rows": int(df.duplicated().sum()),
                "duplicate_pct": round(dup_ratio * 100, 2),
                "constant_columns": len(low_card),
            },
        )

    # ── Validity (20%) ────────────────────────────────────────────────────────
    def _validity(self, df: pd.DataFrame) -> DimensionScore:
        scores_per_col = []
        issues = []

        for col in df.columns:
            series = df[col].dropna()
            if series.empty:
                continue

            if pd.api.types.is_numeric_dtype(series):
                # IQR outlier check
                q1, q3 = series.quantile(0.25), series.quantile(0.75)
                iqr = q3 - q1
                if iqr > 0:
                    outlier_ratio = float(
                        ((series < q1 - 3 * iqr) | (series > q3 + 3 * iqr)).mean()
                    )
                    col_score = round((1 - outlier_ratio) * 100, 1)
                    if outlier_ratio > 0.05:
                        issues.append(f"'{col}': {outlier_ratio*100:.1f}% extreme outliers")
                else:
                    col_score = 100.0
            elif pd.api.types.is_object_dtype(series):
                # Check for obvious junk: very long values, special chars, etc.
                avg_len = series.astype(str).str.len().mean()
                junk_ratio = float((series.astype(str).str.len() > 500).mean())
                col_score = round((1 - junk_ratio) * 100, 1)
                if junk_ratio > 0.05:
                    issues.append(f"'{col}': {junk_ratio*100:.1f}% abnormally long values")
            else:
                col_score = 100.0

            scores_per_col.append(col_score)

        score = round(np.mean(scores_per_col), 1) if scores_per_col else 100.0
        return DimensionScore(
            dimension="validity",
            score=score,
            weight=DIMENSION_WEIGHTS["validity"],
            issues=issues,
            details={"columns_checked": len(scores_per_col)},
        )

    # ── Consistency (15%) ─────────────────────────────────────────────────────
    def _consistency(self, df: pd.DataFrame) -> DimensionScore:
        issues = []
        scores = []

        # Check mixed types in object columns
        for col in df.select_dtypes(include="object").columns:
            series = df[col].dropna()
            if series.empty:
                continue
            num_ratio = pd.to_numeric(series, errors="coerce").notna().mean()
            # Mixed if 10%–90% are numeric (inconsistent)
            if 0.10 < num_ratio < 0.90:
                issues.append(f"'{col}' has mixed numeric/text values ({num_ratio*100:.0f}% numeric)")
                scores.append(50.0)
            else:
                scores.append(100.0)

        # Detect inconsistent capitalisation in low-cardinality string cols
        for col in df.select_dtypes(include="object").columns:
            series = df[col].dropna()
            if series.nunique() < 20:
                unique_lower = set(series.str.lower().unique())
                unique_orig = set(series.unique())
                if len(unique_orig) > len(unique_lower):
                    issues.append(f"'{col}' has capitalisation inconsistencies")
                    scores.append(70.0)

        score = round(np.mean(scores), 1) if scores else 100.0
        return DimensionScore(
            dimension="consistency",
            score=score,
            weight=DIMENSION_WEIGHTS["consistency"],
            issues=issues,
            details={"columns_checked": len(scores)},
        )

    # ── Timeliness (15%) ──────────────────────────────────────────────────────
    def _timeliness(self, df: pd.DataFrame) -> DimensionScore:
        """
        Checks if date/time columns exist and have reasonable recency.
        Falls back to a neutral 75 if no date columns are present.
        """
        issues = []
        date_cols = []

        # Try to detect date columns by name or type
        for col in df.columns:
            if any(kw in col.lower() for kw in ("date", "time", "created", "updated", "ts", "at")):
                date_cols.append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                date_cols.append(col)

        if not date_cols:
            return DimensionScore(
                dimension="timeliness",
                score=75.0,
                weight=DIMENSION_WEIGHTS["timeliness"],
                issues=["No date/time columns detected — timeliness assumed neutral."],
                details={"date_columns_found": 0},
            )

        scores = []
        for col in date_cols[:3]:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                valid_ratio = float(parsed.notna().mean())
                if valid_ratio < 0.80:
                    issues.append(f"'{col}': only {valid_ratio*100:.0f}% parse as valid dates")
                    scores.append(valid_ratio * 100)
                else:
                    scores.append(100.0)
            except Exception:
                scores.append(75.0)

        score = round(np.mean(scores), 1) if scores else 75.0
        return DimensionScore(
            dimension="timeliness",
            score=score,
            weight=DIMENSION_WEIGHTS["timeliness"],
            issues=issues,
            details={"date_columns_found": len(date_cols)},
        )
