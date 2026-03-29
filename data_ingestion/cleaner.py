"""
Enterprise ETL Platform - Auto Cleaning Pipeline

Takes a raw DataFrame from FileParser, runs auto-cleaning,
and returns a before/after report with the cleaned data.
"""
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class ColumnProfile:
    """Profile stats for a single column."""
    name: str
    dtype: str
    null_count: int
    null_pct: float
    unique_count: int
    unique_pct: float
    sample_values: List[Any]
    min_val: Optional[Any] = None
    max_val: Optional[Any] = None
    mean_val: Optional[float] = None
    suggested_action: str = "keep"


@dataclass
class CleaningReport:
    """Full before/after report for one file's cleaning run."""
    filename: str
    original_rows: int
    original_cols: int
    cleaned_rows: int
    cleaned_cols: int
    duplicates_removed: int
    nulls_filled: int
    columns_dropped: int
    column_profiles: List[ColumnProfile] = field(default_factory=list)
    actions_taken: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def rows_removed(self) -> int:
        return self.original_rows - self.cleaned_rows

    @property
    def quality_delta(self) -> str:
        pct = round((self.rows_removed / max(self.original_rows, 1)) * 100, 1)
        return f"{pct}% rows cleaned"


class AutoCleaner:
    """
    Runs a configurable auto-cleaning pass on a DataFrame.

    Steps:
    1. Drop fully-empty columns
    2. Strip string whitespace
    3. Remove full-duplicate rows
    4. Fill missing values (strategy per dtype)
    5. Normalise column names
    6. Infer and coerce data types
    """

    def __init__(
        self,
        max_null_pct: float = 0.80,
        fill_numeric: str = "median",
        fill_string: str = "UNKNOWN",
        normalise_col_names: bool = True,
    ):
        self.max_null_pct = max_null_pct
        self.fill_numeric = fill_numeric
        self.fill_string = fill_string
        self.normalise_col_names = normalise_col_names

    def clean(self, df: pd.DataFrame, filename: str = "file") -> Tuple[pd.DataFrame, CleaningReport]:
        """
        Clean DataFrame and return (cleaned_df, CleaningReport).

        Args:
            df:       Raw DataFrame from FileParser.
            filename: Source filename for the report.

        Returns:
            Tuple of cleaned DataFrame and full CleaningReport.
        """
        original_rows, original_cols = df.shape
        actions: List[str] = []
        warnings: List[str] = []

        # 1. Normalise column names
        if self.normalise_col_names:
            df.columns = [self._clean_col_name(c) for c in df.columns]
            actions.append("Column names normalised (lowercase, snake_case)")

        # 2. Profile original (before cleaning) columns
        profiles = self._profile_columns(df)

        # 3. Drop columns with too many nulls
        before_cols = len(df.columns)
        high_null_cols = [
            p.name for p in profiles
            if p.null_pct >= self.max_null_pct
        ]
        if high_null_cols:
            df = df.drop(columns=high_null_cols, errors="ignore")
            actions.append(f"Dropped {len(high_null_cols)} column(s) with >{self.max_null_pct*100:.0f}% nulls: {high_null_cols}")
        cols_dropped = before_cols - len(df.columns)

        # 4. Strip string whitespace
        str_cols = df.select_dtypes(include=["object"]).columns
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": np.nan, "None": np.nan, "": np.nan})
        if len(str_cols):
            actions.append(f"Stripped whitespace in {len(str_cols)} string column(s)")

        # 5. Remove full duplicates
        before_dup = len(df)
        df = df.drop_duplicates()
        dups_removed = before_dup - len(df)
        if dups_removed:
            actions.append(f"Removed {dups_removed} duplicate rows")

        # 6. Fill missing values
        nulls_filled = 0
        for col in df.columns:
            null_count = df[col].isna().sum()
            if null_count == 0:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                fill_val = df[col].median() if self.fill_numeric == "median" else df[col].mean()
                df[col] = df[col].fillna(round(fill_val, 4) if not pd.isna(fill_val) else 0)
                nulls_filled += null_count
            else:
                df[col] = df[col].fillna(self.fill_string)
                nulls_filled += null_count
        if nulls_filled:
            actions.append(f"Filled {nulls_filled} missing values (numeric→{self.fill_numeric}, string→'{self.fill_string}')")

        # 7. Coerce object columns that look numeric
        for col in df.select_dtypes(include=["object"]).columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().mean() > 0.90:
                df[col] = converted
                actions.append(f"Auto-cast '{col}' to numeric")

        report = CleaningReport(
            filename=filename,
            original_rows=original_rows,
            original_cols=original_cols,
            cleaned_rows=len(df),
            cleaned_cols=len(df.columns),
            duplicates_removed=dups_removed,
            nulls_filled=nulls_filled,
            columns_dropped=cols_dropped,
            column_profiles=profiles,
            actions_taken=actions,
            warnings=warnings,
        )

        return df.reset_index(drop=True), report

    def _profile_columns(self, df: pd.DataFrame) -> List[ColumnProfile]:
        """Generate per-column statistics."""
        profiles = []
        for col in df.columns:
            series = df[col]
            null_count = int(series.isna().sum())
            null_pct = round(null_count / max(len(df), 1), 4)
            unique_count = int(series.nunique())
            unique_pct = round(unique_count / max(len(df), 1), 4)
            sample = [v for v in series.dropna().head(3).tolist()]

            min_val = max_val = mean_val = None
            if pd.api.types.is_numeric_dtype(series):
                min_val = float(series.min()) if not series.isna().all() else None
                max_val = float(series.max()) if not series.isna().all() else None
                mean_val = round(float(series.mean()), 4) if not series.isna().all() else None

            # Suggest action
            action = "keep"
            if null_pct >= self.max_null_pct:
                action = "drop (too many nulls)"
            elif unique_pct >= 0.99 and null_pct == 0:
                action = "possible ID column"
            elif unique_count == 1:
                action = "constant — consider dropping"

            profiles.append(ColumnProfile(
                name=col,
                dtype=str(series.dtype),
                null_count=null_count,
                null_pct=null_pct,
                unique_count=unique_count,
                unique_pct=unique_pct,
                sample_values=sample,
                min_val=min_val,
                max_val=max_val,
                mean_val=mean_val,
                suggested_action=action,
            ))
        return profiles

    @staticmethod
    def _clean_col_name(name: str) -> str:
        """Normalise column name to snake_case."""
        name = str(name).strip().lower()
        name = re.sub(r"[\s\-./]+", "_", name)
        name = re.sub(r"[^\w]", "", name)
        name = re.sub(r"_+", "_", name).strip("_")
        return name or "col"
