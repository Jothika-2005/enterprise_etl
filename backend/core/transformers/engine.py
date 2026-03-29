"""
Enterprise ETL Platform - Data Transformation Engine

Rule-based transformation pipeline supporting Pandas operations.
Each transformation is a self-contained, testable unit.
"""
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from backend.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TransformationResult:
    """Result returned after applying all transformations."""
    data: pd.DataFrame
    records_in: int
    records_out: int
    rules_applied: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def records_dropped(self) -> int:
        return self.records_in - self.records_out


# ── Individual Transformation Functions ─────────────────────────────────────

def remove_duplicates(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Remove duplicate rows.

    Config:
        subset (list): Columns to consider. Default: all.
        keep (str): 'first' | 'last' | False. Default: 'first'.
    """
    before = len(df)
    subset = config.get("subset")
    keep = config.get("keep", "first")
    df = df.drop_duplicates(subset=subset, keep=keep)
    logger.debug(f"remove_duplicates: {before - len(df)} rows removed")
    return df


def fill_missing_values(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Fill or drop missing values.

    Config:
        strategy (str): 'mean' | 'median' | 'mode' | 'constant' | 'ffill' | 'bfill' | 'drop'.
        fill_value (any): Used when strategy='constant'.
        columns (list): Columns to apply to. Default: all.
        threshold (float): Drop row if null ratio exceeds this. Default: None.
    """
    columns = config.get("columns", df.columns.tolist())
    strategy = config.get("strategy", "constant")
    fill_value = config.get("fill_value", "UNKNOWN")
    threshold = config.get("threshold")

    # Drop rows exceeding null threshold
    if threshold is not None:
        null_ratio = df[columns].isnull().mean(axis=1)
        df = df[null_ratio <= threshold]

    for col in columns:
        if col not in df.columns:
            continue
        if strategy == "mean" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "median" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "mode":
            mode_val = df[col].mode()
            df[col] = df[col].fillna(mode_val.iloc[0] if not mode_val.empty else fill_value)
        elif strategy == "ffill":
            df[col] = df[col].ffill()
        elif strategy == "bfill":
            df[col] = df[col].bfill()
        elif strategy == "drop":
            df = df.dropna(subset=[col])
        else:  # constant
            df[col] = df[col].fillna(fill_value)

    return df


def normalize_text_columns(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalize string columns: strip, lowercase, remove extra spaces.

    Config:
        columns (list): Columns to normalize. Default: all string columns.
        lowercase (bool): Convert to lowercase. Default: True.
        strip (bool): Strip whitespace. Default: True.
    """
    columns = config.get("columns")
    lowercase = config.get("lowercase", True)
    strip = config.get("strip", True)

    if columns is None:
        columns = df.select_dtypes(include=["object", "string"]).columns.tolist()

    for col in columns:
        if col not in df.columns:
            continue
        if strip:
            df[col] = df[col].astype(str).str.strip()
        if lowercase:
            df[col] = df[col].str.lower()
        # Collapse multiple spaces
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
        # Replace string 'nan' back to NaN
        df[col] = df[col].replace("nan", np.nan)

    return df


def validate_email_column(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Validate and optionally nullify invalid email addresses.

    Config:
        column (str): Column containing emails. Default: 'email'.
        action (str): 'nullify' | 'drop'. Default: 'nullify'.
    """
    col = config.get("column", "email")
    action = config.get("action", "nullify")
    email_pattern = re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$")

    if col not in df.columns:
        return df

    invalid_mask = ~df[col].astype(str).str.match(email_pattern) & df[col].notna()
    count = invalid_mask.sum()

    if count > 0:
        logger.warning(f"Found {count} invalid emails in column '{col}'")
        if action == "drop":
            df = df[~invalid_mask]
        else:
            df.loc[invalid_mask, col] = np.nan

    return df


def cast_data_types(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Cast columns to specified data types.

    Config:
        mappings (dict): {column_name: dtype_string}
        errors (str): 'coerce' | 'raise'. Default: 'coerce'.
    """
    mappings = config.get("mappings", {})
    errors = config.get("errors", "coerce")

    for col, dtype in mappings.items():
        if col not in df.columns:
            continue
        try:
            if dtype in ("int", "int64"):
                df[col] = pd.to_numeric(df[col], errors=errors).astype("Int64")
            elif dtype in ("float", "float64"):
                df[col] = pd.to_numeric(df[col], errors=errors)
            elif dtype in ("datetime", "date"):
                df[col] = pd.to_datetime(df[col], errors=errors)
            elif dtype == "bool":
                df[col] = df[col].astype(bool)
            elif dtype == "str":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            logger.warning(f"Could not cast column '{col}' to '{dtype}': {e}")

    return df


def rename_columns(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Rename columns based on a mapping.

    Config:
        mapping (dict): {old_name: new_name}
    """
    mapping = config.get("mapping", {})
    return df.rename(columns=mapping)


def filter_rows(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Filter rows using a pandas query expression.

    Config:
        expression (str): Pandas query string, e.g. "age > 18 and status == 'active'"
    """
    expression = config.get("expression")
    if not expression:
        return df
    before = len(df)
    df = df.query(expression)
    logger.debug(f"filter_rows: {before - len(df)} rows removed by expression '{expression}'")
    return df


def add_audit_columns(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Append standard ETL audit columns.

    Config:
        pipeline_name (str): Pipeline identifier.
        run_id (str): Current run UUID.
    """
    from datetime import timezone, datetime
    df["_etl_pipeline"] = config.get("pipeline_name", "unknown")
    df["_etl_run_id"] = config.get("run_id", "")
    df["_etl_loaded_at"] = datetime.now(timezone.utc)
    return df


def select_columns(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Select / reorder a subset of columns.

    Config:
        columns (list): Ordered list of column names to keep.
    """
    columns = config.get("columns", [])
    available = [c for c in columns if c in df.columns]
    return df[available] if available else df


def aggregate_daily(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Aggregate records by date.

    Config:
        date_column (str): Name of date column.
        group_by (list): Additional grouping columns.
        agg_config (dict): {column: aggregation_function}
    """
    date_col = config.get("date_column", "date")
    group_by = config.get("group_by", [date_col])
    agg_config = config.get("agg_config", {})

    if not agg_config:
        # Default: sum all numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        agg_config = {col: "sum" for col in numeric_cols if col not in group_by}

    if not agg_config:
        return df

    return df.groupby(group_by).agg(agg_config).reset_index()


# ── Transformation Registry ───────────────────────────────────────────────────

_TRANSFORMATION_REGISTRY: Dict[str, Callable] = {
    "remove_duplicates": remove_duplicates,
    "fill_missing": fill_missing_values,
    "normalize_text": normalize_text_columns,
    "validate_email": validate_email_column,
    "cast_types": cast_data_types,
    "rename_columns": rename_columns,
    "filter_rows": filter_rows,
    "add_audit_columns": add_audit_columns,
    "select_columns": select_columns,
    "aggregate_daily": aggregate_daily,
}


def register_transformation(name: str, fn: Callable) -> None:
    """Register a custom transformation function."""
    _TRANSFORMATION_REGISTRY[name] = fn
    logger.info(f"Registered transformation: {name}")


# ── Transformation Engine ─────────────────────────────────────────────────────

class TransformationEngine:
    """
    Applies an ordered sequence of transformation rules to a DataFrame.

    Example pipeline config transformations section:
        transformations:
          - name: remove_duplicates
          - name: fill_missing
            config:
              strategy: mean
          - name: normalize_text
          - name: cast_types
            config:
              mappings:
                age: int
                signup_date: datetime
    """

    def __init__(self, pipeline_name: str = "default") -> None:
        self.pipeline_name = pipeline_name
        self.logger = get_logger(f"transformer.{pipeline_name}")

    def apply(
        self,
        df: pd.DataFrame,
        rules: List[Any],
        run_id: Optional[str] = None,
    ) -> TransformationResult:
        """
        Apply transformation rules sequentially to the DataFrame.

        Args:
            df: Input DataFrame.
            rules: List of transformation specs (str or dict with name/config).
            run_id: Current pipeline run ID for audit columns.

        Returns:
            TransformationResult with the transformed DataFrame.
        """
        start = time.monotonic()
        records_in = len(df)
        rules_applied = []
        warnings = []

        for rule in rules:
            # Rule can be a string ("remove_duplicates") or a dict
            if isinstance(rule, str):
                rule_name = rule
                rule_config: Dict[str, Any] = {}
            elif isinstance(rule, dict):
                rule_name = rule.get("name", "")
                rule_config = rule.get("config", {})
            else:
                warnings.append(f"Invalid rule format: {rule}")
                continue

            if rule_name not in _TRANSFORMATION_REGISTRY:
                warnings.append(f"Unknown transformation: '{rule_name}'")
                self.logger.warning(f"Skipping unknown transformation: {rule_name}")
                continue

            try:
                before = len(df)
                # Inject context into config
                rule_config["pipeline_name"] = self.pipeline_name
                if run_id:
                    rule_config["run_id"] = run_id

                fn = _TRANSFORMATION_REGISTRY[rule_name]
                df = fn(df, rule_config)
                rules_applied.append(rule_name)
                self.logger.debug(
                    f"Applied transformation: {rule_name}",
                    records_before=before,
                    records_after=len(df),
                )
            except Exception as exc:
                msg = f"Transformation '{rule_name}' failed: {exc}"
                warnings.append(msg)
                self.logger.error(msg, exc_info=True)
                # Continue with next rule (fault-tolerant)

        elapsed = time.monotonic() - start

        self.logger.info(
            "Transformation complete",
            records_in=records_in,
            records_out=len(df),
            rules_applied=rules_applied,
            duration_seconds=round(elapsed, 3),
        )

        return TransformationResult(
            data=df,
            records_in=records_in,
            records_out=len(df),
            rules_applied=rules_applied,
            warnings=warnings,
            duration_seconds=elapsed,
        )
