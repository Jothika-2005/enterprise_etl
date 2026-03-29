"""
Shared pytest fixtures for the ETL Platform test suite.
"""
import os
import sys

import pandas as pd
import pytest

# Ensure backend package is importable from tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="session")
def minimal_df() -> pd.DataFrame:
    """Tiny DataFrame for quick tests."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "email": ["a@x.com", "b@x.com", "c@x.com"],
        "value": [10.0, 20.5, None],
    })


@pytest.fixture(scope="session")
def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["id", "name", "email", "value"])


@pytest.fixture(scope="module")
def tmp_csv(tmp_path_factory) -> str:
    """Write a minimal CSV and return its path."""
    p = tmp_path_factory.mktemp("data") / "sample.csv"
    df = pd.DataFrame({
        "id": range(1, 11),
        "val": [float(i) for i in range(1, 11)],
        "label": [f"item_{i}" for i in range(1, 11)],
    })
    df.to_csv(str(p), index=False)
    return str(p)
