"""
Enterprise ETL Platform - Data Loader

Loads transformed DataFrames to destination targets.
Supports PostgreSQL (upsert/append/replace), CSV, and Parquet.
"""
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from backend.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LoadResult:
    """Result of a data load operation."""
    destination: str
    records_loaded: int
    records_failed: int
    duration_seconds: float
    load_mode: str
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None


class BaseLoader(ABC):
    """Abstract base loader."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.destination_name = config.get("name", self.__class__.__name__)
        self.logger = get_logger(f"loader.{self.destination_name}")

    @abstractmethod
    def load(self, df: pd.DataFrame) -> LoadResult:
        """Load DataFrame to destination."""
        ...


class PostgreSQLLoader(BaseLoader):
    """
    Loads data into PostgreSQL using SQLAlchemy.

    Config:
        connection_string (str): SQLAlchemy connection URL.
        table (str): Target table name.
        schema (str): Target schema. Default: public.
        mode (str): 'append' | 'replace' | 'upsert'. Default: append.
        upsert_key (list): Columns for upsert conflict resolution.
        chunksize (int): Rows per INSERT batch. Default: 5000.
    """

    def load(self, df: pd.DataFrame) -> LoadResult:
        start = time.monotonic()
        table = self.config["table"]
        schema = self.config.get("schema", "public")
        mode = self.config.get("mode", "append")
        chunksize = self.config.get("chunksize", 5000)
        conn_str = self.config["connection_string"]

        engine = create_engine(conn_str, pool_pre_ping=True)
        records_loaded = 0
        error = None

        try:
            if mode == "upsert":
                records_loaded = self._upsert(df, engine, table, schema, chunksize)
            else:
                if_exists = "replace" if mode == "replace" else "append"
                df.to_sql(
                    name=table,
                    con=engine,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    chunksize=chunksize,
                    method="multi",
                )
                records_loaded = len(df)

            self.logger.info(
                "PostgreSQL load complete",
                table=f"{schema}.{table}",
                records=records_loaded,
                mode=mode,
            )
        except Exception as exc:
            error = str(exc)
            self.logger.error("PostgreSQL load failed", error=error, exc_info=True)
        finally:
            engine.dispose()

        return LoadResult(
            destination=f"{schema}.{table}",
            records_loaded=records_loaded,
            records_failed=len(df) - records_loaded if error else 0,
            duration_seconds=time.monotonic() - start,
            load_mode=mode,
            error=error,
        )

    def _upsert(
        self, df: pd.DataFrame, engine, table: str, schema: str, chunksize: int
    ) -> int:
        """Upsert using PostgreSQL INSERT ... ON CONFLICT DO UPDATE."""
        upsert_key = self.config.get("upsert_key", [])
        if not upsert_key:
            raise ValueError("upsert_key required for upsert mode")

        cols = list(df.columns)
        update_cols = [c for c in cols if c not in upsert_key]
        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholder = ", ".join([f":{c}" for c in cols])
        conflict = ", ".join(f'"{c}"' for c in upsert_key)
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

        sql = f"""
            INSERT INTO {schema}.{table} ({col_list})
            VALUES ({placeholder})
            ON CONFLICT ({conflict}) DO UPDATE SET {update_set}
        """

        total = 0
        with engine.begin() as conn:
            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i:i + chunksize]
                conn.execute(text(sql), chunk.to_dict(orient="records"))
                total += len(chunk)
        return total


class CSVLoader(BaseLoader):
    """
    Writes DataFrame to a CSV file.

    Config:
        path (str): Output file path.
        mode (str): 'write' | 'append'. Default: write.
        encoding (str): Default: utf-8.
        index (bool): Write index. Default: False.
    """

    def load(self, df: pd.DataFrame) -> LoadResult:
        start = time.monotonic()
        path = self.config["path"]
        mode = self.config.get("mode", "write")
        encoding = self.config.get("encoding", "utf-8")
        error = None

        try:
            write_mode = "a" if mode == "append" else "w"
            header = write_mode == "w"
            df.to_csv(path, mode=write_mode, index=False, encoding=encoding, header=header)
            self.logger.info("CSV load complete", path=path, records=len(df))
        except Exception as exc:
            error = str(exc)
            self.logger.error("CSV load failed", error=error)

        return LoadResult(
            destination=path,
            records_loaded=len(df) if not error else 0,
            records_failed=len(df) if error else 0,
            duration_seconds=time.monotonic() - start,
            load_mode=mode,
            error=error,
        )


class ParquetLoader(BaseLoader):
    """
    Writes DataFrame to a Parquet file.

    Config:
        path (str): Output file path.
        compression (str): 'snappy' | 'gzip' | 'brotli'. Default: snappy.
    """

    def load(self, df: pd.DataFrame) -> LoadResult:
        start = time.monotonic()
        path = self.config["path"]
        compression = self.config.get("compression", "snappy")
        error = None

        try:
            df.to_parquet(path, compression=compression, index=False)
            self.logger.info("Parquet load complete", path=path, records=len(df))
        except Exception as exc:
            error = str(exc)
            self.logger.error("Parquet load failed", error=error)

        return LoadResult(
            destination=path,
            records_loaded=len(df) if not error else 0,
            records_failed=len(df) if error else 0,
            duration_seconds=time.monotonic() - start,
            load_mode="write",
            error=error,
        )


# ── Loader Factory ────────────────────────────────────────────────────────────
_LOADER_REGISTRY: Dict[str, type] = {
    "postgresql": PostgreSQLLoader,
    "postgres": PostgreSQLLoader,
    "csv": CSVLoader,
    "parquet": ParquetLoader,
}


def get_loader(config: Dict[str, Any]) -> BaseLoader:
    """Instantiate appropriate loader from config."""
    loader_type = config.get("type", "").lower()
    if loader_type not in _LOADER_REGISTRY:
        raise ValueError(f"Unknown loader type '{loader_type}'. Available: {list(_LOADER_REGISTRY)}")
    return _LOADER_REGISTRY[loader_type](config)
