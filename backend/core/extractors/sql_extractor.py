"""
Enterprise ETL Platform - SQL Database Extractor

Extracts data from PostgreSQL, MySQL, SQLite, and any SQLAlchemy-
compatible database. Supports full and incremental (watermark) loads.
"""
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from backend.core.extractors.base import BaseExtractor, ExtractionResult
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class SQLExtractor(BaseExtractor):
    """
    Extracts data from SQL databases via SQLAlchemy.

    Config keys:
        connection_string (str): SQLAlchemy connection URL.
        query (str): SQL query. Use :checkpoint placeholder for incremental.
        table (str): Table name (alternative to query — extracts full table).
        schema (str): Database schema. Default: public.
        chunk_size (int): Rows per chunk. Default: 50000.
        incremental (bool): Enable incremental load. Default: False.
        checkpoint_column (str): Column used as watermark.
        params (dict): Additional query parameters.
    """

    def get_required_config_keys(self) -> List[str]:
        return ["connection_string"]

    def connect(self) -> None:
        """Create SQLAlchemy engine and test connection."""
        conn_str = self.config["connection_string"]
        self._engine = create_engine(
            conn_str,
            pool_pre_ping=True,
            pool_size=5,
        )
        # Test connection
        with self._engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self._connected = True
        self.logger.info("SQL connection established", dialect=self._engine.dialect.name)

    def disconnect(self) -> None:
        """Dispose SQLAlchemy engine."""
        if hasattr(self, "_engine"):
            self._engine.dispose()
        self._connected = False

    def extract(
        self,
        checkpoint_value: Optional[str] = None,
    ) -> ExtractionResult:
        """
        Execute SQL query and return results as DataFrame.

        Args:
            checkpoint_value: Watermark value for incremental loads.

        Returns:
            ExtractionResult with query results.
        """
        if not self._connected:
            self.connect()

        start = time.monotonic()
        query = self._build_query(checkpoint_value)
        params = self._build_params(checkpoint_value)
        chunk_size = self.config.get("chunk_size", 50_000)

        self.logger.info(
            "Executing SQL extraction",
            query_preview=query[:200],
            incremental=self.config.get("incremental", False),
            checkpoint=checkpoint_value,
        )

        chunks = []
        latest_checkpoint = checkpoint_value

        with self._engine.connect() as conn:
            for chunk_df in pd.read_sql_query(
                text(query),
                conn,
                params=params,
                chunksize=chunk_size,
            ):
                chunks.append(chunk_df)

                # Track latest watermark for incremental
                checkpoint_col = self.config.get("checkpoint_column")
                if checkpoint_col and checkpoint_col in chunk_df.columns:
                    max_val = str(chunk_df[checkpoint_col].max())
                    if latest_checkpoint is None or max_val > latest_checkpoint:
                        latest_checkpoint = max_val

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        elapsed = time.monotonic() - start

        self.logger.info(
            "SQL extraction complete",
            records=len(df),
            duration_seconds=round(elapsed, 3),
            new_checkpoint=latest_checkpoint,
        )

        return ExtractionResult(
            data=df,
            source_name=self.source_name,
            records_count=len(df),
            extraction_time=elapsed,
            checkpoint_value=latest_checkpoint,
            metadata={
                "query": query,
                "dialect": self._engine.dialect.name,
                "chunk_size": chunk_size,
            },
        )

    def _build_query(self, checkpoint_value: Optional[str]) -> str:
        """Build SQL query, using table name or raw query from config."""
        if "query" in self.config:
            return self.config["query"]

        table = self.config.get("table")
        schema = self.config.get("schema", "public")
        if not table:
            raise ValueError("Either 'query' or 'table' must be specified in config.")

        full_table = f"{schema}.{table}" if schema else table
        query = f"SELECT * FROM {full_table}"

        # Add incremental filter
        if self.config.get("incremental") and checkpoint_value:
            checkpoint_col = self.config.get("checkpoint_column")
            if checkpoint_col:
                query += f" WHERE {checkpoint_col} > :checkpoint"

        return query

    def _build_params(self, checkpoint_value: Optional[str]) -> Dict[str, Any]:
        """Build query parameters."""
        params = dict(self.config.get("params", {}))
        if checkpoint_value and self.config.get("incremental"):
            params["checkpoint"] = checkpoint_value
        return params
