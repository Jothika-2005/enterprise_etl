"""
Enterprise ETL Platform - CSV / File Extractor

Supports CSV, TSV, JSON Lines, Parquet, and Excel files.
Config-driven with chunked reading for large files.
"""
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from backend.core.extractors.base import BaseExtractor, ExtractionResult
from backend.utils.logger import get_logger

logger = get_logger(__name__)

# Map file extension → pandas reader function name
READER_MAP = {
    ".csv": "read_csv",
    ".tsv": "read_csv",
    ".json": "read_json",
    ".jsonl": "read_json",
    ".parquet": "read_parquet",
    ".xlsx": "read_excel",
    ".xls": "read_excel",
}


class CSVExtractor(BaseExtractor):
    """
    Extracts data from flat files (CSV, TSV, JSON, Parquet, Excel).

    Config keys:
        path (str): Path to the file.
        encoding (str): File encoding. Default: utf-8.
        separator (str): CSV delimiter. Default: ','.
        chunk_size (int): Rows per chunk for large files. Default: 100000.
        columns (list): Subset of columns to load. Default: all.
        skip_rows (int): Number of header rows to skip.
        date_columns (list): Columns to parse as dates.
    """

    def get_required_config_keys(self) -> List[str]:
        return ["path"]

    def extract(
        self,
        checkpoint_value: Optional[str] = None,
    ) -> ExtractionResult:
        """
        Read file into a DataFrame.

        Args:
            checkpoint_value: Ignored for file sources (full load only).

        Returns:
            ExtractionResult with file contents.
        """
        start = time.monotonic()
        file_path = Path(self.config["path"])

        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        suffix = file_path.suffix.lower()
        reader_name = READER_MAP.get(suffix, "read_csv")

        self.logger.info(
            "Extracting file",
            path=str(file_path),
            format=suffix,
            size_bytes=file_path.stat().st_size,
        )

        read_kwargs = self._build_read_kwargs(suffix)

        chunk_size = self.config.get("chunk_size", 100_000)
        columns = self.config.get("columns")

        reader_fn = getattr(pd, reader_name)

        if reader_name == "read_csv" and chunk_size:
            # Chunked reading to handle large files
            chunks = []
            for chunk in reader_fn(file_path, chunksize=chunk_size, **read_kwargs):
                if columns:
                    chunk = chunk[columns]
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        else:
            df = reader_fn(file_path, **read_kwargs)
            if columns:
                df = df[columns]

        elapsed = time.monotonic() - start
        self.logger.info(
            "File extraction complete",
            records=len(df),
            columns=list(df.columns),
            duration_seconds=round(elapsed, 3),
        )

        return ExtractionResult(
            data=df,
            source_name=self.source_name,
            records_count=len(df),
            extraction_time=elapsed,
            metadata={
                "file_path": str(file_path),
                "file_size_bytes": file_path.stat().st_size,
                "columns": list(df.columns),
            },
        )

    def _build_read_kwargs(self, suffix: str) -> Dict[str, Any]:
        """Build kwargs for the pandas reader based on config."""
        kwargs: Dict[str, Any] = {}

        if suffix in (".csv", ".tsv"):
            kwargs["encoding"] = self.config.get("encoding", "utf-8")
            kwargs["sep"] = self.config.get("separator", "\t" if suffix == ".tsv" else ",")
            kwargs["skiprows"] = self.config.get("skip_rows", 0)
            date_cols = self.config.get("date_columns")
            if date_cols:
                kwargs["parse_dates"] = date_cols
            kwargs["on_bad_lines"] = "warn"
            kwargs["low_memory"] = False

        elif suffix in (".json", ".jsonl"):
            if suffix == ".jsonl":
                kwargs["lines"] = True

        return kwargs
