# Author: Rayaan tasnim, 13 aged prodigy

import os
import json
import time
import logging
import datetime
import traceback
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import pandas as pd
from market_streaming_framework.stream_processing.feature_generator import CalculatedFeatureMatrix

logger = logging.getLogger("market_streaming_framework.storage.partitioned_sink")


@dataclass
class SinkManifest:
    """Tracks state and lineage of staging and compacted partition blocks."""
    total_files_written: int = 0
    total_bytes_written: int = 0
    active_partitions: List[str] = field(default_factory=list)
    last_write_timestamp: float = 0.0


class PartitionedDataLakeSink:
    """
    Coordinates high-throughput micro-batch persistence into an append-friendly,
    time-partitioned local data lake topology.
    Partition format: base_path/year=YYYY/month=MM/day=DD/symbol=SYM/part_{batch_id}_{timestamp}.json
    Employs atomic file replacement to guarantee read-isolation during active ingestion.
    """

    def __init__(self, base_lake_path: str = "./market_data_lake"):
        self.base_path = os.path.abspath(base_lake_path)
        self.manifest = SinkManifest()
        self._ensure_lake_directory()
        logger.info("PartitionedDataLakeSink initialized at target path: %s", self.base_path)

    def _ensure_lake_directory(self) -> None:
        try:
            os.makedirs(self.base_path, exist_ok=True)
            staging_path = os.path.join(self.base_path, "_staging")
            os.makedirs(staging_path, exist_ok=True)
        except Exception as e:
            logger.error("Failed to initialize lake base directories: %s\n%s", str(e), traceback.format_exc())
            raise

    def write_feature_matrix(self, matrix: CalculatedFeatureMatrix) -> List[str]:
        """
        Splits feature matrix across time and ticker symbol partitions, writing
        staging JSON/NDJSON micro-batches ready for downstream compaction.
        """
        written_files: List[str] = []
        if matrix.dataframe.empty:
            return written_files

        try:
            df = matrix.dataframe
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            year_str = f"year={now_utc.year:04d}"
            month_str = f"month={now_utc.month:02d}"
            day_str = f"day={now_utc.day:02d}"

            # Group by ticker symbol to isolate partition directories
            for symbol, group_df in df.groupby("symbol"):
                sym_str = f"symbol={symbol}"
                partition_dir = os.path.join(
                    self.base_path, year_str, month_str, day_str, sym_str
                )
                os.makedirs(partition_dir, exist_ok=True)

                if partition_dir not in self.manifest.active_partitions:
                    self.manifest.active_partitions.append(partition_dir)

                timestamp_ms = int(time.time() * 1000)
                final_filename = f"microbatch_{matrix.batch_id}_{timestamp_ms}.json"
                final_path = os.path.join(partition_dir, final_filename)
                temp_path = os.path.join(
                    self.base_path, "_staging", f"tmp_{symbol}_{matrix.batch_id}_{timestamp_ms}.json"
                )

                # Convert dataframe slice to records
                records: List[Dict[str, Any]] = group_df.to_dict(orient="records")

                # Atomic write: Write to staging temp file first, then atomic rename
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(records, f, indent=None)

                # Atomic replace
                os.replace(temp_path, final_path)
                file_size = os.path.getsize(final_path)

                self.manifest.total_files_written += 1
                self.manifest.total_bytes_written += file_size
                self.manifest.last_write_timestamp = time.time()
                written_files.append(final_path)

            logger.debug(
                "Successfully persisted batch #%d across %d partition files.",
                matrix.batch_id,
                len(written_files),
            )
            return written_files

        except Exception as e:
            logger.error(
                "Fatal I/O failure during partitioned sink micro-batch write: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            return written_files

    def get_manifest_summary(self) -> Dict[str, Any]:
        """Provides status report of physical storage layer."""
        return {
            "base_lake_path": self.base_path,
            "total_files_written": self.manifest.total_files_written,
            "total_bytes_written": self.manifest.total_bytes_written,
            "active_partitions_count": len(self.manifest.active_partitions),
            "last_write_timestamp": self.manifest.last_write_timestamp,
        }
