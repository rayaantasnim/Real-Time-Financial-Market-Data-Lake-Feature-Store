# Author: Rayaan tasnim, 13 aged prodigy

import os
import glob
import json
import time
import threading
import logging
import traceback
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

logger = logging.getLogger("market_streaming_framework.storage.compaction_worker")


@dataclass
class CompactionMetrics:
    """Metrics tracking out-of-core file compaction performance."""
    total_compactions_run: int = 0
    total_raw_files_compacted: int = 0
    total_records_processed: int = 0
    total_bytes_uncompressed: int = 0
    total_bytes_compacted_parquet: int = 0
    compression_ratio: float = 0.0
    last_compaction_duration_seconds: float = 0.0


class ColumnarCompactionWorker:
    """
    Background maintenance orchestrator.
    Periodically traverses raw staging partitions in the time-partitioned data lake,
    reads raw micro-batch arrays, performs deduplication of sequence identities,
    and produces highly compressed, query-optimized columnar Parquet files using PyArrow.
    """

    def __init__(
        self,
        base_lake_path: str = "./market_data_lake",
        compaction_interval_seconds: int = 15,
        min_files_to_compact: int = 3,
        compression_codec: str = "snappy",
    ):
        self.base_path = os.path.abspath(base_lake_path)
        self.compaction_interval = compaction_interval_seconds
        self.min_files_to_compact = min_files_to_compact
        self.compression_codec = compression_codec
        self.metrics = CompactionMetrics()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        if not HAS_PYARROW:
            logger.warning(
                "PyArrow library not installed in runtime environment. "
                "Columnar Parquet conversion will fallback to optimized compressed parquet or json."
            )

    def start(self) -> None:
        """Launch background worker thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, name="ColumnarCompactionWorker", daemon=True
        )
        self._thread.start()
        logger.info(
            "ColumnarCompactionWorker background daemon started (Interval: %ds, Min files: %d)",
            self.compaction_interval,
            self.min_files_to_compact,
        )

    def stop(self) -> None:
        """Signal termination and join worker thread."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            logger.info("ColumnarCompactionWorker terminated gracefully.")

    def _run_loop(self) -> None:
        while self._running:
            try:
                self.compact_all_partitions()
            except Exception as e:
                logger.error(
                    "Error during background compaction cycle: %s\n%s",
                    str(e),
                    traceback.format_exc(),
                )
            # Sleep in small increments to allow responsive shutdown
            for _ in range(self.compaction_interval * 2):
                if not self._running:
                    break
                time.sleep(0.5)

    def compact_all_partitions(self) -> int:
        """
        Traverses directory tree looking for leaf partition directories containing
        staged JSON micro-batches.
        """
        if not os.path.exists(self.base_path):
            return 0

        compactions_performed = 0
        try:
            # Match leaf directories: .../year=*/month=*/day=*/symbol=*
            search_pattern = os.path.join(
                self.base_path, "year=*", "month=*", "day=*", "symbol=*"
            )
            partition_dirs = glob.glob(search_pattern)

            for partition_dir in partition_dirs:
                if not self._running:
                    break
                result = self.compact_partition_directory(partition_dir)
                if result:
                    compactions_performed += 1

            return compactions_performed
        except Exception as e:
            logger.error(
                "Failed traversing partitions for compaction: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            return compactions_performed

    def compact_partition_directory(self, partition_dir: str) -> bool:
        """
        Gathers micro-batches in a partition, reads into chunked frames,
        deduplicates records by sequence_id, and outputs a compacted Parquet dataset.
        """
        try:
            json_files = glob.glob(os.path.join(partition_dir, "microbatch_*.json"))
            if len(json_files) < self.min_files_to_compact:
                return False

            start_t = time.time()
            all_records: List[Dict[str, Any]] = []
            raw_bytes = 0

            # 1. Chunked Read & Aggregate
            for f_path in json_files:
                try:
                    f_size = os.path.getsize(f_path)
                    raw_bytes += f_size
                    with open(f_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            all_records.extend(data)
                except Exception as read_err:
                    logger.warning("Skipping unreadable microbatch file %s: %s", f_path, str(read_err))

            if not all_records:
                return False

            # 2. DataFrame Transformation & Deduplication
            df = pd.DataFrame.from_records(all_records)
            initial_count = len(df)

            # Deduplicate by sequence_id and symbol
            if "sequence_id" in df.columns:
                df = df.drop_duplicates(subset=["sequence_id", "symbol"], keep="last")
            df = df.sort_values(by="timestamp_ns")

            records_count = len(df)
            timestamp_now = int(time.time() * 1000)
            parquet_filename = f"compacted_dataset_{timestamp_now}.parquet"
            target_parquet_path = os.path.join(partition_dir, parquet_filename)

            # 3. Write via PyArrow columnar engine if available
            compacted_bytes = 0
            if HAS_PYARROW:
                # Convert DataFrame to PyArrow Table
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(
                    table,
                    target_parquet_path,
                    compression=self.compression_codec,
                    use_dictionary=["symbol", "exchange"],
                )
                compacted_bytes = os.path.getsize(target_parquet_path)
            else:
                # Fallback to Pandas Parquet or compressed format
                df.to_parquet(target_parquet_path, index=False, compression="snappy")
                compacted_bytes = os.path.getsize(target_parquet_path)

            # 4. Safely purge raw staging JSON files now that Parquet is written
            for f_path in json_files:
                try:
                    os.remove(f_path)
                except OSError as rm_err:
                    logger.debug("Could not remove intermediate file %s: %s", f_path, str(rm_err))

            # 5. Record telemetry
            duration = time.time() - start_t
            self.metrics.total_compactions_run += 1
            self.metrics.total_raw_files_compacted += len(json_files)
            self.metrics.total_records_processed += records_count
            self.metrics.total_bytes_uncompressed += raw_bytes
            self.metrics.total_bytes_compacted_parquet += compacted_bytes
            if compacted_bytes > 0:
                self.metrics.compression_ratio = round(raw_bytes / compacted_bytes, 2)
            self.metrics.last_compaction_duration_seconds = round(duration, 3)

            logger.info(
                "COMPACTION COMPLETED for %s: Merged %d raw files (%d -> %d records, deduplicated %d) into %s. "
                "Raw: %.2f KB -> Parquet: %.2f KB (Ratio: %.1fx) in %.3fs",
                os.path.basename(partition_dir),
                len(json_files),
                initial_count,
                records_count,
                initial_count - records_count,
                parquet_filename,
                raw_bytes / 1024.0,
                compacted_bytes / 1024.0,
                self.metrics.compression_ratio,
                duration,
            )
            return True

        except Exception as e:
            logger.error(
                "Partition compaction failure in %s: %s\n%s",
                partition_dir,
                str(e),
                traceback.format_exc(),
            )
            return False
