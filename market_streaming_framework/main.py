# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import time
import signal
import asyncio
import logging
import threading
import traceback
from typing import Optional

# Setup standard enterprise stdout logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("market_streaming_framework.main")

from market_streaming_framework.config.kafka_config import (
    PipelineConfig,
    KafkaBrokerConfig,
)
from market_streaming_framework.ingestion.base_producer import MarketTickPayload
from market_streaming_framework.ingestion.market_harvester import (
    MarketHarvester,
    TokenBucketRateLimiter,
    ProxyRotator,
    MockKafkaDirectProducer,
)
from market_streaming_framework.stream_processing.backpressure_queue import (
    RingBackpressureQueue,
)
from market_streaming_framework.stream_processing.feature_generator import (
    VectorizedFeatureGenerator,
)
from market_streaming_framework.storage.partitioned_sink import (
    PartitionedDataLakeSink,
)
from market_streaming_framework.storage.compaction_worker import (
    ColumnarCompactionWorker,
)
from market_streaming_framework.monitoring.drift_auditor import (
    SchemaEvolutionManager,
    DataDriftAuditor,
)


class FrameworkOrchestrator:
    """
    Root Multi-Threaded & Asynchronous Framework Orchestration Controller.
    Ties together the entire real-time financial market data lake & feature store:
      - Ingestion Engine (Async Harvester + Token Bucket + Proxy Rotator)
      - Streaming Bus & Ring Backpressure Buffer
      - Vectorized Feature Computation (NumPy/Pandas)
      - Quality Gatekeeping & Dynamic Schema Evolution (SciPy Drift Auditor)
      - Partitioned Local Staging Sink
      - Columnar Parquet Compaction Engine (PyArrow)
    """

    def __init__(self, pipeline_config: Optional[PipelineConfig] = None):
        self.config = pipeline_config or PipelineConfig()
        self.broker_config = KafkaBrokerConfig()
        self._shutdown_event = threading.Event()
        self._async_stop_event = asyncio.Event()

        logger.info("Initializing ChronosLake Financial Framework Orchestrator...")

        # 1. Thread-Safe Ring Backpressure Queue
        self.ring_queue = RingBackpressureQueue[MarketTickPayload](
            capacity=self.config.ring_queue_capacity,
            high_watermark_ratio=0.80,
            critical_watermark_ratio=0.95,
        )

        # 2. Broker Producer Bridge
        self.producer = MockKafkaDirectProducer(self.broker_config)
        # Wire producer batches directly into the backpressure ring buffer
        self.producer.on_batch_delivered_callback = self._on_kafka_batch_received

        # 3. Rate Limiter and Ingestion Harvester
        self.rate_limiter = TokenBucketRateLimiter(
            refill_rate_per_sec=self.config.rate_limit_tokens_per_sec,
            max_burst_capacity=self.config.rate_limit_burst_capacity,
        )
        self.proxy_rotator = ProxyRotator()
        self.harvester = MarketHarvester(
            symbols=self.config.symbols,
            producer=self.producer,
            rate_limiter=self.rate_limiter,
            proxy_rotator=self.proxy_rotator,
        )

        # 4. Vectorized Feature Generator
        self.feature_generator = VectorizedFeatureGenerator(rolling_window_size=100)

        # 5. Partitioned Lake Sink
        self.lake_sink = PartitionedDataLakeSink(
            base_lake_path=self.config.lake_storage_path
        )

        # 6. Columnar Compaction Daemon
        self.compaction_worker = ColumnarCompactionWorker(
            base_lake_path=self.config.lake_storage_path,
            compaction_interval_seconds=self.config.compaction_interval_seconds,
            min_files_to_compact=2,
            compression_codec="snappy",
        )

        # 7. Schema Evolution & Drift Auditor
        self.schema_manager = SchemaEvolutionManager(
            registry_path=self.config.schema_registry_path
        )
        self.drift_auditor = DataDriftAuditor(
            schema_manager=self.schema_manager,
            drift_threshold=self.config.drift_detection_threshold,
        )

        # Worker thread handles
        self._consumer_thread: Optional[threading.Thread] = None
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._harvester_task: Optional[asyncio.Task] = None

    def _on_kafka_batch_received(self, batch: list[MarketTickPayload]) -> None:
        """Callback invoked by the producer to push records into the consumer ring queue."""
        try:
            enqueued = self.ring_queue.put_batch(batch, timeout_seconds=1.0)
            if enqueued < len(batch):
                logger.warning(
                    "Ring queue backpressure caused drop of %d records.",
                    len(batch) - enqueued,
                )
        except Exception as e:
            logger.error("Error transferring batch into ring queue: %s", str(e))

    def _consumer_processing_worker(self) -> None:
        """
        Dedicated processing thread consuming micro-batches from the ring queue,
        running vectorized matrix computations, auditing distribution drift, and writing to sink.
        """
        logger.info("Consumer Feature Processing Worker thread initiated.")
        batch_counter = 0

        while not self._shutdown_event.is_set():
            try:
                # Drain micro-batch out of the ring backpressure buffer
                raw_batch = self.ring_queue.get_batch(
                    max_items=self.config.micro_batch_size,
                    timeout_seconds=0.1,
                )

                if not raw_batch:
                    continue

                batch_counter += 1

                # 1. Vectorized Matrix Arithmetic (NumPy / Pandas)
                feature_matrix = self.feature_generator.process_micro_batch(raw_batch)
                if feature_matrix is None or feature_matrix.dataframe.empty:
                    continue

                # 2. Statistical Distribution Drift Audit & Schema Evolution (SciPy)
                audit_report = self.drift_auditor.audit_feature_matrix(
                    feature_matrix.dataframe
                )

                if audit_report.has_drift:
                    logger.warning(
                        "[ALERT] Micro-Batch #%d showed statistical metric drift! Evolved schema: %s",
                        batch_counter,
                        audit_report.schema_mutations_detected,
                    )

                # 3. Micro-Batch Storage Persistence
                written = self.lake_sink.write_feature_matrix(feature_matrix)

                if batch_counter % 20 == 0:
                    q_metrics = self.ring_queue.get_metrics()
                    logger.info(
                        "PIPELINE METRICS: Batch #%d | Enqueued: %d | Dequeued: %d | Queue Depth: %d (%.1f%%) | Lake Files: %d",
                        batch_counter,
                        q_metrics.total_enqueued,
                        q_metrics.total_dequeued,
                        q_metrics.current_size,
                        q_metrics.buffer_utilization_pct,
                        self.lake_sink.manifest.total_files_written,
                    )

            except Exception as e:
                logger.error(
                    "Unhandled exception in consumer worker thread: %s\n%s",
                    str(e),
                    traceback.format_exc(),
                )
                time.sleep(0.1)

        logger.info("Consumer Feature Processing Worker thread safely drained.")

    async def _start_async_pipeline(self) -> None:
        """Initializes async producer sockets and runs market harvester."""
        try:
            await self.producer.connect()
            self._harvester_task = asyncio.create_task(
                self.harvester.start_harvesting(stop_event=self._async_stop_event)
            )
            await self._harvester_task
        except asyncio.CancelledError:
            logger.info("Async harvester event loop cancelled.")
        except Exception as e:
            logger.error("Async pipeline execution failure: %s\n%s", str(e), traceback.format_exc())

    def run(self, run_duration_seconds: Optional[float] = None) -> None:
        """
        Launches all processes and background threads.
        Blocks until timeout or interrupt signal.
        """
        logger.info(
            "====================================================================\n"
            "   CHRONOSLAKE: EVENT-DRIVEN MARKET DATA LAKE & FEATURE STORE       \n"
            "   Author: Rayaan tasnim, 13 aged prodigy                           \n"
            "   Symbols: %s                                                      \n"
            "   Data Lake Base: %s                                               \n"
            "====================================================================",
            ", ".join(self.config.symbols),
            self.config.lake_storage_path,
        )

        # Register signal handlers for clean SIGINT / SIGTERM shutdown
        def _sig_handler(sig, frame):
            logger.info("Received interrupt signal (%d). Initiating graceful shutdown...", sig)
            self.stop()

        try:
            signal.signal(signal.SIGINT, _sig_handler)
            signal.signal(signal.SIGTERM, _sig_handler)
        except Exception:
            pass  # Handle platforms/threads where signal registration differs

        # 1. Start Columnar Compaction Background Daemon
        self.compaction_worker.start()

        # 2. Start Downstream Processing Consumer Thread
        self._consumer_thread = threading.Thread(
            target=self._consumer_processing_worker,
            name="StreamConsumerWorker",
            daemon=True,
        )
        self._consumer_thread.start()

        # 3. Start Async Ingestion in Dedicated Event Loop Thread
        def _async_runner():
            self._async_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._async_loop)
            self._async_loop.run_until_complete(self._start_async_pipeline())

        ingestion_thread = threading.Thread(
            target=_async_runner,
            name="AsyncIngestionWorker",
            daemon=True,
        )
        ingestion_thread.start()

        start_time = time.time()
        try:
            while not self._shutdown_event.is_set():
                time.sleep(0.5)
                if run_duration_seconds and (time.time() - start_time) >= run_duration_seconds:
                    logger.info("Target run duration reached (%.1fs). Stopping...", run_duration_seconds)
                    break
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt detected.")
        finally:
            self.stop()
            if self._consumer_thread and self._consumer_thread.is_alive():
                self._consumer_thread.join(timeout=3.0)
            if ingestion_thread and ingestion_thread.is_alive():
                ingestion_thread.join(timeout=3.0)
            logger.info("ChronosLake Framework Orchestrator successfully halted.")

    def stop(self) -> None:
        """Gracefully halts all pipeline tiers without data loss."""
        if self._shutdown_event.is_set():
            return
        logger.info("Signaling framework shutdown...")
        self._shutdown_event.set()
        self._async_stop_event.set()
        self.harvester.stop()
        self.compaction_worker.stop()


def main():
    """CLI Entrypoint."""
    try:
        config = PipelineConfig()
        orchestrator = FrameworkOrchestrator(config)
        # Run pipeline demonstration
        run_duration = float(os.environ.get("RUN_DURATION_SEC", "30.0"))
        orchestrator.run(run_duration_seconds=run_duration)
    except Exception as e:
        logger.error("Fatal startup error in main: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
