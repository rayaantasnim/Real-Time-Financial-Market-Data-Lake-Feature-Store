# Author: Rayaan tasnim, 13 aged prodigy

import time
import threading
import logging
import traceback
from dataclasses import dataclass
from typing import List, TypeVar, Generic, Optional

logger = logging.getLogger("market_streaming_framework.stream_processing.backpressure_queue")

T = TypeVar("T")


@dataclass
class QueueWatermarkMetrics:
    """Real-time observability into ring buffer depth and memory saturation."""
    capacity: int
    current_size: int
    high_watermark_ratio: float
    total_enqueued: int
    total_dequeued: int
    backpressure_pause_events: int
    buffer_utilization_pct: float


class RingBackpressureQueue(Generic[T]):
    """
    High-throughput, thread-safe, bounded circular ring queue with active backpressure control.
    Protects process memory bounds by dynamically signaling ingestion throttles when
    consumer processing delays occur.
    """

    def __init__(
        self,
        capacity: int = 50000,
        high_watermark_ratio: float = 0.80,
        critical_watermark_ratio: float = 0.95,
    ):
        if capacity <= 0:
            raise ValueError("Ring queue capacity must be strictly positive.")

        self.capacity: int = capacity
        self.high_watermark_ratio: float = high_watermark_ratio
        self.critical_watermark_ratio: float = critical_watermark_ratio
        self.high_watermark_limit: int = int(capacity * high_watermark_ratio)
        self.critical_limit: int = int(capacity * critical_watermark_ratio)

        # Preallocated ring array for zero memory fragmentation
        self._buffer: List[Optional[T]] = [None] * capacity
        self._head: int = 0
        self._tail: int = 0
        self._size: int = 0

        # Synchronization primitives
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

        # Operational telemetry
        self._total_enqueued: int = 0
        self._total_dequeued: int = 0
        self._backpressure_pause_events: int = 0
        self._is_throttled: bool = False

        logger.info(
            "RingBackpressureQueue initialized with capacity: %d (High WM: %d, Crit WM: %d)",
            self.capacity,
            self.high_watermark_limit,
            self.critical_limit,
        )

    def put(self, item: T, timeout_seconds: Optional[float] = 2.0) -> bool:
        """
        Thread-safe enqueue operation with adaptive backpressure gating.
        Blocks producer if buffer hits critical bounds until consumer drains items.
        """
        try:
            with self._not_full:
                start_wait = time.monotonic()
                while self._size >= self.capacity:
                    self._backpressure_pause_events += 1
                    if not self._is_throttled:
                        self._is_throttled = True
                        logger.warning(
                            "CRITICAL BACKPRESSURE: Ring buffer 100%% saturated (%d items). Halting ingestion!",
                            self._size,
                        )

                    remaining = None
                    if timeout_seconds is not None:
                        elapsed = time.monotonic() - start_wait
                        remaining = timeout_seconds - elapsed
                        if remaining <= 0:
                            logger.error(
                                "Backpressure put timeout exceeded (%.2fs). Dropping tick to protect RAM bounds.",
                                timeout_seconds,
                            )
                            return False

                    self._not_full.wait(timeout=remaining)

                # Insert into circular buffer
                self._buffer[self._tail] = item
                self._tail = (self._tail + 1) % self.capacity
                self._size += 1
                self._total_enqueued += 1

                # Evaluate watermark warnings
                if self._size >= self.high_watermark_limit and not self._is_throttled:
                    self._is_throttled = True
                    logger.warning(
                        "WARNING: Queue depth crossed high watermark [%d/%d (%.1f%%)]. Downstream lagging.",
                        self._size,
                        self.capacity,
                        (self._size / self.capacity) * 100.0,
                    )

                self._not_empty.notify()
                return True
        except Exception as e:
            logger.error(
                "Unexpected failure in ring buffer put: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            return False

    def put_batch(self, items: List[T], timeout_seconds: Optional[float] = 2.0) -> int:
        """Vectorized atomic enqueue for micro-batches."""
        enqueued_count = 0
        for item in items:
            if self.put(item, timeout_seconds=timeout_seconds):
                enqueued_count += 1
            else:
                break
        return enqueued_count

    def get_batch(self, max_items: int, timeout_seconds: float = 0.2) -> List[T]:
        """
        Drains a micro-batch of up to max_items.
        Signals waiting producers when memory depth recedes under safe thresholds.
        """
        batch: List[T] = []
        try:
            with self._not_empty:
                start_wait = time.monotonic()
                while self._size == 0:
                    elapsed = time.monotonic() - start_wait
                    remaining = timeout_seconds - elapsed
                    if remaining <= 0:
                        return batch
                    self._not_empty.wait(timeout=remaining)

                drain_count = min(max_items, self._size)
                for _ in range(drain_count):
                    item = self._buffer[self._head]
                    self._buffer[self._head] = None  # Free object reference for GC
                    self._head = (self._head + 1) % self.capacity
                    self._size -= 1
                    self._total_dequeued += 1
                    if item is not None:
                        batch.append(item)

                # Reset backpressure throttling if below watermark
                if self._is_throttled and self._size < self.high_watermark_limit:
                    self._is_throttled = False
                    logger.info(
                        "Backpressure relieved: Ring queue depth normalized to %d items (%.1f%%).",
                        self._size,
                        (self._size / self.capacity) * 100.0,
                    )

                self._not_full.notify_all()
                return batch
        except Exception as e:
            logger.error(
                "Unexpected failure in ring buffer get_batch: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            return batch

    def get_metrics(self) -> QueueWatermarkMetrics:
        """Snapshot current depth and telemetry in a non-blocking inspection."""
        with self._lock:
            utilization = (self._size / self.capacity) * 100.0 if self.capacity > 0 else 0.0
            return QueueWatermarkMetrics(
                capacity=self.capacity,
                current_size=self._size,
                high_watermark_ratio=self.high_watermark_ratio,
                total_enqueued=self._total_enqueued,
                total_dequeued=self._total_dequeued,
                backpressure_pause_events=self._backpressure_pause_events,
                buffer_utilization_pct=round(utilization, 2),
            )

    def size(self) -> int:
        with self._lock:
            return self._size

    def is_empty(self) -> bool:
        with self._lock:
            return self._size == 0
