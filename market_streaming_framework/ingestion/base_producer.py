# Author: Rayaan tasnim, 13 aged prodigy

import abc
import time
import logging
import traceback
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Callable

logger = logging.getLogger("market_streaming_framework.ingestion.base_producer")


@dataclass
class MarketTickPayload:
    """Canonical memory model for high-frequency financial market tick."""
    sequence_id: int
    timestamp_ns: int
    iso_timestamp: str
    symbol: str
    exchange: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    last_trade_price: float
    last_trade_size: float
    schema_version: str = "1.0.0"
    extra_fields: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert payload to a flat dictionary honoring dynamic schema extensions."""
        base = asdict(self)
        extras = base.pop("extra_fields", {})
        base.update(extras)
        return base


@dataclass
class ProducerMetrics:
    """Thread-safe performance telemetry counters for the producer layer."""
    total_produced: int = 0
    total_bytes_sent: int = 0
    total_failures: int = 0
    rate_limited_drops: int = 0
    last_produced_timestamp: float = 0.0
    start_time: float = field(default_factory=time.time)

    def get_throughput_msg_per_sec(self) -> float:
        elapsed = time.time() - self.start_time
        return self.total_produced / elapsed if elapsed > 0 else 0.0


class BaseProducer(abc.ABC):
    """
    Abstract Base Class establishing operational contracts for thread-safe
    and asynchronous financial stream producers.
    """

    def __init__(self, client_id: str, topic: str):
        self.client_id = client_id
        self.topic = topic
        self.is_connected = False
        self.metrics = ProducerMetrics()
        logger.info("Initializing %s for topic: %s", self.__class__.__name__, self.topic)

    @abc.abstractmethod
    async def connect(self) -> None:
        """Establish asynchronous connection pool or network sockets."""
        pass

    @abc.abstractmethod
    async def produce(
        self,
        key: str,
        payload: MarketTickPayload,
        on_delivery: Optional[Callable[[Optional[Exception], Any], None]] = None,
    ) -> bool:
        """
        Asynchronously publish a validated market tick event to the distributed log.
        Must guarantee thread safety and return status boolean.
        """
        pass

    @abc.abstractmethod
    async def produce_batch(
        self,
        batch: List[MarketTickPayload],
    ) -> int:
        """
        High-throughput vectorized/micro-batched publish operation.
        Returns the number of successfully enqueued records.
        """
        pass

    @abc.abstractmethod
    async def flush(self, timeout_seconds: float = 10.0) -> None:
        """Flush all pending in-flight buffers to target brokers."""
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        """Gracefully drain buffers and terminate client network sessions."""
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """Export snapshot of producer telemetry."""
        try:
            return {
                "client_id": self.client_id,
                "topic": self.topic,
                "is_connected": self.is_connected,
                "total_produced": self.metrics.total_produced,
                "total_bytes_sent": self.metrics.total_bytes_sent,
                "total_failures": self.metrics.total_failures,
                "rate_limited_drops": self.metrics.rate_limited_drops,
                "throughput_msg_per_sec": round(self.metrics.get_throughput_msg_per_sec(), 2),
                "uptime_seconds": round(time.time() - self.metrics.start_time, 2),
            }
        except Exception as e:
            logger.error("Failed to collect producer metrics: %s\n%s", str(e), traceback.format_exc())
            return {"error": str(e)}
