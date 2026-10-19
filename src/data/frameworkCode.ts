export interface FrameworkFile {
  path: string;
  name: string;
  category: "config" | "ingestion" | "stream_processing" | "storage" | "monitoring" | "root";
  description: string;
  language: "python" | "json" | "text";
  code: string;
}

export const FRAMEWORK_FILES: FrameworkFile[] = [
  {
    path: "market_streaming_framework/config/kafka_config.py",
    name: "kafka_config.py",
    category: "config",
    description: "Connection pools, consumer group topologies, topic configurations, serialization, and socket timeouts.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

import os
import logging
import traceback
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

# Configure standard root logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("market_streaming_framework.config.kafka_config")


@dataclass(frozen=True)
class KafkaSocketConfig:
    """Socket level timeouts and low-level connection pool rules."""
    socket_timeout_ms: int = int(os.environ.get("KAFKA_SOCKET_TIMEOUT_MS", "30000"))
    request_timeout_ms: int = int(os.environ.get("KAFKA_REQUEST_TIMEOUT_MS", "40000"))
    retry_backoff_ms: int = int(os.environ.get("KAFKA_RETRY_BACKOFF_MS", "500"))
    max_in_flight_requests_per_connection: int = int(
        os.environ.get("KAFKA_MAX_IN_FLIGHT", "5")
    )
    receive_buffer_bytes: int = int(os.environ.get("KAFKA_RECEIVE_BUFFER_BYTES", "65536"))
    send_buffer_bytes: int = int(os.environ.get("KAFKA_SEND_BUFFER_BYTES", "131072"))


@dataclass(frozen=True)
class KafkaTopicConfig:
    """Topic topology definitions for market ingestion and calculated features."""
    raw_ticks_topic: str = os.environ.get("KAFKA_TOPIC_RAW_TICKS", "financial.market.ticks.raw")
    features_topic: str = os.environ.get("KAFKA_TOPIC_FEATURES", "financial.market.features.vector")
    dead_letter_topic: str = os.environ.get("KAFKA_TOPIC_DLQ", "financial.market.ticks.dlq")
    partitions_count: int = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "8"))
    replication_factor: int = int(os.environ.get("KAFKA_TOPIC_REPLICATION", "3"))
    retention_ms: int = int(os.environ.get("KAFKA_TOPIC_RETENTION_MS", "604800000"))  # 7 days


@dataclass(frozen=True)
class KafkaSerializationParams:
    """Serialization, compression codecs, and payload schemas."""
    compression_type: str = os.environ.get("KAFKA_COMPRESSION_TYPE", "zstd")  # snappy, lz4, zstd, gzip
    key_serializer: str = os.environ.get("KAFKA_KEY_SERIALIZER", "org.apache.kafka.common.serialization.StringSerializer")
    value_serializer: str = os.environ.get("KAFKA_VALUE_SERIALIZER", "org.apache.kafka.common.serialization.ByteArraySerializer")
    batch_size: int = int(os.environ.get("KAFKA_PRODUCER_BATCH_SIZE", "65536"))  # 64 KB microbatch
    linger_ms: int = int(os.environ.get("KAFKA_PRODUCER_LINGER_MS", "10"))
    acks: str = os.environ.get("KAFKA_PRODUCER_ACKS", "all")


@dataclass(frozen=True)
class KafkaConsumerTopology:
    """Consumer group topology, balancing strategy, and commit policies."""
    consumer_group_id: str = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "chronoslake-feature-engine-v1")
    auto_offset_reset: str = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest")
    enable_auto_commit: bool = os.environ.get("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
    max_poll_records: int = int(os.environ.get("KAFKA_MAX_POLL_RECORDS", "5000"))
    max_poll_interval_ms: int = int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))
    session_timeout_ms: int = int(os.environ.get("KAFKA_SESSION_TIMEOUT_MS", "45000"))
    heartbeat_interval_ms: int = int(os.environ.get("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))


@dataclass
class KafkaBrokerConfig:
    """
    Kafka Broker Connection Pool and Security Topology.
    Uses environment variables for all critical endpoints without hardcoded secrets.
    """
    bootstrap_servers: List[str] = field(
        default_factory=lambda: os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
        ).split(",")
    )
    security_protocol: str = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism: Optional[str] = os.environ.get("KAFKA_SASL_MECHANISM", None)
    sasl_username: Optional[str] = os.environ.get("KAFKA_SASL_USERNAME", None)
    sasl_password: Optional[str] = os.environ.get("KAFKA_SASL_PASSWORD", None)
    ssl_ca_location: Optional[str] = os.environ.get("KAFKA_SSL_CA_LOCATION", None)
    client_id: str = os.environ.get("KAFKA_CLIENT_ID", "chronoslake-core-producer")

    socket_config: KafkaSocketConfig = field(default_factory=KafkaSocketConfig)
    topic_config: KafkaTopicConfig = field(default_factory=KafkaTopicConfig)
    serialization: KafkaSerializationParams = field(default_factory=KafkaSerializationParams)
    consumer_topology: KafkaConsumerTopology = field(default_factory=KafkaConsumerTopology)

    def to_producer_dict(self) -> Dict[str, Any]:
        """Produce safe configuration dictionary for Kafka Producer clients."""
        try:
            cfg: Dict[str, Any] = {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
                "client.id": self.client_id,
                "compression.type": self.serialization.compression_type,
                "batch.size": self.serialization.batch_size,
                "linger.ms": self.serialization.linger_ms,
                "acks": self.serialization.acks,
                "request.timeout.ms": self.socket_config.request_timeout_ms,
                "retry.backoff.ms": self.socket_config.retry_backoff_ms,
                "max.in.flight.requests.per.connection": self.socket_config.max_in_flight_requests_per_connection,
                "receive.message.max.bytes": self.socket_config.receive_buffer_bytes,
            }
            if self.security_protocol != "PLAINTEXT":
                cfg["security.protocol"] = self.security_protocol
                if self.sasl_mechanism:
                    cfg["sasl.mechanism"] = self.sasl_mechanism
                if self.sasl_username and self.sasl_password:
                    cfg["sasl.username"] = self.sasl_username
                    cfg["sasl.password"] = self.sasl_password
                if self.ssl_ca_location:
                    cfg["ssl.ca.location"] = self.ssl_ca_location
            return cfg
        except Exception as e:
            logger.error(
                "Failed to construct Kafka producer configuration dictionary: %s\\n%s",
                str(e),
                traceback.format_exc(),
            )
            raise


@dataclass
class PipelineConfig:
    """Master application pipeline configuration parameters."""
    symbols: List[str] = field(
        default_factory=lambda: os.environ.get(
            "MARKET_SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,SPY,QQQ,NVDA"
        ).split(",")
    )
    lake_storage_path: str = os.environ.get("DATA_LAKE_PATH", "./market_data_lake")
    ring_queue_capacity: int = int(os.environ.get("RING_QUEUE_CAPACITY", "50000"))
    micro_batch_size: int = int(os.environ.get("MICRO_BATCH_SIZE", "500"))
    micro_batch_timeout_ms: int = int(os.environ.get("MICRO_BATCH_TIMEOUT_MS", "250"))
    compaction_interval_seconds: int = int(os.environ.get("COMPACTION_INTERVAL_SEC", "15"))
    drift_detection_threshold: float = float(os.environ.get("DRIFT_THRESHOLD", "0.08"))
    rate_limit_tokens_per_sec: float = float(os.environ.get("RATE_LIMIT_TPS", "2500.0"))
    rate_limit_burst_capacity: float = float(os.environ.get("RATE_LIMIT_BURST", "5000.0"))
    schema_registry_path: str = os.environ.get(
        "SCHEMA_REGISTRY_PATH",
        os.path.join(os.path.dirname(__file__), "schema_registry.json"),
    )
`,
  },
  {
    path: "market_streaming_framework/config/schema_registry.json",
    name: "schema_registry.json",
    category: "config",
    description: "Structural JSON schema registry for market ticks with backward compatibility rules.",
    language: "json",
    code: `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "FinancialMarketTickSchema",
  "version": "1.0.0",
  "compatibility": "BACKWARD_TRANSITIVE",
  "description": "Baseline transaction schema for high-throughput financial market order book tick events.",
  "type": "object",
  "properties": {
    "sequence_id": {
      "type": "integer",
      "description": "Monotonically increasing 64-bit integer tick sequence identifier."
    },
    "timestamp_ns": {
      "type": "integer",
      "description": "UTC timestamp in nanoseconds since UNIX epoch."
    },
    "iso_timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "ISO-8601 formatted UTC timestamp string."
    },
    "symbol": {
      "type": "string",
      "pattern": "^[A-Z0-9.-]+$",
      "description": "Standardized ticker symbol identifier (e.g. BTC-USD, SPY)."
    },
    "exchange": {
      "type": "string",
      "description": "Source market exchange identifier (e.g. NASDAQ, CME, BINANCE)."
    },
    "bid_price": {
      "type": "number",
      "minimum": 0.0,
      "description": "Top-of-book highest active bid price."
    },
    "ask_price": {
      "type": "number",
      "minimum": 0.0,
      "description": "Top-of-book lowest active ask price."
    },
    "bid_size": {
      "type": "number",
      "minimum": 0.0,
      "description": "Top-of-book total cumulative volume available at bid price."
    },
    "ask_size": {
      "type": "number",
      "minimum": 0.0,
      "description": "Top-of-book total cumulative volume available at ask price."
    },
    "last_trade_price": {
      "type": "number",
      "minimum": 0.0,
      "description": "Last execution match price."
    },
    "last_trade_size": {
      "type": "number",
      "minimum": 0.0,
      "description": "Last execution matched quantity."
    },
    "schema_version": {
      "type": "string",
      "default": "1.0.0",
      "description": "Semantic versioning identifier of the emitting tick schema."
    }
  },
  "required": [
    "sequence_id",
    "timestamp_ns",
    "symbol",
    "bid_price",
    "ask_price",
    "bid_size",
    "ask_size"
  ],
  "additionalProperties": true
}`,
  },
  {
    path: "market_streaming_framework/ingestion/base_producer.py",
    name: "base_producer.py",
    category: "ingestion",
    description: "Abstract base class establishing contracts for thread-safe asynchronous market tick producers.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

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
            logger.error("Failed to collect producer metrics: %s\\n%s", str(e), traceback.format_exc())
            return {"error": str(e)}
`,
  },
  {
    path: "market_streaming_framework/ingestion/market_harvester.py",
    name: "market_harvester.py",
    category: "ingestion",
    description: "Concurrent market tick simulator, token-bucket rate limiter, proxy rotation, and Kafka producer.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

import os
import time
import math
import random
import asyncio
import logging
import datetime
import traceback
import urllib.parse
from typing import List, Dict, Optional, Callable, Any
from market_streaming_framework.ingestion.base_producer import (
    BaseProducer,
    MarketTickPayload,
)
from market_streaming_framework.config.kafka_config import KafkaBrokerConfig

logger = logging.getLogger("market_streaming_framework.ingestion.market_harvester")


class TokenBucketRateLimiter:
    """
    High-precision, thread-safe asynchronous Token-Bucket Rate Limiter.
    Enforces maximum events per second while absorbing micro-burst spikes.
    """

    def __init__(self, refill_rate_per_sec: float, max_burst_capacity: float):
        self.refill_rate = refill_rate_per_sec
        self.capacity = max_burst_capacity
        self.current_tokens = max_burst_capacity
        self.last_refill_timestamp = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens_requested: float = 1.0) -> bool:
        """
        Non-blocking token consumption check.
        Returns True if tokens were consumed, False if rate-limit breached.
        """
        try:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill_timestamp
                self.last_refill_timestamp = now

                # Replenish bucket based on elapsed time
                self.current_tokens = min(
                    self.capacity, self.current_tokens + elapsed * self.refill_rate
                )

                if self.current_tokens >= tokens_requested:
                    self.current_tokens -= tokens_requested
                    return True
                return False
        except Exception as e:
            logger.error(
                "Rate limiter calculation error: %s\\n%s", str(e), traceback.format_exc()
            )
            return False

    async def wait_and_acquire(self, tokens_requested: float = 1.0) -> None:
        """Cooperative backoff until sufficient tokens regenerate."""
        while not await self.acquire(tokens_requested):
            await asyncio.sleep(0.005)


class ProxyRotator:
    """
    Simulates high-frequency distributed proxy rotation pool for resilient
    upstream exchange websocket / HTTP gateway connections.
    """

    def __init__(self, proxy_pool: Optional[List[str]] = None):
        self.proxy_pool = proxy_pool or [
            "http://proxy-us-east.edge.internal:8080",
            "http://proxy-eu-central.edge.internal:8080",
            "http://proxy-ap-tokyo.edge.internal:8080",
            "http://proxy-us-west.edge.internal:8080",
            "http://proxy-uk-london.edge.internal:8080",
        ]
        self._index = 0
        self._active_proxy = self.proxy_pool[0]
        self._rotation_count = 0

    def rotate(self) -> str:
        """Thread-safe round-robin proxy rotation."""
        try:
            self._index = (self._index + 1) % len(self.proxy_pool)
            self._active_proxy = self.proxy_pool[self._index]
            self._rotation_count += 1
            logger.debug(
                "Rotated proxy to: %s (total rotations: %d)",
                self._active_proxy,
                self._rotation_count,
            )
            return self._active_proxy
        except Exception as e:
            logger.error("Error during proxy rotation: %s", str(e))
            return self.proxy_pool[0]

    def get_current_proxy(self) -> str:
        return self._active_proxy


class MockKafkaDirectProducer(BaseProducer):
    """
    High-performance in-memory simulated Kafka Broker Producer.
    Directly interfaces with pipeline sinks and backpressure queues while
    emulating partition keys, CRC checks, network serialization, and latency.
    """

    def __init__(self, config: KafkaBrokerConfig):
        super().__init__(
            client_id=config.client_id,
            topic=config.topic_config.raw_ticks_topic,
        )
        self.config = config
        self._buffer: List[MarketTickPayload] = []
        self._lock = asyncio.Lock()
        self.on_batch_delivered_callback: Optional[Callable[[List[MarketTickPayload]], None]] = None

    async def connect(self) -> None:
        try:
            logger.info(
                "Establishing virtual socket pool to Kafka cluster: %s",
                ",".join(self.config.bootstrap_servers),
            )
            await asyncio.sleep(0.02)
            self.is_connected = True
            logger.info("Kafka Broker connection pool verified [PLAINTEXT/SASL OK].")
        except Exception as e:
            self.is_connected = False
            logger.error("Kafka connection initialization failed: %s\\n%s", str(e), traceback.format_exc())
            raise

    async def produce(
        self,
        key: str,
        payload: MarketTickPayload,
        on_delivery: Optional[Callable[[Optional[Exception], Any], None]] = None,
    ) -> bool:
        if not self.is_connected:
            raise ConnectionError("Cannot produce: Kafka producer is disconnected.")

        try:
            async with self._lock:
                self._buffer.append(payload)
                self.metrics.total_produced += 1
                self.metrics.total_bytes_sent += 256  # Approximate serialized byte footprint
                self.metrics.last_produced_timestamp = time.time()

            if on_delivery:
                on_delivery(None, {"topic": self.topic, "partition": hash(key) % 8, "key": key})
            return True
        except Exception as e:
            self.metrics.total_failures += 1
            logger.error("Produce event failure: %s\\n%s", str(e), traceback.format_exc())
            if on_delivery:
                on_delivery(e, None)
            return False

    async def produce_batch(self, batch: List[MarketTickPayload]) -> int:
        if not self.is_connected or not batch:
            return 0
        try:
            async with self._lock:
                self._buffer.extend(batch)
                self.metrics.total_produced += len(batch)
                self.metrics.total_bytes_sent += len(batch) * 256
                self.metrics.last_produced_timestamp = time.time()

            if self.on_batch_delivered_callback:
                self.on_batch_delivered_callback(batch)
            return len(batch)
        except Exception as e:
            self.metrics.total_failures += len(batch)
            logger.error("Produce batch failure: %s\\n%s", str(e), traceback.format_exc())
            return 0

    async def flush(self, timeout_seconds: float = 10.0) -> None:
        try:
            async with self._lock:
                drained = len(self._buffer)
                self._buffer.clear()
            logger.debug("Flushed %d in-flight records from producer buffer.", drained)
        except Exception as e:
            logger.error("Flush failure: %s\\n%s", str(e), traceback.format_exc())

    async def close(self) -> None:
        try:
            await self.flush()
            self.is_connected = False
            logger.info("Kafka producer gracefully closed.")
        except Exception as e:
            logger.error("Close failure: %s\\n%s", str(e), traceback.format_exc())


class MarketHarvester:
    """
    Highly concurrent financial market simulator and tick ingestion engine.
    Mocks live crypto/equity Level-1 and Level-2 order book tick streams,
    enforces token-bucket rate limits, simulates proxy rotation, and pushes
    validated events directly to Kafka producers.
    """

    def __init__(
        self,
        symbols: List[str],
        producer: BaseProducer,
        rate_limiter: TokenBucketRateLimiter,
        proxy_rotator: Optional[ProxyRotator] = None,
    ):
        self.symbols = symbols
        self.producer = producer
        self.rate_limiter = rate_limiter
        self.proxy_rotator = proxy_rotator or ProxyRotator()
        self._running = False
        self._sequence_id = 0
        self._price_state: Dict[str, float] = {}
        self._volatility_state: Dict[str, float] = {}
        self._initialize_market_state()

    def _initialize_market_state(self) -> None:
        """Seed initial spot prices and dynamic volatility per asset class."""
        baseline_prices = {
            "BTC-USD": 68450.0,
            "ETH-USD": 3520.0,
            "SOL-USD": 178.50,
            "SPY": 512.30,
            "QQQ": 441.80,
            "NVDA": 128.40,
        }
        for sym in self.symbols:
            self._price_state[sym] = baseline_prices.get(sym, 100.0)
            self._volatility_state[sym] = 0.0012  # baseline per-tick variance

    def _simulate_tick(self, symbol: str) -> MarketTickPayload:
        """
        Generate realistic financial tick using Geometric Brownian Motion
        with Poisson micro-jumps and order-book depth spread dynamics.
        """
        try:
            self._sequence_id += 1
            now_ns = time.time_ns()
            iso_now = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Geometric Brownian step: S_{t+1} = S_t * exp((mu - 0.5*sigma^2)*dt + sigma*dW)
            current_price = self._price_state[symbol]
            vol = self._volatility_state[symbol]
            drift = 0.00001
            shock = random.gauss(0.0, 1.0) * vol
            
            # 2% chance of micro-liquidity jump
            if random.random() < 0.02:
                jump = random.choice([-1.0, 1.0]) * (vol * 3.5)
                shock += jump

            new_mid = max(0.01, current_price * math.exp(drift + shock))
            self._price_state[symbol] = new_mid

            # Half spread dynamic based on price magnitude
            half_spread = max(0.01, round(new_mid * 0.00015, 2))
            bid_price = round(new_mid - half_spread, 2)
            ask_price = round(new_mid + half_spread, 2)

            # Sizes with Pareto-tail simulation
            bid_size = round(abs(random.weibullvariate(1.5, 0.8)) * 12.5 + 1.0, 4)
            ask_size = round(abs(random.weibullvariate(1.5, 0.8)) * 12.5 + 1.0, 4)

            # Last executed trade
            side_executed = random.choice(["BUY", "SELL"])
            last_trade_price = ask_price if side_executed == "BUY" else bid_price
            last_trade_size = round(min(bid_size, ask_size) * random.uniform(0.1, 0.8), 4)

            # Exchange classification
            exchange = "BINANCE" if "USD" in symbol else "NASDAQ"

            payload = MarketTickPayload(
                sequence_id=self._sequence_id,
                timestamp_ns=now_ns,
                iso_timestamp=iso_now,
                symbol=symbol,
                exchange=exchange,
                bid_price=bid_price,
                ask_price=ask_price,
                bid_size=bid_size,
                ask_size=ask_size,
                last_trade_price=last_trade_price,
                last_trade_size=last_trade_size,
                schema_version="1.0.0",
            )

            # Dynamic Schema Evolution Mocking:
            # 5% of ticks include experimental advanced fields to test schema agility
            if random.random() < 0.05:
                payload.extra_fields["implied_volatility"] = round(vol * math.sqrt(252 * 390 * 60), 4)
                payload.extra_fields["synthetic_liquidity_score"] = round(random.uniform(0.75, 1.0), 3)

            return payload
        except Exception as e:
            logger.error("Failed to generate simulated tick: %s\\n%s", str(e), traceback.format_exc())
            raise

    async def start_harvesting(self, stop_event: Optional[asyncio.Event] = None) -> None:
        """
        Main concurrent harvest execution loop.
        Monitors token bucket, rotates simulated proxy routing, and streams ticks.
        """
        self._running = True
        logger.info(
            "Starting market harvester for %d symbols across proxy %s",
            len(self.symbols),
            self.proxy_rotator.get_current_proxy(),
        )

        batch_buffer: List[MarketTickPayload] = []
        last_proxy_rotation = time.time()

        try:
            while self._running:
                if stop_event and stop_event.is_set():
                    logger.info("Stop event signaled to harvester loop.")
                    break

                # Periodic proxy rotation check (every 30s)
                if time.time() - last_proxy_rotation > 30.0:
                    self.proxy_rotator.rotate()
                    last_proxy_rotation = time.time()

                # Generate ticks across symbols
                for sym in self.symbols:
                    if await self.rate_limiter.acquire(1.0):
                        tick = self._simulate_tick(sym)
                        batch_buffer.append(tick)
                    else:
                        self.producer.metrics.rate_limited_drops += 1
                        logger.warning("Ingestion rate limit throttled event for %s", sym)

                # Micro-batch dispatch
                if len(batch_buffer) >= 50:
                    await self.producer.produce_batch(batch_buffer)
                    batch_buffer.clear()

                # High-frequency sleep to mimic real-world 1-5ms event interval
                await asyncio.sleep(0.01)

            # Flush residual buffer
            if batch_buffer:
                await self.producer.produce_batch(batch_buffer)
                batch_buffer.clear()

        except asyncio.CancelledError:
            logger.info("Harvester task cancelled gracefully.")
        except Exception as e:
            logger.error("Harvester encountered fatal error: %s\\n%s", str(e), traceback.format_exc())
        finally:
            self._running = False
            logger.info("Market harvester stopped.")

    def stop(self) -> None:
        self._running = False
`,
  },
  {
    path: "market_streaming_framework/stream_processing/backpressure_queue.py",
    name: "backpressure_queue.py",
    category: "stream_processing",
    description: "Bound-ring circular buffer queue managing micro-batches with high/critical watermark throttling.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

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
                "Unexpected failure in ring buffer put: %s\\n%s",
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
                "Unexpected failure in ring buffer get_batch: %s\\n%s",
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
`,
  },
  {
    path: "market_streaming_framework/stream_processing/feature_generator.py",
    name: "feature_generator.py",
    category: "stream_processing",
    description: "Vectorized stream analytics utilizing Pandas and NumPy for VWAP, realized volatility, and OBI.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

import math
import logging
import traceback
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd
from market_streaming_framework.ingestion.base_producer import MarketTickPayload

logger = logging.getLogger("market_streaming_framework.stream_processing.feature_generator")


@dataclass
class CalculatedFeatureMatrix:
    """Encapsulates the vectorized analytical feature output batch."""
    batch_id: int
    record_count: int
    symbols_present: List[str]
    dataframe: pd.DataFrame
    computation_latency_ms: float
    timestamp_ns: int


class VectorizedFeatureGenerator:
    """
    High-throughput stream processing consumer block powered by NumPy and Pandas.
    Transforms raw micro-batches into structured continuous memory matrices and computes:
      1. Top-of-Book Micro-Price (Volume-Weighted Midpoint)
      2. Spread and Spread Basis Points (Bps)
      3. Real-Time Order Book Imbalance (OBI in [-1, +1])
      4. Rolling Volume-Weighted Average Price (VWAP)
      5. Vectorized Rolling Realized Volatility (Annualized)
      6. Preserves dynamic schema attributes without data loss or pipeline interruption.
    """

    def __init__(self, rolling_window_size: int = 100):
        self.rolling_window_size = rolling_window_size
        self._batch_counter = 0
        # Rolling historical price and volume state for cumulative metrics
        self._symbol_cum_dollar_vol: Dict[str, float] = {}
        self._symbol_cum_vol: Dict[str, float] = {}
        self._symbol_price_history: Dict[str, List[float]] = {}
        logger.info(
            "VectorizedFeatureGenerator initialized with rolling window: %d",
            self.rolling_window_size,
        )

    def process_micro_batch(
        self, raw_ticks: List[MarketTickPayload]
    ) -> Optional[CalculatedFeatureMatrix]:
        """
        Consumes raw tick payloads, converts to contiguous 2D float64 memory buffers,
        and applies vectorized arithmetic across rows without iterative Python loops.
        """
        if not raw_ticks:
            return None

        start_time = np.datetime64("now")
        perf_start = start_time.astype("int64")

        try:
            self._batch_counter += 1

            # 1. Transform tick payloads into flat row records preserving extra fields
            records = [tick.to_dict() for tick in raw_ticks]
            df = pd.DataFrame.from_records(records)

            # Ensure baseline numeric types are contiguous float64 / int64
            numeric_cols = [
                "bid_price",
                "ask_price",
                "bid_size",
                "ask_size",
                "last_trade_price",
                "last_trade_size",
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(np.float64)

            # 2. Vectorized Midpoint, Spread, and Spread Bps
            bid_p = df["bid_price"].to_numpy()
            ask_p = df["ask_price"].to_numpy()
            bid_sz = df["bid_size"].to_numpy()
            ask_sz = df["ask_size"].to_numpy()
            trade_p = df["last_trade_price"].to_numpy()
            trade_sz = df["last_trade_size"].to_numpy()

            # Mid price vector
            mid_price_vec = (bid_p + ask_p) / 2.0
            df["mid_price"] = mid_price_vec

            # Spread vector
            spread_vec = np.maximum(0.0001, ask_p - bid_p)
            df["spread"] = spread_vec
            df["spread_bps"] = (spread_vec / np.maximum(0.0001, mid_price_vec)) * 10000.0

            # 3. Vectorized Order Book Imbalance (OBI): (BidSize - AskSize) / (BidSize + AskSize)
            total_size_vec = bid_sz + ask_sz
            total_size_safe = np.where(total_size_vec > 0, total_size_vec, 1.0)
            obi_vec = (bid_sz - ask_sz) / total_size_safe
            df["order_book_imbalance"] = np.clip(obi_vec, -1.0, 1.0)

            # 4. Micro-price: (BidPrice * AskSize + AskPrice * BidSize) / (BidSize + AskSize)
            micro_price_vec = (bid_p * ask_sz + ask_p * bid_sz) / total_size_safe
            df["micro_price"] = micro_price_vec

            # 5. Vectorized Rolling VWAP per symbol
            # Dollar volume = TradePrice * TradeSize
            dollar_vol_vec = trade_p * trade_sz
            df["dollar_volume"] = dollar_vol_vec

            # Update rolling cumulative VWAP states
            vwap_results: List[float] = []
            volatility_results: List[float] = []

            for sym, group in df.groupby("symbol", sort=False):
                group_idxs = group.index.to_numpy()
                group_trade_p = group["last_trade_price"].to_numpy()
                group_trade_sz = group["last_trade_size"].to_numpy()
                group_mid = group["mid_price"].to_numpy()

                # VWAP cumulative calculation
                current_cum_dollar = self._symbol_cum_dollar_vol.get(sym, 0.0)
                current_cum_vol = self._symbol_cum_vol.get(sym, 0.0)

                cum_dollar = current_cum_dollar + np.cumsum(group_trade_p * group_trade_sz)
                cum_vol = current_cum_vol + np.cumsum(group_trade_sz)
                cum_vol_safe = np.where(cum_vol > 0, cum_vol, 1.0)
                sym_vwap = cum_dollar / cum_vol_safe

                self._symbol_cum_dollar_vol[sym] = float(cum_dollar[-1])
                self._symbol_cum_vol[sym] = float(cum_vol[-1])
                df.loc[group_idxs, "rolling_vwap"] = sym_vwap

                # Vectorized Realized Volatility:
                # Maintain price window in history buffer
                hist = self._symbol_price_history.get(sym, [])
                hist.extend(group_mid.tolist())
                # Bound history size
                if len(hist) > self.rolling_window_size:
                    hist = hist[-self.rolling_window_size:]
                self._symbol_price_history[sym] = hist

                # Calculate log returns vector
                hist_arr = np.array(hist, dtype=np.float64)
                if len(hist_arr) >= 5:
                    log_returns = np.diff(np.log(np.maximum(1e-6, hist_arr)))
                    # Standard deviation of returns scaled to annualized volatility
                    # Scaling factor: sqrt(252 trading days * 6.5 hours * 3600 seconds)
                    annualization_factor = math.sqrt(252.0 * 23400.0)
                    realized_vol = np.std(log_returns) * annualization_factor
                    df.loc[group_idxs, "rolling_volatility"] = float(realized_vol)
                else:
                    df.loc[group_idxs, "rolling_volatility"] = 0.0150  # baseline default

            # Fill any initial NaN values cleanly
            df["rolling_vwap"] = df["rolling_vwap"].fillna(df["mid_price"])
            df["rolling_volatility"] = df["rolling_volatility"].fillna(0.0150)

            perf_end = np.datetime64("now").astype("int64")
            latency_ms = max(0.01, float(perf_end - perf_start))

            logger.debug(
                "Processed batch #%d with %d ticks across %d symbols in %.2f ms",
                self._batch_counter,
                len(df),
                df["symbol"].nunique(),
                latency_ms,
            )

            return CalculatedFeatureMatrix(
                batch_id=self._batch_counter,
                record_count=len(df),
                symbols_present=df["symbol"].unique().tolist(),
                dataframe=df,
                computation_latency_ms=latency_ms,
                timestamp_ns=int(df["timestamp_ns"].iloc[-1]),
            )

        except Exception as e:
            logger.error(
                "Critical failure during vectorized feature generation: %s\\n%s",
                str(e),
                traceback.format_exc(),
            )
            return None
`,
  },
  {
    path: "market_streaming_framework/storage/partitioned_sink.py",
    name: "partitioned_sink.py",
    category: "storage",
    description: "Micro-batch persistence coordinator writing to time-partitioned directories with atomic file replacements.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

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
            logger.error("Failed to initialize lake base directories: %s\\n%s", str(e), traceback.format_exc())
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
                "Fatal I/O failure during partitioned sink micro-batch write: %s\\n%s",
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
`,
  },
  {
    path: "market_streaming_framework/storage/compaction_worker.py",
    name: "compaction_worker.py",
    category: "storage",
    description: "Background maintenance daemon merging raw micro-batches into Snappy-compressed columnar Parquet files via PyArrow.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

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
                    "Error during background compaction cycle: %s\\n%s",
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
                "Failed traversing partitions for compaction: %s\\n%s",
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
                "Partition compaction failure in %s: %s\\n%s",
                partition_dir,
                str(e),
                traceback.format_exc(),
            )
            return False
`,
  },
  {
    path: "market_streaming_framework/monitoring/drift_auditor.py",
    name: "drift_auditor.py",
    category: "monitoring",
    description: "Statistical data quality gatekeeper using SciPy Wasserstein distance, JS-divergence, and dynamic schema evolution.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

import os
import json
import time
import logging
import traceback
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple, Set
import numpy as np
import pandas as pd

try:
    from scipy.stats import wasserstein_distance
    from scipy.spatial.distance import jensenshannon
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

logger = logging.getLogger("market_streaming_framework.monitoring.drift_auditor")


def _fallback_wasserstein(u: np.ndarray, v: np.ndarray) -> float:
    """Vectorized Earth Mover's Distance fallback when SciPy is unbundled."""
    u_sorted = np.sort(u)
    v_sorted = np.sort(v)
    # Quantile interpolation
    all_vals = np.concatenate([u_sorted, v_sorted])
    all_vals.sort()
    u_cdf = np.searchsorted(u_sorted, all_vals, side="right") / len(u_sorted)
    v_cdf = np.searchsorted(v_sorted, all_vals, side="right") / len(v_sorted)
    deltas = np.diff(all_vals)
    return float(np.sum(np.abs(u_cdf[:-1] - v_cdf[:-1]) * deltas))


def _fallback_jensenshannon(p: np.ndarray, q: np.ndarray) -> float:
    """Vectorized Jensen-Shannon Divergence fallback."""
    p_safe = np.asarray(p, dtype=np.float64) + 1e-12
    q_safe = np.asarray(q, dtype=np.float64) + 1e-12
    p_norm = p_safe / np.sum(p_safe)
    q_norm = q_safe / np.sum(q_safe)
    m = 0.5 * (p_norm + q_norm)
    kl_pm = np.sum(p_norm * np.log(p_norm / m))
    kl_qm = np.sum(q_norm * np.log(q_norm / m))
    js_div = 0.5 * (kl_pm + kl_qm)
    return float(np.sqrt(max(0.0, js_div)))


@dataclass
class DriftMetricResult:
    """Individual metric statistical distribution drift test result."""
    metric_name: str
    wasserstein_distance: float
    jensen_shannon_divergence: float
    is_drifted: bool
    threshold: float
    baseline_mean: float
    batch_mean: float


@dataclass
class DriftAuditReport:
    """Comprehensive data quality and distribution shift evaluation."""
    audit_id: int
    timestamp_ns: int
    has_drift: bool
    metric_results: Dict[str, DriftMetricResult]
    schema_mutations_detected: List[str]
    processing_time_ms: float


class SchemaEvolutionManager:
    """
    Guarantees structural agility and backward-compatible dynamic evolution.
    Checks incoming payloads against the central schema registry, automatically
    absorbs unmapped feature columns into the canonical dictionary, and logs
    schema version progressions (e.g. v1.0.0 -> v1.1.0) without crashing ingestion.
    """

    def __init__(self, registry_path: str):
        self.registry_path = os.path.abspath(registry_path)
        self.schema: Dict[str, Any] = {}
        self.known_properties: Set[str] = set()
        self.current_version: str = "1.0.0"
        self._load_registry()

    def _load_registry(self) -> None:
        try:
            if os.path.exists(self.registry_path):
                with open(self.registry_path, "r", encoding="utf-8") as f:
                    self.schema = json.load(f)
                    self.known_properties = set(self.schema.get("properties", {}).keys())
                    self.current_version = self.schema.get("version", "1.0.0")
                logger.info(
                    "Schema registry loaded (Version: %s, %d properties).",
                    self.current_version,
                    len(self.known_properties),
                )
            else:
                logger.warning("Registry path %s not found. Initializing memory baseline.", self.registry_path)
                self.known_properties = {
                    "sequence_id", "timestamp_ns", "iso_timestamp", "symbol", "exchange",
                    "bid_price", "ask_price", "bid_size", "ask_size", "last_trade_price",
                    "last_trade_size", "schema_version"
                }
        except Exception as e:
            logger.error("Failed loading schema registry: %s\\n%s", str(e), traceback.format_exc())

    def validate_and_evolve(self, sample_records: List[Dict[str, Any]]) -> List[str]:
        """
        Scans batch for unrecognized fields. If detected, dynamically expands
        schema definition, records new property types, and increments minor version.
        """
        evolved_fields: List[str] = []
        if not sample_records:
            return evolved_fields

        try:
            detected_keys: Set[str] = set()
            for rec in sample_records:
                detected_keys.update(rec.keys())

            new_fields = detected_keys - self.known_properties
            if new_fields:
                for field_name in new_fields:
                    sample_val = next(
                        (rec[field_name] for rec in sample_records if field_name in rec), None
                    )
                    inferred_type = "string"
                    if isinstance(sample_val, int):
                        inferred_type = "integer"
                    elif isinstance(sample_val, float):
                        inferred_type = "number"
                    elif isinstance(sample_val, bool):
                        inferred_type = "boolean"

                    # Register dynamically
                    if "properties" in self.schema:
                        self.schema["properties"][field_name] = {
                            "type": inferred_type,
                            "description": f"Dynamically evolved market metric at runtime: {field_name}",
                            "evolved_at_epoch": time.time(),
                        }
                    self.known_properties.add(field_name)
                    evolved_fields.append(f"{field_name}:{inferred_type}")

                # Increment minor version (e.g., 1.0.0 -> 1.1.0)
                v_parts = self.current_version.split(".")
                if len(v_parts) == 3:
                    minor = int(v_parts[1]) + 1
                    self.current_version = f"{v_parts[0]}.{minor}.0"
                    self.schema["version"] = self.current_version

                logger.info(
                    "DYNAMIC SCHEMA EVOLUTION EVENT: Evolved schema to %s. Added properties: %s",
                    self.current_version,
                    evolved_fields,
                )
                self._persist_registry()

            return evolved_fields
        except Exception as e:
            logger.error(
                "Error during dynamic schema evolution: %s\\n%s", str(e), traceback.format_exc()
            )
            return evolved_fields

    def _persist_registry(self) -> None:
        """Persists evolved schema definition to disk atomically."""
        try:
            temp_path = f"{self.registry_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self.schema, f, indent=2)
            os.replace(temp_path, self.registry_path)
            logger.debug("Persisted updated schema registry to %s", self.registry_path)
        except Exception as e:
            logger.error("Failed to persist updated schema: %s", str(e))


class DataDriftAuditor:
    """
    Statistical quality gatekeeper.
    Employs SciPy Wasserstein Distance and Jensen-Shannon Divergence to evaluate
    distributional shifts across rolling feature vectors (Spread, Volatility, OBI).
    Prevents degenerate statistical inputs from feeding production ML inference.
    """

    def __init__(
        self,
        schema_manager: SchemaEvolutionManager,
        drift_threshold: float = 0.08,
        baseline_sample_size: int = 500,
    ):
        self.schema_manager = schema_manager
        self.drift_threshold = drift_threshold
        self.baseline_sample_size = baseline_sample_size
        self._audit_counter = 0

        # Baseline empirical distributions
        self._baselines: Dict[str, np.ndarray] = {
            "spread_bps": np.random.normal(loc=2.5, scale=0.8, size=baseline_sample_size),
            "order_book_imbalance": np.random.uniform(low=-0.6, high=0.6, size=baseline_sample_size),
            "rolling_volatility": np.random.normal(loc=0.018, scale=0.005, size=baseline_sample_size),
        }
        logger.info(
            "DataDriftAuditor initialized (Threshold: %.3f, Baseline size: %d, SciPy: %s)",
            self.drift_threshold,
            self.baseline_sample_size,
            "Enabled" if HAS_SCIPY else "Fallback",
        )

    def audit_feature_matrix(self, df: pd.DataFrame) -> DriftAuditReport:
        """
        Executes non-parametric statistical distance tests against baseline distributions.
        """
        start_t = time.time()
        self._audit_counter += 1
        metric_results: Dict[str, DriftMetricResult] = {}
        has_any_drift = False

        try:
            # 1. Check Schema Evolution on batch
            records_sample = df.head(10).to_dict(orient="records")
            evolved = self.schema_manager.validate_and_evolve(records_sample)

            # 2. Vectorized Statistical Distance Testing
            for col_name, baseline_dist in self._baselines.items():
                if col_name not in df.columns:
                    continue

                batch_vals = df[col_name].dropna().to_numpy(dtype=np.float64)
                if len(batch_vals) < 10:
                    continue

                # Compute Wasserstein distance
                if HAS_SCIPY:
                    w_dist = float(wasserstein_distance(baseline_dist, batch_vals))
                    # Compute JS Divergence over 20-bin histogram density
                    hist_range = (
                        min(baseline_dist.min(), batch_vals.min()),
                        max(baseline_dist.max(), batch_vals.max()),
                    )
                    p_hist, _ = np.histogram(baseline_dist, bins=20, range=hist_range, density=True)
                    q_hist, _ = np.histogram(batch_vals, bins=20, range=hist_range, density=True)
                    js_div = float(jensenshannon(p_hist, q_hist))
                else:
                    w_dist = _fallback_wasserstein(baseline_dist, batch_vals)
                    hist_range = (
                        min(baseline_dist.min(), batch_vals.min()),
                        max(baseline_dist.max(), batch_vals.max()),
                    )
                    p_hist, _ = np.histogram(baseline_dist, bins=20, range=hist_range, density=True)
                    q_hist, _ = np.histogram(batch_vals, bins=20, range=hist_range, density=True)
                    js_div = _fallback_jensenshannon(p_hist, q_hist)

                # Flag drift condition
                is_drift = (w_dist > self.drift_threshold) or (js_div > self.drift_threshold * 1.5)
                if is_drift:
                    has_any_drift = True
                    logger.warning(
                        "DISTRIBUTION DRIFT DETECTED in '%s'! Wasserstein: %.4f, JS-Div: %.4f (Limit: %.4f)",
                        col_name,
                        w_dist,
                        js_div,
                        self.drift_threshold,
                    )

                metric_results[col_name] = DriftMetricResult(
                    metric_name=col_name,
                    wasserstein_distance=round(w_dist, 5),
                    jensen_shannon_divergence=round(js_div, 5),
                    is_drifted=is_drift,
                    threshold=self.drift_threshold,
                    baseline_mean=round(float(np.mean(baseline_dist)), 5),
                    batch_mean=round(float(np.mean(batch_vals)), 5),
                )

                # Slow adaptive update of baseline (exponential moving reservoir)
                if not is_drift:
                    blend_idx = np.random.choice(len(baseline_dist), size=min(10, len(batch_vals)), replace=False)
                    self._baselines[col_name][blend_idx] = batch_vals[: len(blend_idx)]

            duration_ms = (time.time() - start_t) * 1000.0
            return DriftAuditReport(
                audit_id=self._audit_counter,
                timestamp_ns=time.time_ns(),
                has_drift=has_any_drift,
                metric_results=metric_results,
                schema_mutations_detected=evolved,
                processing_time_ms=round(duration_ms, 2),
            )

        except Exception as e:
            logger.error("Drift audit failure: %s\\n%s", str(e), traceback.format_exc())
            return DriftAuditReport(
                audit_id=self._audit_counter,
                timestamp_ns=time.time_ns(),
                has_drift=False,
                metric_results={},
                schema_mutations_detected=[],
                processing_time_ms=0.0,
            )
`,
  },
  {
    path: "market_streaming_framework/requirements.txt",
    name: "requirements.txt",
    category: "root",
    description: "Exact system package requirements (numpy, pandas, scipy, pyarrow, pydantic, confluent-kafka).",
    language: "text",
    code: `# ChronosLake Financial Market Data Lake & Feature Store Requirements
# Target Python Version: >=3.10

numpy>=1.26.0,<2.2.0
pandas>=2.2.0,<2.3.0
scipy>=1.12.0,<1.15.0
pyarrow>=15.0.0,<18.0.0
pydantic>=2.6.0,<2.10.0
confluent-kafka>=2.3.0
aiohttp>=3.9.0
python-dateutil>=2.8.2
fastparquet>=2024.2.0
`,
  },
  {
    path: "market_streaming_framework/main.py",
    name: "main.py",
    category: "root",
    description: "Root multi-threaded & async framework orchestrator tying ingestion, ring queue, features, sink, and compaction.",
    language: "python",
    code: `# Author: Rayaan tasnim, 13 aged prodigy

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
                    "Unhandled exception in consumer worker thread: %s\\n%s",
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
            logger.error("Async pipeline execution failure: %s\\n%s", str(e), traceback.format_exc())

    def run(self, run_duration_seconds: Optional[float] = None) -> None:
        """
        Launches all processes and background threads.
        Blocks until timeout or interrupt signal.
        """
        logger.info(
            "====================================================================\\n"
            "   CHRONOSLAKE: EVENT-DRIVEN MARKET DATA LAKE & FEATURE STORE       \\n"
            "   Author: Rayaan tasnim, 13 aged prodigy                           \\n"
            "   Symbols: %s                                                      \\n"
            "   Data Lake Base: %s                                               \\n"
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
        logger.error("Fatal startup error in main: %s\\n%s", str(e), traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
`,
  },
];
