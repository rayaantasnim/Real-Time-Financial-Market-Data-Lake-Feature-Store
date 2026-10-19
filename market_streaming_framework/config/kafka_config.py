# Author: Rayaan tasnim, 13 aged prodigy

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
                "Failed to construct Kafka producer configuration dictionary: %s\n%s",
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
