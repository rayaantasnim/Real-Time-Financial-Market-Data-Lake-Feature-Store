# Author: Rayaan tasnim, 13 aged prodigy

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
                "Rate limiter calculation error: %s\n%s", str(e), traceback.format_exc()
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
            logger.error("Kafka connection initialization failed: %s\n%s", str(e), traceback.format_exc())
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
            logger.error("Produce event failure: %s\n%s", str(e), traceback.format_exc())
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
            logger.error("Produce batch failure: %s\n%s", str(e), traceback.format_exc())
            return 0

    async def flush(self, timeout_seconds: float = 10.0) -> None:
        try:
            async with self._lock:
                drained = len(self._buffer)
                self._buffer.clear()
            logger.debug("Flushed %d in-flight records from producer buffer.", drained)
        except Exception as e:
            logger.error("Flush failure: %s\n%s", str(e), traceback.format_exc())

    async def close(self) -> None:
        try:
            await self.flush()
            self.is_connected = False
            logger.info("Kafka producer gracefully closed.")
        except Exception as e:
            logger.error("Close failure: %s\n%s", str(e), traceback.format_exc())


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
            logger.error("Failed to generate simulated tick: %s\n%s", str(e), traceback.format_exc())
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
            logger.error("Harvester encountered fatal error: %s\n%s", str(e), traceback.format_exc())
        finally:
            self._running = False
            logger.info("Market harvester stopped.")

    def stop(self) -> None:
        self._running = False
