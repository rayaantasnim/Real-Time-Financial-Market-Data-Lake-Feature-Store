# Author: Rayaan tasnim, 13 aged prodigy

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
                "Critical failure during vectorized feature generation: %s\n%s",
                str(e),
                traceback.format_exc(),
            )
            return None
