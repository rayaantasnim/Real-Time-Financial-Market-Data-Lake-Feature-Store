# Author: Rayaan tasnim, 13 aged prodigy

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
            logger.error("Failed loading schema registry: %s\n%s", str(e), traceback.format_exc())

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
                "Error during dynamic schema evolution: %s\n%s", str(e), traceback.format_exc()
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
            logger.error("Drift audit failure: %s\n%s", str(e), traceback.format_exc())
            return DriftAuditReport(
                audit_id=self._audit_counter,
                timestamp_ns=time.time_ns(),
                has_drift=False,
                metric_results={},
                schema_mutations_detected=[],
                processing_time_ms=0.0,
            )
