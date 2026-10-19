import React from "react";
import {
  Layers,
  Cpu,
  Database,
  ShieldCheck,
  TrendingUp,
  FileCode,
  Activity,
  ArrowRight,
  Sparkles,
  Zap,
} from "lucide-react";

export const ArchitectureBlueprint: React.FC = () => {
  return (
    <div className="space-y-6">
      {/* Header Banner */}
      <div className="p-6 rounded-2xl bg-gradient-to-br from-slate-900 via-slate-900 to-emerald-950/40 border border-slate-800 shadow-xl">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <div className="inline-flex items-center gap-2 px-2.5 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-emerald-300 text-xs font-mono mb-2">
              <Sparkles className="w-3.5 h-3.5" />
              <span>Architectural Blueprint &bull; Author: Rayaan tasnim, 13 aged prodigy</span>
            </div>
            <h1 className="text-2xl font-bold text-slate-100">
              Event-Driven Financial Market Data Lake &amp; Feature Store
            </h1>
            <p className="text-sm text-slate-400 mt-1 max-w-3xl">
              High-throughput micro-batching stream topology, bounded ring queue backpressure,
              zero-loop vectorized matrix arithmetic in NumPy/Pandas, PyArrow out-of-core columnar
              compaction, and non-parametric SciPy distribution drift detection with dynamic schema evolution.
            </p>
          </div>
        </div>
      </div>

      {/* Mathematical Formulations Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {/* Card 1: VWAP */}
        <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
          <div className="flex items-center gap-2 text-emerald-400 text-xs font-semibold uppercase tracking-wider">
            <TrendingUp className="w-4 h-4" />
            <span>1. Vectorized Rolling VWAP</span>
          </div>
          <div className="p-3 rounded-lg bg-slate-950 font-mono text-xs text-emerald-300 border border-slate-800">
            {"VWAP_t = \\frac{\\sum_{i=1}^t P_{trade, i} \\cdot V_{trade, i}}{\\sum_{i=1}^t V_{trade, i}}"}
          </div>
          <p className="text-xs text-slate-400 leading-relaxed">
            Vectorized cumulative dot-product without nested loops. Group-wise cumulative sum is calculated using continuous NumPy arrays for sub-millisecond execution over 500-tick micro-batches.
          </p>
        </div>

        {/* Card 2: Realized Volatility */}
        <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
          <div className="flex items-center gap-2 text-cyan-400 text-xs font-semibold uppercase tracking-wider">
            <Activity className="w-4 h-4" />
            <span>2. Realized Volatility (Annualized)</span>
          </div>
          <div className="p-3 rounded-lg bg-slate-950 font-mono text-xs text-cyan-300 border border-slate-800">
            {"r_t = \\ln\\left(\\frac{P_t}{P_{t-1}}\\right); \\quad \\sigma = \\text{std}(r) \\cdot \\sqrt{252 \\times 23400}"}
          </div>
          <p className="text-xs text-slate-400 leading-relaxed">
            Computes logarithmic returns across the rolling price window buffer, applying standard sample deviation scaled by the annual trading seconds square-root multiplier.
          </p>
        </div>

        {/* Card 3: OBI & Micro-Price */}
        <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
          <div className="flex items-center gap-2 text-teal-400 text-xs font-semibold uppercase tracking-wider">
            <Cpu className="w-4 h-4" />
            <span>3. Order Book Imbalance (OBI) &amp; Micro-Price</span>
          </div>
          <div className="p-3 rounded-lg bg-slate-950 font-mono text-xs text-teal-300 border border-slate-800 space-y-1">
            <div>{"OBI = \\frac{V_{bid} - V_{ask}}{V_{bid} + V_{ask}} \\in [-1.0, 1.0]"}</div>
            <div>{"P_{micro} = \\frac{P_{bid} V_{ask} + P_{ask} V_{bid}}{V_{bid} + V_{ask}}"}</div>
          </div>
          <p className="text-xs text-slate-400 leading-relaxed">
            Volume-weighted top-of-book metrics that anticipate short-term price discovery and order flow toxicity ahead of simple midpoint quotes.
          </p>
        </div>

        {/* Card 4: Ring Queue */}
        <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
          <div className="flex items-center gap-2 text-indigo-400 text-xs font-semibold uppercase tracking-wider">
            <Layers className="w-4 h-4" />
            <span>4. Bounded Ring Backpressure Queue</span>
          </div>
          <div className="p-3 rounded-lg bg-slate-950 font-mono text-xs text-indigo-300 border border-slate-800">
            {"High WM: 80\\%, \\quad Crit WM: 95\\%, \\quad Capacity: 50,000"}
          </div>
          <p className="text-xs text-slate-400 leading-relaxed">
            Pre-allocated contiguous circular buffer index pointers (<code className="text-slate-300">head</code>, <code className="text-slate-300">tail</code>, <code className="text-slate-300">size</code>) with dual condition variables. Halts producer rate when memory depth crosses limits to prevent Out-Of-Memory crashes.
          </p>
        </div>

        {/* Card 5: SciPy Wasserstein Drift */}
        <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
          <div className="flex items-center gap-2 text-purple-400 text-xs font-semibold uppercase tracking-wider">
            <ShieldCheck className="w-4 h-4" />
            <span>5. SciPy Wasserstein Distance Drift</span>
          </div>
          <div className="p-3 rounded-lg bg-slate-950 font-mono text-xs text-purple-300 border border-slate-800">
            {"l_1(u, v) = \\int_{-\\infty}^{+\\infty} |U(x) - V(x)| dx"}
          </div>
          <p className="text-xs text-slate-400 leading-relaxed">
            Non-parametric statistical distance gatekeeper comparing empirical cumulative distributions of rolling features against baseline models. Triggers alarms whenever <code className="text-slate-300">l_1 &gt; 0.08</code>.
          </p>
        </div>

        {/* Card 6: Dynamic Schema Evolution */}
        <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
          <div className="flex items-center gap-2 text-amber-400 text-xs font-semibold uppercase tracking-wider">
            <Sparkles className="w-4 h-4" />
            <span>6. Dynamic Schema Evolution</span>
          </div>
          <div className="p-3 rounded-lg bg-slate-950 font-mono text-xs text-amber-300 border border-slate-800">
            {"Compatibility: BACKWARD_TRANSITIVE, \\quad v1.0.0 \\to v1.1.0"}
          </div>
          <p className="text-xs text-slate-400 leading-relaxed">
            Runtime reflection absorbs newly introduced exchange fields (e.g., <code className="text-slate-300">implied_volatility</code>, <code className="text-slate-300">order_flow_toxicity</code>) without dropping records or breaking downstream sinks.
          </p>
        </div>
      </div>

      {/* Directory Layout & Storage Pipeline */}
      <div className="p-6 rounded-xl bg-slate-900 border border-slate-800 space-y-4">
        <h3 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
          <Database className="w-4 h-4 text-emerald-400" />
          Time-Partitioned Storage Layout &amp; Out-of-Core Columnar Compaction
        </h3>
        <p className="text-xs text-slate-400">
          Raw micro-batches are written atomically to staging and partition directories. The background compaction worker scans leaf partitions, merges and deduplicates records on <code className="text-slate-300">(sequence_id, symbol)</code>, and writes compressed Snappy Parquet files via PyArrow.
        </p>

        <div className="p-4 rounded-xl bg-slate-950 border border-slate-800 font-mono text-xs text-slate-300 space-y-2">
          <div className="text-emerald-400 font-semibold">market_data_lake/</div>
          <div className="pl-4 text-slate-400">&boxur;&boxh;&boxh; _staging/ <span className="text-slate-600">(atomic temp file write target)</span></div>
          <div className="pl-4 text-slate-300">&boxvr;&boxh;&boxh; year=2026/</div>
          <div className="pl-8 text-slate-300">&boxvr;&boxh;&boxh; month=10/</div>
          <div className="pl-12 text-slate-300">&boxvr;&boxh;&boxh; day=18/</div>
          <div className="pl-16 text-slate-300">&boxvr;&boxh;&boxh; symbol=BTC-USD/</div>
          <div className="pl-20 text-slate-500">&boxvr;&boxh;&boxh; microbatch_101_1729314000.json <span className="text-slate-600">(raw staging)</span></div>
          <div className="pl-20 text-emerald-400 font-bold">&boxur;&boxh;&boxh; compacted_dataset_1729314050.parquet <span className="text-emerald-500 font-normal">(PyArrow Snappy 4.8x compression)</span></div>
          <div className="pl-16 text-slate-300">&boxur;&boxh;&boxh; symbol=SPY/</div>
          <div className="pl-20 text-emerald-400 font-bold">&boxur;&boxh;&boxh; compacted_dataset_1729314050.parquet</div>
        </div>
      </div>
    </div>
  );
};
