import React, { useState, useEffect, useRef } from "react";
import {
  Play,
  Pause,
  RotateCcw,
  AlertTriangle,
  Zap,
  Activity,
  Layers,
  Database,
  ArrowRight,
  TrendingUp,
  Cpu,
  ShieldAlert,
  Gauge,
  Sparkles,
  Server,
  FileSpreadsheet,
  CheckCircle2,
  Flame,
  Globe,
  Radio,
} from "lucide-react";

interface SimulatedTick {
  sequence_id: number;
  timestamp: string;
  symbol: string;
  exchange: string;
  bid_price: number;
  ask_price: number;
  bid_size: number;
  ask_size: number;
  mid_price: number;
  micro_price: number;
  spread_bps: number;
  order_book_imbalance: number;
  rolling_vwap: number;
  rolling_volatility: number;
  extra_fields?: Record<string, any>;
}

interface LogEntry {
  id: string;
  time: string;
  level: "INFO" | "WARN" | "CRIT" | "SUCCESS";
  component: string;
  message: string;
}

export const PipelineSimulator: React.FC = () => {
  const [isRunning, setIsRunning] = useState(true);
  const [ingestionRate, setIngestionRate] = useState(250); // ticks per second
  const [induceLag, setInduceLag] = useState(false);
  const [schemaVersion, setSchemaVersion] = useState("1.0.0");
  const [evolvedProperties, setEvolvedProperties] = useState<string[]>([]);
  const [activeProxy, setActiveProxy] = useState("http://proxy-us-east.edge.internal:8080");

  // Metrics state
  const [ticksCount, setTicksCount] = useState(14820);
  const [queueSize, setQueueSize] = useState(1850);
  const queueCapacity = 10000;
  const highWatermark = 8000; // 80%
  const critWatermark = 9500; // 95%

  const [activeSymbol, setActiveSymbol] = useState("BTC-USD");
  const [symbolPrices, setSymbolPrices] = useState<Record<string, number>>({
    "BTC-USD": 68450.0,
    "ETH-USD": 3520.0,
    "SOL-USD": 178.5,
    SPY: 512.3,
    QQQ: 441.8,
    NVDA: 128.4,
  });

  const [latestTicks, setLatestTicks] = useState<SimulatedTick[]>([]);
  const [lakeFilesCount, setLakeFilesCount] = useState(42);
  const [parquetFilesCount, setParquetFilesCount] = useState(14);
  const [lastCompactionRatio, setLastCompactionRatio] = useState(4.82);

  // Drift metrics
  const [driftStats, setDriftStats] = useState({
    spreadWasserstein: 0.024,
    spreadJS: 0.031,
    volWasserstein: 0.018,
    volJS: 0.022,
    obiWasserstein: 0.035,
    obiJS: 0.041,
    hasDrift: false,
  });

  // Event logs
  const [logs, setLogs] = useState<LogEntry[]>([
    {
      id: "1",
      time: "21:04:15",
      level: "INFO",
      component: "main",
      message: "ChronosLake Orchestrator online. Author: Rayaan tasnim, 13 aged prodigy",
    },
    {
      id: "2",
      time: "21:04:16",
      level: "INFO",
      component: "harvester",
      message: "Virtual connection pool established to Kafka cluster (6 partitions).",
    },
    {
      id: "3",
      time: "21:04:18",
      level: "INFO",
      component: "compactor",
      message: "ColumnarCompactionWorker daemon initialized with Snappy codec.",
    },
  ]);

  const logsEndRef = useRef<HTMLDivElement>(null);

  const addLog = (level: LogEntry["level"], component: string, message: string) => {
    const timeStr = new Date().toTimeString().split(" ")[0];
    setLogs((prev) => [
      ...prev.slice(-40),
      { id: Math.random().toString(), time: timeStr, level, component, message },
    ]);
  };

  // Main Simulation Loop
  useEffect(() => {
    if (!isRunning) return;

    const interval = setInterval(() => {
      // 1. Advance Prices
      const symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "SPY", "QQQ", "NVDA"];
      const currentSym = symbols[Math.floor(Math.random() * symbols.length)];
      const basePrice = symbolPrices[currentSym] || 100;
      const pctChange = (Math.random() - 0.495) * 0.002;
      const newPrice = Math.max(0.01, basePrice * (1 + pctChange));

      setSymbolPrices((prev) => ({ ...prev, [currentSym]: newPrice }));

      const halfSpread = newPrice * 0.00015;
      const bid = Number((newPrice - halfSpread).toFixed(2));
      const ask = Number((newPrice + halfSpread).toFixed(2));
      const bidSize = Number((Math.random() * 12 + 1).toFixed(4));
      const askSize = Number((Math.random() * 12 + 1).toFixed(4));
      const totalSize = bidSize + askSize;
      const obi = Number(((bidSize - askSize) / totalSize).toFixed(3));
      const microPrice = Number(((bid * askSize + ask * bidSize) / totalSize).toFixed(2));
      const spreadBps = Number((((ask - bid) / newPrice) * 10000).toFixed(2));
      const rollingVwap = Number((newPrice * (1 + (Math.random() - 0.5) * 0.0004)).toFixed(2));
      const rollingVol = Number((0.015 + Math.random() * 0.006).toFixed(4));

      const tickObj: SimulatedTick = {
        sequence_id: ticksCount + 1,
        timestamp: new Date().toISOString(),
        symbol: currentSym,
        exchange: currentSym.includes("USD") ? "BINANCE" : "NASDAQ",
        bid_price: bid,
        ask_price: ask,
        bid_size: bidSize,
        ask_size: askSize,
        mid_price: Number(newPrice.toFixed(2)),
        micro_price: microPrice,
        spread_bps: spreadBps,
        order_book_imbalance: obi,
        rolling_vwap: rollingVwap,
        rolling_volatility: rollingVol,
      };

      if (evolvedProperties.length > 0) {
        tickObj.extra_fields = {
          implied_volatility: Number((rollingVol * 1.8).toFixed(4)),
          order_flow_toxicity: Number((Math.abs(obi) * 0.85).toFixed(3)),
        };
      }

      setTicksCount((c) => c + 1);
      setLatestTicks((prev) => [tickObj, ...prev.slice(0, 14)]);

      // 2. Queue Simulation with Lag
      setQueueSize((curr) => {
        let newSize = curr;
        if (induceLag) {
          newSize = Math.min(queueCapacity, curr + 80);
          if (newSize >= critWatermark && Math.random() < 0.25) {
            addLog(
              "CRIT",
              "ring_queue",
              `CRITICAL BACKPRESSURE: Queue 98% full (${newSize}/${queueCapacity}). Pausing ingestion!`
            );
          } else if (newSize >= highWatermark && Math.random() < 0.2) {
            addLog(
              "WARN",
              "ring_queue",
              `HIGH WATERMARK: Queue depth ${newSize} (>80%). Throttling producers.`
            );
          }
        } else {
          // Normal draining
          newSize = Math.max(800, curr + (Math.random() > 0.5 ? -40 : 25));
        }
        return newSize;
      });

      // Periodic Lake File increment
      if (Math.random() < 0.15) {
        setLakeFilesCount((f) => f + 1);
      }

      // 3. Periodic Compaction check (every ~10s in sim)
      if (Math.random() < 0.05) {
        setParquetFilesCount((p) => p + 1);
        setLakeFilesCount((f) => Math.max(12, f - 4));
        const ratio = Number((4.5 + Math.random() * 0.8).toFixed(2));
        setLastCompactionRatio(ratio);
        addLog(
          "SUCCESS",
          "compactor",
          `Compacted partition /symbol=${currentSym}/ into Parquet. Snappy ratio: ${ratio}x`
        );
      }

      // Random proxy rotation every ~40 ticks
      if (Math.random() < 0.03) {
        const proxies = [
          "http://proxy-us-east.edge.internal:8080",
          "http://proxy-eu-central.edge.internal:8080",
          "http://proxy-ap-tokyo.edge.internal:8080",
          "http://proxy-us-west.edge.internal:8080",
          "http://proxy-uk-london.edge.internal:8080",
        ];
        const nextProxy = proxies[Math.floor(Math.random() * proxies.length)];
        setActiveProxy(nextProxy);
      }
    }, 180);

    return () => clearInterval(interval);
  }, [isRunning, ticksCount, induceLag, evolvedProperties, symbolPrices]);

  // Trigger Volatility Shock for SciPy Drift Demonstration
  const triggerVolatilityShock = () => {
    addLog("WARN", "drift_auditor", "Simulating massive volatility shock & liquidity drain...");
    setDriftStats({
      spreadWasserstein: 0.142,
      spreadJS: 0.185,
      volWasserstein: 0.168,
      volJS: 0.194,
      obiWasserstein: 0.125,
      obiJS: 0.152,
      hasDrift: true,
    });
    addLog(
      "CRIT",
      "drift_auditor",
      "DISTRIBUTION DRIFT DETECTED via SciPy Wasserstein Distance (W=0.168 > 0.080)!"
    );

    setTimeout(() => {
      setDriftStats({
        spreadWasserstein: 0.026,
        spreadJS: 0.033,
        volWasserstein: 0.021,
        volJS: 0.025,
        obiWasserstein: 0.038,
        obiJS: 0.042,
        hasDrift: false,
      });
      addLog("INFO", "drift_auditor", "Distribution normalized. Moving reservoir updated.");
    }, 6000);
  };

  // Trigger Dynamic Schema Evolution
  const triggerDynamicSchemaEvolution = () => {
    if (schemaVersion === "1.0.0") {
      setSchemaVersion("1.1.0");
      setEvolvedProperties(["implied_volatility:number", "order_flow_toxicity:number"]);
      addLog(
        "SUCCESS",
        "schema_registry",
        "DYNAMIC SCHEMA EVOLUTION EVENT: Evolved schema to v1.1.0. Absorbed: ['implied_volatility', 'order_flow_toxicity'] without crashing pipeline."
      );
    } else {
      setSchemaVersion("1.2.0");
      setEvolvedProperties((prev) => [
        ...prev,
        "synthetic_liquidity_score:number",
        "dark_pool_fill_ratio:number",
      ]);
      addLog(
        "SUCCESS",
        "schema_registry",
        "DYNAMIC SCHEMA EVOLUTION EVENT: Evolved schema to v1.2.0. Dynamically registered: ['synthetic_liquidity_score', 'dark_pool_fill_ratio']."
      );
    }
  };

  const currentTick = latestTicks[0] || {
    symbol: "BTC-USD",
    bid_price: 68445.0,
    ask_price: 68455.0,
    bid_size: 4.85,
    ask_size: 3.12,
    mid_price: 68450.0,
    micro_price: 68451.2,
    spread_bps: 1.46,
    order_book_imbalance: 0.217,
    rolling_vwap: 68448.5,
    rolling_volatility: 0.0182,
  };

  const queuePct = Math.round((queueSize / queueCapacity) * 100);

  return (
    <div className="space-y-6">
      {/* Control Banner & Quick Actions */}
      <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 shadow-xl flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="p-2.5 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            <Activity className="w-5 h-5 animate-pulse" />
          </div>
          <div>
            <h2 className="text-base font-semibold text-slate-100 flex items-center gap-2">
              Real-Time Financial Market Stream Simulator
              <span className="text-[11px] font-mono px-2 py-0.5 rounded bg-emerald-950 text-emerald-300 border border-emerald-800">
                Live Engine Active
              </span>
            </h2>
            <p className="text-xs text-slate-400">
              High-Frequency Harvester &bull; Ring Backpressure &bull; Vectorized Features &bull; Parquet Lake
            </p>
          </div>
        </div>

        {/* Buttons */}
        <div className="flex flex-wrap items-center gap-2.5">
          <button
            onClick={() => setIsRunning(!isRunning)}
            className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition shadow-sm ${
              isRunning
                ? "bg-amber-500/20 text-amber-300 border border-amber-500/30 hover:bg-amber-500/30"
                : "bg-emerald-500 text-slate-950 font-bold hover:bg-emerald-400"
            }`}
          >
            {isRunning ? <Pause className="w-3.5 h-3.5" /> : <Play className="w-3.5 h-3.5" />}
            {isRunning ? "Pause Stream" : "Resume Stream"}
          </button>

          <button
            onClick={() => setInduceLag(!induceLag)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition ${
              induceLag
                ? "bg-red-500/20 text-red-300 border border-red-500/40 animate-pulse"
                : "bg-slate-800 text-slate-300 border border-slate-700 hover:bg-slate-700"
            }`}
          >
            <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
            {induceLag ? "Release Consumer Lag" : "Induce Consumer Lag"}
          </button>

          <button
            onClick={triggerVolatilityShock}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition"
          >
            <Flame className="w-3.5 h-3.5 text-orange-400" />
            Trigger Volatility Shock
          </button>

          <button
            onClick={triggerDynamicSchemaEvolution}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-gradient-to-r from-teal-600 to-cyan-600 hover:from-teal-500 hover:to-cyan-500 text-white shadow transition"
          >
            <Sparkles className="w-3.5 h-3.5" />
            Evolve Schema (+Fields)
          </button>
        </div>
      </div>

      {/* Top Telemetry Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
        <div className="p-3.5 rounded-xl bg-slate-900/90 border border-slate-800">
          <div className="text-[11px] uppercase font-mono text-slate-400 flex items-center justify-between">
            <span>Throughput</span>
            <Radio className="w-3.5 h-3.5 text-emerald-400 animate-pulse" />
          </div>
          <div className="mt-1 text-xl font-bold font-mono text-emerald-400">
            {isRunning ? "2,480" : "0"} <span className="text-xs font-normal text-slate-400">msg/s</span>
          </div>
          <div className="text-[10px] text-slate-500 font-mono mt-0.5">
            Total: {ticksCount.toLocaleString()} ticks
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900/90 border border-slate-800">
          <div className="text-[11px] uppercase font-mono text-slate-400 flex items-center justify-between">
            <span>Active Proxy</span>
            <Globe className="w-3.5 h-3.5 text-cyan-400" />
          </div>
          <div className="mt-1 text-xs font-mono font-medium text-slate-200 truncate">
            {activeProxy.split(".")[0].replace("http://", "")}
          </div>
          <div className="text-[10px] text-emerald-400 font-mono mt-0.5">
            Token-Bucket: 5,000 burst
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900/90 border border-slate-800">
          <div className="text-[11px] uppercase font-mono text-slate-400 flex items-center justify-between">
            <span>Ring Buffer</span>
            <Cpu className="w-3.5 h-3.5 text-indigo-400" />
          </div>
          <div
            className={`mt-1 text-xl font-bold font-mono ${
              queuePct > 90 ? "text-red-400" : queuePct > 75 ? "text-amber-400" : "text-slate-100"
            }`}
          >
            {queuePct}% <span className="text-xs font-normal text-slate-400">capacity</span>
          </div>
          <div className="text-[10px] text-slate-500 font-mono mt-0.5">
            Depth: {queueSize.toLocaleString()} / {queueCapacity.toLocaleString()}
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900/90 border border-slate-800">
          <div className="text-[11px] uppercase font-mono text-slate-400 flex items-center justify-between">
            <span>Lake Partitions</span>
            <Database className="w-3.5 h-3.5 text-purple-400" />
          </div>
          <div className="mt-1 text-xl font-bold font-mono text-slate-100">
            {lakeFilesCount} <span className="text-xs font-normal text-slate-400">staged</span>
          </div>
          <div className="text-[10px] text-slate-500 font-mono mt-0.5">
            Format: year/month/day/sym
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900/90 border border-slate-800">
          <div className="text-[11px] uppercase font-mono text-slate-400 flex items-center justify-between">
            <span>Parquet Compaction</span>
            <FileSpreadsheet className="w-3.5 h-3.5 text-emerald-400" />
          </div>
          <div className="mt-1 text-xl font-bold font-mono text-emerald-400">
            {lastCompactionRatio}x <span className="text-xs font-normal text-slate-400">ratio</span>
          </div>
          <div className="text-[10px] text-slate-500 font-mono mt-0.5">
            {parquetFilesCount} columnar files
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900/90 border border-slate-800">
          <div className="text-[11px] uppercase font-mono text-slate-400 flex items-center justify-between">
            <span>Schema Registry</span>
            <Sparkles className="w-3.5 h-3.5 text-teal-400" />
          </div>
          <div className="mt-1 text-xl font-bold font-mono text-teal-300">
            v{schemaVersion}
          </div>
          <div className="text-[10px] text-slate-400 font-mono mt-0.5 truncate">
            {evolvedProperties.length > 0
              ? `+${evolvedProperties.length} evolved fields`
              : "Baseline transitive"}
          </div>
        </div>
      </div>

      {/* Main 2-Column Grid: Pipeline Flow + Vectorized Analytics */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Left Column (7 cols): End-to-End Pipeline Visualization */}
        <div className="lg:col-span-7 space-y-4">
          <div className="p-5 rounded-xl bg-slate-900 border border-slate-800">
            <h3 className="text-sm font-semibold text-slate-200 mb-3 flex items-center justify-between">
              <span className="flex items-center gap-2">
                <Layers className="w-4 h-4 text-emerald-400" />
                Architectural Data Pipeline &amp; Buffer Topology
              </span>
              <span className="text-xs text-slate-500 font-mono">End-to-End Latency: &lt;1.2ms</span>
            </h3>

            {/* Pipeline Stage Blocks */}
            <div className="space-y-3">
              {/* Stage 1: Harvester */}
              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-blue-500/10 text-blue-400 flex items-center justify-center font-mono text-xs font-bold border border-blue-500/20">
                    S1
                  </div>
                  <div>
                    <div className="text-xs font-semibold text-slate-200">
                      Asynchronous Market Harvester
                    </div>
                    <div className="text-[11px] text-slate-400 font-mono">
                      Token-Bucket Limiter (2.5k tps) &bull; Rotating Proxy Pool
                    </div>
                  </div>
                </div>
                <div className="text-right font-mono text-xs text-emerald-400">
                  HEALTHY
                </div>
              </div>

              {/* Arrow */}
              <div className="flex justify-center text-slate-600 text-xs">
                &darr; Async Non-Blocking Dispatch
              </div>

              {/* Stage 2: Ring Backpressure Queue */}
              <div
                className={`p-3.5 rounded-lg border transition ${
                  queuePct >= 95
                    ? "bg-red-950/30 border-red-500/60"
                    : queuePct >= 80
                    ? "bg-amber-950/30 border-amber-500/50"
                    : "bg-slate-950/70 border-slate-800"
                }`}
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 rounded-lg bg-indigo-500/10 text-indigo-400 flex items-center justify-center font-mono text-xs font-bold border border-indigo-500/20">
                      S2
                    </div>
                    <div>
                      <div className="text-xs font-semibold text-slate-200 flex items-center gap-2">
                        Bound-Ring Backpressure Queue
                        {queuePct >= 95 && (
                          <span className="text-[10px] font-bold px-1.5 py-0.2 rounded bg-red-900 text-red-200">
                            HALT INGESTION
                          </span>
                        )}
                        {queuePct >= 80 && queuePct < 95 && (
                          <span className="text-[10px] font-bold px-1.5 py-0.2 rounded bg-amber-900 text-amber-200">
                            THROTTLE ACTIVE
                          </span>
                        )}
                      </div>
                      <div className="text-[11px] text-slate-400 font-mono">
                        Thread-Safe Condition Variables &bull; High: 80% &bull; Critical: 95%
                      </div>
                    </div>
                  </div>
                  <div className="font-mono text-xs font-semibold text-slate-300">
                    {queueSize} / {queueCapacity}
                  </div>
                </div>

                {/* Visual Depth Bar */}
                <div className="relative w-full h-3 bg-slate-900 rounded-full overflow-hidden border border-slate-800">
                  {/* High watermark marker */}
                  <div className="absolute top-0 bottom-0 left-[80%] w-0.5 bg-amber-400 z-10" />
                  {/* Critical watermark marker */}
                  <div className="absolute top-0 bottom-0 left-[95%] w-0.5 bg-red-400 z-10" />
                  {/* Progress fill */}
                  <div
                    className={`h-full transition-all duration-300 ${
                      queuePct >= 95
                        ? "bg-gradient-to-r from-amber-500 to-red-500"
                        : queuePct >= 80
                        ? "bg-gradient-to-r from-emerald-500 to-amber-500"
                        : "bg-gradient-to-r from-emerald-600 to-teal-500"
                    }`}
                    style={{ width: `${queuePct}%` }}
                  />
                </div>
                <div className="flex justify-between text-[10px] text-slate-500 font-mono mt-1">
                  <span>0% (Empty)</span>
                  <span className="text-amber-400">80% High WM</span>
                  <span className="text-red-400">95% Crit WM</span>
                  <span>100% (Drop)</span>
                </div>
              </div>

              {/* Arrow */}
              <div className="flex justify-center text-slate-600 text-xs">
                &darr; Vectorized Micro-Batch Drain (500 items/batch)
              </div>

              {/* Stage 3: Feature Generator */}
              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-teal-500/10 text-teal-400 flex items-center justify-center font-mono text-xs font-bold border border-teal-500/20">
                    S3
                  </div>
                  <div>
                    <div className="text-xs font-semibold text-slate-200">
                      Vectorized NumPy / Pandas Feature Engine
                    </div>
                    <div className="text-[11px] text-slate-400 font-mono">
                      Rolling VWAP &bull; Annualized Realized Volatility &bull; OBI Matrix
                    </div>
                  </div>
                </div>
                <div className="text-right font-mono text-xs text-teal-400">
                  0.34ms Calc
                </div>
              </div>

              {/* Arrow */}
              <div className="flex justify-center text-slate-600 text-xs">
                &darr; Partitioned Write + Compaction
              </div>

              {/* Stage 4: Lake Sink & Compaction */}
              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-purple-500/10 text-purple-400 flex items-center justify-center font-mono text-xs font-bold border border-purple-500/20">
                    S4
                  </div>
                  <div>
                    <div className="text-xs font-semibold text-slate-200">
                      Time-Partitioned Lake &amp; Columnar Compactor
                    </div>
                    <div className="text-[11px] text-slate-400 font-mono">
                      year=2026/month=10/day=18/ &rarr; PyArrow Snappy Parquet
                    </div>
                  </div>
                </div>
                <div className="text-right font-mono text-xs text-purple-400">
                  {lastCompactionRatio}x Compacted
                </div>
              </div>
            </div>
          </div>

          {/* SciPy Drift Auditor Panel */}
          <div
            className={`p-4 rounded-xl border transition ${
              driftStats.hasDrift
                ? "bg-red-950/20 border-red-500/50"
                : "bg-slate-900 border-slate-800"
            }`}
          >
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <ShieldAlert
                  className={`w-4 h-4 ${driftStats.hasDrift ? "text-red-400 animate-bounce" : "text-emerald-400"}`}
                />
                <h3 className="text-xs font-semibold text-slate-200">
                  Data Quality &amp; Statistical Drift Gatekeeper (SciPy)
                </h3>
              </div>
              <span
                className={`text-[10px] font-mono px-2 py-0.5 rounded font-bold ${
                  driftStats.hasDrift
                    ? "bg-red-900 text-red-200 border border-red-700"
                    : "bg-emerald-950 text-emerald-300 border border-emerald-800"
                }`}
              >
                {driftStats.hasDrift ? "ALERT: DRIFT DETECTED" : "DISTRIBUTIONS STABLE"}
              </span>
            </div>

            <div className="grid grid-cols-3 gap-2.5 text-xs font-mono">
              <div className="p-2.5 rounded bg-slate-950 border border-slate-800">
                <div className="text-[10px] text-slate-400">Spread Bps (W-Dist)</div>
                <div
                  className={`text-sm font-bold ${
                    driftStats.spreadWasserstein > 0.08 ? "text-red-400" : "text-slate-200"
                  }`}
                >
                  {driftStats.spreadWasserstein.toFixed(4)}
                </div>
                <div className="text-[9px] text-slate-500">Threshold: 0.080</div>
              </div>

              <div className="p-2.5 rounded bg-slate-950 border border-slate-800">
                <div className="text-[10px] text-slate-400">Realized Vol (W-Dist)</div>
                <div
                  className={`text-sm font-bold ${
                    driftStats.volWasserstein > 0.08 ? "text-red-400" : "text-slate-200"
                  }`}
                >
                  {driftStats.volWasserstein.toFixed(4)}
                </div>
                <div className="text-[9px] text-slate-500">Threshold: 0.080</div>
              </div>

              <div className="p-2.5 rounded bg-slate-950 border border-slate-800">
                <div className="text-[10px] text-slate-400">OBI JS-Divergence</div>
                <div
                  className={`text-sm font-bold ${
                    driftStats.obiJS > 0.12 ? "text-red-400" : "text-slate-200"
                  }`}
                >
                  {driftStats.obiJS.toFixed(4)}
                </div>
                <div className="text-[9px] text-slate-500">Limit: 0.120</div>
              </div>
            </div>
          </div>
        </div>

        {/* Right Column (5 cols): Live Asset Inspection & Vectorized Metrics */}
        <div className="lg:col-span-5 space-y-4">
          {/* Active Asset Selector */}
          <div className="p-4 rounded-xl bg-slate-900 border border-slate-800">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                <TrendingUp className="w-4 h-4 text-emerald-400" />
                Live Calculated Feature Inspection
              </h3>
              <div className="flex gap-1">
                {["BTC-USD", "ETH-USD", "SOL-USD", "SPY", "NVDA"].map((sym) => (
                  <button
                    key={sym}
                    onClick={() => setActiveSymbol(sym)}
                    className={`px-2 py-1 text-[10px] font-mono rounded transition ${
                      activeSymbol === sym
                        ? "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40"
                        : "bg-slate-800 text-slate-400 hover:text-slate-200"
                    }`}
                  >
                    {sym}
                  </button>
                ))}
              </div>
            </div>

            {/* Price & Vectorized Metrics */}
            <div className="p-3.5 rounded-lg bg-slate-950 border border-slate-800 space-y-3">
              <div className="flex items-baseline justify-between">
                <div>
                  <span className="text-2xl font-bold font-mono text-slate-100">
                    ${currentTick.mid_price.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </span>
                  <span className="ml-2 text-xs font-mono text-emerald-400">
                    Micro: ${currentTick.micro_price.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </span>
                </div>
                <span className="text-[10px] font-mono text-slate-500">
                  {currentTick.exchange} &bull; seq #{currentTick.sequence_id}
                </span>
              </div>

              {/* Order Book Level 1 Depth Bar */}
              <div>
                <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
                  <span className="text-emerald-400">Bid: {currentTick.bid_size} @ ${currentTick.bid_price}</span>
                  <span className="text-red-400">Ask: {currentTick.ask_size} @ ${currentTick.ask_price}</span>
                </div>
                <div className="flex h-2.5 rounded-full overflow-hidden bg-slate-900 border border-slate-800">
                  <div
                    className="bg-emerald-500 transition-all duration-200"
                    style={{
                      width: `${(currentTick.bid_size / (currentTick.bid_size + currentTick.ask_size)) * 100}%`,
                    }}
                  />
                  <div
                    className="bg-red-500 transition-all duration-200"
                    style={{
                      width: `${(currentTick.ask_size / (currentTick.bid_size + currentTick.ask_size)) * 100}%`,
                    }}
                  />
                </div>
              </div>

              {/* Order Book Imbalance (OBI) Gauge */}
              <div className="pt-1">
                <div className="flex justify-between text-[10px] font-mono text-slate-400">
                  <span>Order Book Imbalance (OBI)</span>
                  <span
                    className={`font-bold ${
                      currentTick.order_book_imbalance > 0 ? "text-emerald-400" : "text-red-400"
                    }`}
                  >
                    {currentTick.order_book_imbalance > 0 ? "+" : ""}
                    {currentTick.order_book_imbalance}
                  </span>
                </div>
                <div className="relative w-full h-2 bg-slate-800 rounded-full mt-1 overflow-hidden">
                  <div className="absolute top-0 bottom-0 left-1/2 w-0.5 bg-slate-500" />
                  <div
                    className={`h-full absolute top-0 bottom-0 ${
                      currentTick.order_book_imbalance >= 0
                        ? "left-1/2 bg-emerald-400"
                        : "right-1/2 bg-red-400"
                    }`}
                    style={{
                      width: `${Math.abs(currentTick.order_book_imbalance) * 50}%`,
                    }}
                  />
                </div>
              </div>

              {/* Micro Metrics Grid */}
              <div className="grid grid-cols-3 gap-2 pt-1 text-[11px] font-mono">
                <div className="p-2 rounded bg-slate-900 border border-slate-800/80">
                  <div className="text-[10px] text-slate-500">Spread Bps</div>
                  <div className="text-slate-200 font-semibold">{currentTick.spread_bps} bps</div>
                </div>

                <div className="p-2 rounded bg-slate-900 border border-slate-800/80">
                  <div className="text-[10px] text-slate-500">Rolling VWAP</div>
                  <div className="text-teal-400 font-semibold">${currentTick.rolling_vwap}</div>
                </div>

                <div className="p-2 rounded bg-slate-900 border border-slate-800/80">
                  <div className="text-[10px] text-slate-500">Annualized Vol</div>
                  <div className="text-purple-400 font-semibold">
                    {(currentTick.rolling_volatility * 100).toFixed(2)}%
                  </div>
                </div>
              </div>

              {/* Dynamic Evolved Attributes if Present */}
              {evolvedProperties.length > 0 && (
                <div className="p-2.5 rounded bg-teal-950/30 border border-teal-500/30 space-y-1 text-[11px] font-mono">
                  <div className="text-[10px] text-teal-400 font-bold flex items-center gap-1">
                    <Sparkles className="w-3 h-3" />
                    DYNAMIC SCHEMA FIELDS (v{schemaVersion}):
                  </div>
                  <div className="text-slate-300">
                    &bull; Implied Vol: <span className="text-teal-300">{(currentTick.rolling_volatility * 1.8).toFixed(4)}</span>
                  </div>
                  <div className="text-slate-300">
                    &bull; Toxicity Score: <span className="text-teal-300">{(Math.abs(currentTick.order_book_imbalance) * 0.85).toFixed(3)}</span>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Real-Time Framework Logs */}
          <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 flex flex-col h-60">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                <Server className="w-4 h-4 text-emerald-400" />
                Live Engine Execution Log
              </h3>
              <span className="text-[10px] font-mono text-slate-500">stdout</span>
            </div>
            <div className="flex-1 overflow-y-auto font-mono text-[11px] space-y-1.5 p-2 rounded bg-slate-950 border border-slate-800/80 text-slate-300">
              {logs.map((log) => (
                <div key={log.id} className="leading-tight">
                  <span className="text-slate-600">[{log.time}]</span>{" "}
                  <span
                    className={
                      log.level === "CRIT"
                        ? "text-red-400 font-bold"
                        : log.level === "WARN"
                        ? "text-amber-400 font-bold"
                        : log.level === "SUCCESS"
                        ? "text-emerald-400 font-bold"
                        : "text-blue-400"
                    }
                  >
                    [{log.level}]
                  </span>{" "}
                  <span className="text-slate-400">[{log.component}]</span> {log.message}
                </div>
              ))}
              <div ref={logsEndRef} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
