import React, { useState } from "react";
import {
  Activity,
  Code2,
  FileCode2,
  BookOpen,
  Sparkles,
  ShieldCheck,
  Cpu,
  Layers,
  Database,
  Terminal,
  ExternalLink,
  Zap,
} from "lucide-react";
import { PipelineSimulator } from "./components/PipelineSimulator";
import { CodeExplorer } from "./components/CodeExplorer";
import { ArchitectureBlueprint } from "./components/ArchitectureBlueprint";
import { SchemaRegistryViewer } from "./components/SchemaRegistryViewer";

export default function App() {
  const [activeTab, setActiveTab] = useState<"simulator" | "code" | "blueprint" | "schema">(
    "simulator"
  );

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans selection:bg-emerald-800 selection:text-white">
      {/* Top Enterprise Navigation */}
      <header className="sticky top-0 z-50 bg-slate-950/80 backdrop-blur-md border-b border-slate-800/80">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between gap-4">
          {/* Logo & Author Badge */}
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-emerald-500 via-teal-400 to-cyan-500 flex items-center justify-center text-slate-950 shadow-lg shadow-emerald-500/20 font-bold">
              <Zap className="w-6 h-6 fill-current" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-bold text-base tracking-tight text-white">ChronosLake</span>
                <span className="text-[10px] font-mono px-2 py-0.5 rounded-full bg-emerald-950 text-emerald-300 border border-emerald-800 font-semibold">
                  v1.0.0
                </span>
                <span className="text-[11px] font-mono px-2 py-0.5 rounded-full bg-slate-900 text-teal-300 border border-teal-800 hidden sm:inline">
                  Author: Rayaan tasnim, 13 aged prodigy
                </span>
              </div>
              <p className="text-xs text-slate-400 hidden sm:block">
                Real-Time Financial Data Lake &amp; Feature Store with Dynamic Schema Evolution
              </p>
            </div>
          </div>

          {/* Nav Tabs */}
          <nav className="flex items-center gap-1 bg-slate-900/90 p-1 rounded-xl border border-slate-800 text-xs">
            <button
              onClick={() => setActiveTab("simulator")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-medium transition ${
                activeTab === "simulator"
                  ? "bg-emerald-500 text-slate-950 font-bold shadow-sm"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <Activity className="w-3.5 h-3.5" />
              <span>Stream Simulator</span>
            </button>

            <button
              onClick={() => setActiveTab("code")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-medium transition ${
                activeTab === "code"
                  ? "bg-emerald-500 text-slate-950 font-bold shadow-sm"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <FileCode2 className="w-3.5 h-3.5" />
              <span>Codebase (11 Files)</span>
            </button>

            <button
              onClick={() => setActiveTab("blueprint")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-medium transition ${
                activeTab === "blueprint"
                  ? "bg-emerald-500 text-slate-950 font-bold shadow-sm"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <BookOpen className="w-3.5 h-3.5" />
              <span>Blueprint &amp; Math</span>
            </button>

            <button
              onClick={() => setActiveTab("schema")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-medium transition ${
                activeTab === "schema"
                  ? "bg-emerald-500 text-slate-950 font-bold shadow-sm"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <Sparkles className="w-3.5 h-3.5" />
              <span>Schema Registry</span>
            </button>
          </nav>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-6">
        {activeTab === "simulator" && <PipelineSimulator />}
        {activeTab === "code" && <CodeExplorer />}
        {activeTab === "blueprint" && <ArchitectureBlueprint />}
        {activeTab === "schema" && <SchemaRegistryViewer />}
      </main>

      {/* Enterprise Status Footer */}
      <footer className="border-t border-slate-900 bg-slate-950 text-slate-500 text-xs py-4 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-3 font-mono text-[11px]">
          <div className="flex items-center gap-4">
            <span className="text-slate-400 flex items-center gap-1">
              <ShieldCheck className="w-3.5 h-3.5 text-emerald-400" />
              Zero Hardcoded Secrets &bull; os.environ.get() enforced
            </span>
            <span className="text-slate-600 hidden md:inline">|</span>
            <span className="text-slate-400 hidden md:inline">
              Author: Rayaan tasnim, 13 aged prodigy
            </span>
          </div>

          <div className="flex items-center gap-3">
            <span className="px-2 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-400">
              Python &gt;= 3.10
            </span>
            <span className="px-2 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-400">
              PyArrow Parquet
            </span>
            <span className="px-2 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-400">
              SciPy Wasserstein
            </span>
            <span className="px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800 font-semibold">
              Ready for Production
            </span>
          </div>
        </div>
      </footer>
    </div>
  );
}
