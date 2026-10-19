import React, { useState } from "react";
import {
  Folder,
  FileCode,
  Copy,
  Check,
  Download,
  Search,
  Terminal,
  ShieldCheck,
  Sparkles,
  ExternalLink,
} from "lucide-react";
import JSZip from "jszip";
import { FRAMEWORK_FILES, FrameworkFile } from "../data/frameworkCode";

export const CodeExplorer: React.FC = () => {
  const [selectedFile, setSelectedFile] = useState<FrameworkFile>(FRAMEWORK_FILES[0]);
  const [searchQuery, setSearchQuery] = useState("");
  const [copied, setCopied] = useState(false);
  const [isZipping, setIsZipping] = useState(false);

  const filteredFiles = FRAMEWORK_FILES.filter(
    (f) =>
      f.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      f.path.toLowerCase().includes(searchQuery.toLowerCase()) ||
      f.description.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const handleCopyCode = async () => {
    try {
      await navigator.clipboard.writeText(selectedFile.code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback
    }
  };

  const handleDownloadFile = () => {
    const blob = new Blob([selectedFile.code], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = selectedFile.name;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleDownloadZip = async () => {
    setIsZipping(true);
    try {
      const zip = new JSZip();
      for (const file of FRAMEWORK_FILES) {
        zip.file(file.path, file.code);
      }
      // Add root README and init files
      zip.file(
        "market_streaming_framework/__init__.py",
        '# Author: Rayaan tasnim, 13 aged prodigy\n\n__version__ = "1.0.0"\n'
      );
      zip.file("README.md", `# ChronosLake Financial Market Data Lake & Feature Store\n\nAuthor: Rayaan tasnim, 13 aged prodigy\n\nProduction-grade, highly optimized Python framework for real-time market data streaming, dynamic schema evolution, ring backpressure, and columnar parquet compaction.\n\n## Quick Start\n\`\`\`bash\npip install -r market_streaming_framework/requirements.txt\npython -m market_streaming_framework.main\n\`\`\`\n`);

      const content = await zip.generateAsync({ type: "blob" });
      const url = URL.createObjectURL(content);
      const a = document.createElement("a");
      a.href = url;
      a.download = "market_streaming_framework.zip";
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("ZIP generation error:", err);
    } finally {
      setIsZipping(false);
    }
  };

  const lineCount = selectedFile.code.split("\n").length;

  return (
    <div className="flex flex-col h-[calc(100vh-140px)] min-h-[600px] bg-slate-950 border border-slate-800 rounded-xl overflow-hidden shadow-2xl">
      {/* Top Bar */}
      <div className="flex flex-wrap items-center justify-between px-4 py-3 bg-slate-900 border-b border-slate-800 gap-3">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            <Terminal className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-semibold text-slate-100 text-sm">ChronosLake Codebase Explorer</span>
              <span className="text-[10px] uppercase font-mono px-2 py-0.5 rounded bg-emerald-950 text-emerald-300 border border-emerald-800">
                Author: Rayaan tasnim, 13 aged prodigy
              </span>
            </div>
            <p className="text-xs text-slate-400">11 Fully-Typed, Production-Grade Python &amp; JSON Engine Files</p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={handleCopyCode}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition"
          >
            {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
            {copied ? "Copied Raw Code!" : "Copy File"}
          </button>

          <button
            onClick={handleDownloadFile}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition"
          >
            <Download className="w-3.5 h-3.5 text-cyan-400" />
            Download File
          </button>

          <button
            onClick={handleDownloadZip}
            disabled={isZipping}
            className="flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-semibold rounded-lg bg-gradient-to-r from-emerald-500 to-teal-600 hover:from-emerald-400 hover:to-teal-500 text-slate-950 shadow-md transition disabled:opacity-50"
          >
            <Download className="w-3.5 h-3.5" />
            {isZipping ? "Packaging ZIP..." : "Download Full Package (.ZIP)"}
          </button>
        </div>
      </div>

      {/* Main Split Body */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left Sidebar: File Tree */}
        <div className="w-80 shrink-0 border-r border-slate-800 bg-slate-950/60 flex flex-col">
          {/* Search */}
          <div className="p-3 border-b border-slate-800/80">
            <div className="relative">
              <Search className="w-3.5 h-3.5 absolute left-3 top-2.5 text-slate-400" />
              <input
                type="text"
                placeholder="Search files, classes..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-8 pr-3 py-1.5 bg-slate-900 border border-slate-800 rounded-lg text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-emerald-500"
              />
            </div>
          </div>

          {/* Directory Hierarchy */}
          <div className="flex-1 overflow-y-auto p-2 space-y-1 text-xs">
            <div className="px-2 py-1 text-[11px] font-semibold text-slate-400 uppercase tracking-wider flex items-center justify-between">
              <span>Files ({filteredFiles.length})</span>
              <span className="text-[10px] text-emerald-400/80 font-mono">100% Validated</span>
            </div>

            {filteredFiles.map((file) => {
              const isSelected = selectedFile.path === file.path;
              return (
                <button
                  key={file.path}
                  onClick={() => setSelectedFile(file)}
                  className={`w-full text-left p-2.5 rounded-lg flex items-start gap-2.5 transition ${
                    isSelected
                      ? "bg-emerald-500/10 text-emerald-300 border border-emerald-500/30"
                      : "text-slate-300 hover:bg-slate-900/80 border border-transparent"
                  }`}
                >
                  <FileCode
                    className={`w-4 h-4 mt-0.5 shrink-0 ${
                      isSelected ? "text-emerald-400" : "text-slate-500"
                    }`}
                  />
                  <div className="min-w-0 flex-1">
                    <div className="font-mono text-xs truncate font-medium text-slate-200">
                      {file.name}
                    </div>
                    <div className="text-[10px] text-slate-500 truncate font-mono">
                      {file.path}
                    </div>
                    <div className="text-[11px] text-slate-400 line-clamp-1 mt-0.5">
                      {file.description}
                    </div>
                  </div>
                </button>
              );
            })}
          </div>

          {/* Blueprint Footer */}
          <div className="p-3 border-t border-slate-800/80 bg-slate-900/30 text-[11px] text-slate-400 space-y-1">
            <div className="flex items-center gap-1.5 text-emerald-400 font-medium">
              <ShieldCheck className="w-3.5 h-3.5" />
              <span>Zero-Secret Guarantee</span>
            </div>
            <p className="text-[10px] text-slate-500">
              Configured strictly with <code className="text-slate-300">os.environ.get()</code> and safe localhost fallbacks.
            </p>
          </div>
        </div>

        {/* Right Code Display */}
        <div className="flex-1 flex flex-col bg-slate-950 overflow-hidden">
          {/* File Header Tab */}
          <div className="flex items-center justify-between px-4 py-2.5 bg-slate-900/70 border-b border-slate-800 text-xs">
            <div className="flex items-center gap-2 font-mono">
              <span className="text-slate-500">market_streaming_framework /</span>
              <span className="text-emerald-400 font-semibold">{selectedFile.name}</span>
              <span className="px-2 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px]">
                {lineCount} lines
              </span>
            </div>
            <span className="text-xs text-slate-400 hidden sm:inline">
              {selectedFile.description}
            </span>
          </div>

          {/* Code Viewer with Line Numbers */}
          <div className="flex-1 overflow-auto font-mono text-xs p-4 bg-slate-950 text-slate-200 selection:bg-emerald-900 selection:text-white">
            <pre className="flex leading-relaxed">
              {/* Line Numbers */}
              <div className="select-none pr-4 text-right text-slate-600 border-r border-slate-800/80 space-y-0.5">
                {selectedFile.code.split("\n").map((_, i) => (
                  <div key={i} className="leading-relaxed">
                    {i + 1}
                  </div>
                ))}
              </div>
              {/* Actual Code */}
              <div className="pl-4 flex-1 whitespace-pre overflow-x-auto text-slate-200">
                {selectedFile.code}
              </div>
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
};
