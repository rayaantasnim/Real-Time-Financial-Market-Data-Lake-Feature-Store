import React, { useState } from "react";
import { Sparkles, FileJson, CheckCircle2, ShieldCheck, Copy, Check } from "lucide-react";
import { FRAMEWORK_FILES } from "../data/frameworkCode";

export const SchemaRegistryViewer: React.FC = () => {
  const schemaFile = FRAMEWORK_FILES.find((f) => f.name === "schema_registry.json");
  const schemaObj = schemaFile ? JSON.parse(schemaFile.code) : {};
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    if (schemaFile) {
      await navigator.clipboard.writeText(schemaFile.code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const properties = schemaObj.properties || {};

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 flex flex-wrap items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-slate-100">Central Schema Registry</span>
            <span className="px-2 py-0.5 rounded bg-emerald-950 text-emerald-300 font-mono text-[10px] border border-emerald-800">
              v{schemaObj.version || "1.0.0"}
            </span>
            <span className="px-2 py-0.5 rounded bg-teal-950 text-teal-300 font-mono text-[10px] border border-teal-800">
              {schemaObj.compatibility || "BACKWARD_TRANSITIVE"}
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-1">
            Dynamic schema evolution registry with non-breaking field absorption and version tracking.
          </p>
        </div>

        <button
          onClick={handleCopy}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition"
        >
          {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
          {copied ? "Copied JSON!" : "Copy Schema JSON"}
        </button>
      </div>

      {/* Property Catalog Table */}
      <div className="p-5 rounded-xl bg-slate-900 border border-slate-800">
        <h3 className="text-xs font-semibold uppercase text-slate-400 tracking-wider mb-3">
          Property Definition Matrix ({Object.keys(properties).length} Registered Properties)
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="border-b border-slate-800 text-slate-400">
                <th className="pb-2">Field Name</th>
                <th className="pb-2">Type</th>
                <th className="pb-2">Requirement</th>
                <th className="pb-2">Description</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60">
              {Object.entries(properties).map(([fieldName, def]: [string, any]) => {
                const isRequired = (schemaObj.required || []).includes(fieldName);
                return (
                  <tr key={fieldName} className="hover:bg-slate-800/40">
                    <td className="py-2.5 text-emerald-300 font-semibold">{fieldName}</td>
                    <td className="py-2.5 text-cyan-400">{def.type || "any"}</td>
                    <td className="py-2.5">
                      {isRequired ? (
                        <span className="px-1.5 py-0.5 rounded bg-red-950/60 text-red-300 border border-red-800 text-[10px]">
                          REQUIRED
                        </span>
                      ) : (
                        <span className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 text-[10px]">
                          OPTIONAL / EVOLVED
                        </span>
                      )}
                    </td>
                    <td className="py-2.5 text-slate-300 font-sans text-xs">{def.description}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Raw Schema JSON Display */}
      <div className="p-5 rounded-xl bg-slate-900 border border-slate-800">
        <h3 className="text-xs font-semibold uppercase text-slate-400 tracking-wider mb-2 flex items-center gap-2">
          <FileJson className="w-4 h-4 text-emerald-400" />
          Raw Registry JSON Specification
        </h3>
        <pre className="p-4 rounded-lg bg-slate-950 font-mono text-xs text-slate-200 overflow-x-auto border border-slate-800 leading-relaxed">
          {schemaFile?.code}
        </pre>
      </div>
    </div>
  );
};
