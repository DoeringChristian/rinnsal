import { useState, useEffect, useMemo } from "react";
import { fetchFlows, FlowInfo } from "../lib/api";

interface FlowSidebarProps {
  rootDir: string;
  selectedFlow: string | null;
  onSelectFlow: (flow: string | null) => void;
  refreshKey?: number;
}

export default function FlowSidebar({
  rootDir,
  selectedFlow,
  onSelectFlow,
  refreshKey = 0,
}: FlowSidebarProps) {
  const [flows, setFlows] = useState<FlowInfo[]>([]);
  const [filter, setFilter] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!rootDir) {
      setFlows([]);
      return;
    }
    setIsLoading(true);
    setError(null);
    fetchFlows(rootDir)
      .then(setFlows)
      .catch((e) => {
        console.error("Failed to fetch flows:", e);
        setError(String(e));
      })
      .finally(() => setIsLoading(false));
  }, [rootDir, refreshKey]);

  const filteredFlows = useMemo(() => {
    if (!filter) return flows;
    try {
      const regex = new RegExp(filter, "i");
      return flows.filter((f) => regex.test(f.name));
    } catch {
      return flows;
    }
  }, [flows, filter]);

  if (!rootDir) {
    return <p className="text-sm text-gray-500">Enter a directory path above.</p>;
  }

  if (isLoading) {
    return <p className="text-sm text-gray-500">Loading flows...</p>;
  }

  if (error) {
    return <p className="text-sm text-red-500">{error}</p>;
  }

  if (flows.length === 0) {
    return <p className="text-sm text-gray-500">No flows found.</p>;
  }

  return (
    <div className="space-y-3">
      <div>
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter flows"
          className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      <p className="text-xs text-gray-500">
        {filteredFlows.length}/{flows.length} flows
      </p>

      <div className="space-y-0.5">
        {filteredFlows.map((flow) => {
          const isSelected = flow.name === selectedFlow;
          return (
            <button
              key={flow.name}
              onClick={() => onSelectFlow(isSelected ? null : flow.name)}
              className={`w-full text-left px-2 py-1.5 rounded text-sm transition-colors ${
                isSelected
                  ? "bg-blue-100 text-blue-700 font-medium"
                  : "hover:bg-gray-100 text-gray-800"
              }`}
              title={flow.name}
            >
              <div className="flex items-center justify-between gap-2">
                <span className="break-all leading-tight flex-1">{flow.name}</span>
                <span className="text-xs text-gray-500 shrink-0">
                  {flow.run_count} run{flow.run_count === 1 ? "" : "s"}
                </span>
              </div>
              <div className="text-xs text-gray-400">
                {flow.nodes.length} task{flow.nodes.length === 1 ? "" : "s"}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
