import { useState, useEffect, useMemo } from "react";
import { fetchFlows, fetchTaskHistory, FlowInfo, TaskHistoryEntry } from "../lib/api";

interface FlowSidebarProps {
  rootDir: string;
  selectedFlow: string | null;
  onSelectFlow: (flow: string | null) => void;
  onSelectRun?: (runPath: string, label: string) => void;
  refreshKey?: number;
}

const STATUS_DOT: Record<string, string> = {
  success: "bg-green-500",
  failed: "bg-red-500",
  cached: "bg-gray-400",
  running: "bg-yellow-500",
};

function formatRunId(name: string): string {
  const m = name.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/);
  if (!m) return name;
  return `${m[2]}-${m[3]} ${m[4]}:${m[5]}`;
}

export default function FlowSidebar({
  rootDir,
  selectedFlow,
  onSelectFlow,
  onSelectRun,
  refreshKey = 0,
}: FlowSidebarProps) {
  const [flows, setFlows] = useState<FlowInfo[]>([]);
  const [filter, setFilter] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [expandedTasks, setExpandedTasks] = useState<Set<string>>(new Set());
  const [taskHistories, setTaskHistories] = useState<
    Map<string, TaskHistoryEntry[]>
  >(new Map());

  useEffect(() => {
    if (!rootDir) {
      setFlows([]);
      return;
    }
    setIsLoading(true);
    fetchFlows(rootDir)
      .then(setFlows)
      .catch((e) => console.error("Failed to fetch flows:", e))
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

  const toggleTask = (flowName: string, taskName: string) => {
    const key = `${flowName}/${taskName}`;
    setExpandedTasks((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
        // Fetch history if not cached
        if (!taskHistories.has(key)) {
          fetchTaskHistory(rootDir, flowName, taskName)
            .then((h) =>
              setTaskHistories((prev) => new Map(prev).set(key, h))
            )
            .catch(() =>
              setTaskHistories((prev) => new Map(prev).set(key, []))
            );
        }
      }
      return next;
    });
  };

  if (!rootDir) {
    return (
      <p className="text-sm text-gray-500">Enter a directory path above.</p>
    );
  }

  if (isLoading) {
    return <p className="text-sm text-gray-500">Loading flows...</p>;
  }

  if (flows.length === 0) {
    return <p className="text-sm text-gray-500">No flows found.</p>;
  }

  return (
    <div className="space-y-2">
      <div>
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter flows"
          className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      <div className="space-y-0.5">
        {filteredFlows.map((flow) => {
          const isSelected = flow.name === selectedFlow;

          return (
            <div key={flow.name}>
              {/* Flow header */}
              <div className="flex items-center gap-1">
                <button
                  onClick={() =>
                    onSelectFlow(isSelected ? null : flow.name)
                  }
                  className={`flex-1 text-left px-2 py-1.5 rounded text-sm transition-colors ${
                    isSelected
                      ? "bg-blue-100 text-blue-700 font-medium"
                      : "hover:bg-gray-100 text-gray-800"
                  }`}
                  title={flow.name}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="break-all leading-tight flex-1">
                      {flow.name}
                    </span>
                    <span className="text-xs text-gray-500 shrink-0">
                      {flow.run_count}
                    </span>
                  </div>
                </button>
              </div>

              {/* Expanded: show tasks under the flow */}
              {isSelected && (
                <div className="ml-3 mt-0.5 space-y-0.5">
                  {flow.nodes.map((node) => {
                    const taskKey = `${flow.name}/${node.name}`;
                    const isTaskExpanded = expandedTasks.has(taskKey);
                    const history = taskHistories.get(taskKey) || [];

                    return (
                      <div key={node.name}>
                        <button
                          onClick={() =>
                            toggleTask(flow.name, node.name)
                          }
                          className="w-full text-left px-2 py-1 rounded text-xs hover:bg-gray-100 transition-colors flex items-center gap-1.5"
                        >
                          <span className="text-gray-400">
                            {isTaskExpanded ? "\u25BC" : "\u25B6"}
                          </span>
                          <span
                            className={`w-2 h-2 rounded-full shrink-0 ${
                              STATUS_DOT[node.status] || STATUS_DOT.cached
                            }`}
                          />
                          <span className="flex-1 truncate text-gray-700">
                            {node.name}
                          </span>
                          <span className="text-gray-400 shrink-0">
                            {node.run_count}
                          </span>
                        </button>

                        {/* Expanded: show runs for this task */}
                        {isTaskExpanded && (
                          <div className="ml-5 space-y-0.5 mt-0.5">
                            {history.length === 0 ? (
                              <div className="text-xs text-gray-400 px-2 py-0.5">
                                Loading...
                              </div>
                            ) : (
                              history.map((h) => (
                                <button
                                  key={h.run_id}
                                  onClick={() =>
                                    onSelectRun?.(
                                      h.run_path,
                                      `${flow.name} / ${node.name} / ${h.run_id}`
                                    )
                                  }
                                  className="w-full text-left px-2 py-0.5 rounded text-xs hover:bg-blue-50 hover:text-blue-700 transition-colors flex items-center gap-1.5"
                                >
                                  <span
                                    className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                                      STATUS_DOT[h.status] ||
                                      STATUS_DOT.cached
                                    }`}
                                  />
                                  <span className="text-gray-600">
                                    {formatRunId(h.run_id)}
                                  </span>
                                </button>
                              ))
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
