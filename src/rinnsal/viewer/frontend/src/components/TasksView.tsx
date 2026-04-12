import { useState, useEffect, useMemo } from "react";
import { fetchFlows, FlowInfo, fetchTaskHistory, TaskHistoryEntry } from "../lib/api";

interface TasksViewProps {
  rootDir: string;
  refreshKey?: number;
  onSelectRun: (runPath: string, label: string) => void;
}

interface TaskSummary {
  name: string;
  flows: string[];
  latestStatus: string;
  latestDuration: number;
  latestTimestamp: number;
  totalRuns: number;
}

const STATUS_COLORS: Record<string, string> = {
  success: "bg-green-100 text-green-700",
  failed: "bg-red-100 text-red-700",
  cached: "bg-gray-100 text-gray-700",
  running: "bg-yellow-100 text-yellow-700",
};

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "\u2014";
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds - m * 60);
  return `${m}m${s}s`;
}

function formatTimestamp(ts: number): string {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleString();
}

export default function TasksView({ rootDir, refreshKey = 0, onSelectRun }: TasksViewProps) {
  const [flows, setFlows] = useState<FlowInfo[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [filter, setFilter] = useState("");
  const [expandedTask, setExpandedTask] = useState<string | null>(null);
  const [taskHistory, setTaskHistory] = useState<TaskHistoryEntry[]>([]);
  const [historyFlow, setHistoryFlow] = useState<string | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  useEffect(() => {
    if (!rootDir) return;
    setIsLoading(true);
    fetchFlows(rootDir)
      .then(setFlows)
      .catch((e) => console.error("Failed to fetch flows:", e))
      .finally(() => setIsLoading(false));
  }, [rootDir, refreshKey]);

  // Aggregate tasks across all flows
  const tasks = useMemo(() => {
    const taskMap = new Map<string, TaskSummary>();
    for (const flow of flows) {
      for (const node of flow.nodes) {
        const existing = taskMap.get(node.name);
        if (existing) {
          if (!existing.flows.includes(flow.name)) {
            existing.flows.push(flow.name);
          }
          existing.totalRuns += node.run_count;
          if (node.timestamp > existing.latestTimestamp) {
            existing.latestStatus = node.status;
            existing.latestDuration = node.duration;
            existing.latestTimestamp = node.timestamp;
          }
        } else {
          taskMap.set(node.name, {
            name: node.name,
            flows: [flow.name],
            latestStatus: node.status,
            latestDuration: node.duration,
            latestTimestamp: node.timestamp,
            totalRuns: node.run_count,
          });
        }
      }
    }
    return [...taskMap.values()].sort((a, b) => b.latestTimestamp - a.latestTimestamp);
  }, [flows]);

  const filteredTasks = useMemo(() => {
    if (!filter) return tasks;
    try {
      const regex = new RegExp(filter, "i");
      return tasks.filter(
        (t) => regex.test(t.name) || t.flows.some((f) => regex.test(f))
      );
    } catch {
      return tasks;
    }
  }, [tasks, filter]);

  const loadHistory = (taskName: string) => {
    if (expandedTask === taskName) {
      setExpandedTask(null);
      return;
    }
    setExpandedTask(taskName);
    // Find which flow has this task — use the first one
    const task = tasks.find((t) => t.name === taskName);
    if (!task || task.flows.length === 0) return;
    const flow = task.flows[0];
    setHistoryFlow(flow);
    setHistoryLoading(true);
    fetchTaskHistory(rootDir, flow, taskName)
      .then(setTaskHistory)
      .catch(() => setTaskHistory([]))
      .finally(() => setHistoryLoading(false));
  };

  if (!rootDir) {
    return <div className="text-center text-gray-500 mt-8">Enter a root directory.</div>;
  }

  if (isLoading) {
    return <div className="text-center text-gray-500 mt-8">Loading tasks...</div>;
  }

  if (tasks.length === 0) {
    return <div className="text-center text-gray-500 mt-8">No tasks found. Run a flow to see tasks here.</div>;
  }

  return (
    <div className="max-w-4xl mx-auto">
      <div className="mb-4">
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter tasks or flows..."
          className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      <div className="space-y-1">
        {filteredTasks.map((task) => (
          <div key={task.name} className="bg-white rounded-lg border border-gray-200">
            <button
              onClick={() => loadHistory(task.name)}
              className="w-full text-left px-4 py-3 flex items-center gap-3 hover:bg-gray-50 transition-colors"
            >
              <span
                className={`px-1.5 py-0.5 rounded text-xs font-medium shrink-0 ${
                  STATUS_COLORS[task.latestStatus] || STATUS_COLORS.cached
                }`}
              >
                {task.latestStatus}
              </span>
              <span className="font-medium text-sm text-gray-900 flex-1 truncate">
                {task.name}
              </span>
              <span className="text-xs text-gray-400 shrink-0">
                {task.flows.map((f) => f).join(", ")}
              </span>
              <span className="text-xs text-gray-500 shrink-0">
                {task.totalRuns} run{task.totalRuns === 1 ? "" : "s"}
              </span>
              <span className="text-xs text-gray-400 shrink-0">
                {formatDuration(task.latestDuration)}
              </span>
              <span className="text-xs text-gray-400">
                {expandedTask === task.name ? "\u25BC" : "\u25B6"}
              </span>
            </button>

            {expandedTask === task.name && (
              <div className="border-t border-gray-100 px-4 py-2">
                {historyLoading ? (
                  <div className="text-xs text-gray-400 py-2">Loading history...</div>
                ) : taskHistory.length === 0 ? (
                  <div className="text-xs text-gray-400 py-2">No history recorded.</div>
                ) : (
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-gray-500 uppercase tracking-wide">
                        <th className="py-1 pr-2">Run</th>
                        <th className="py-1 pr-2">Status</th>
                        <th className="py-1 pr-2">Duration</th>
                        <th className="py-1">When</th>
                      </tr>
                    </thead>
                    <tbody>
                      {taskHistory.map((h) => (
                        <tr
                          key={h.run_id}
                          className="border-t border-gray-50 hover:bg-blue-50 cursor-pointer transition-colors"
                          onClick={() =>
                            onSelectRun(
                              h.run_path,
                              `${historyFlow} / ${task.name} / ${h.run_id}`
                            )
                          }
                        >
                          <td className="py-1.5 pr-2 font-mono text-xs">{h.run_id}</td>
                          <td className="py-1.5 pr-2">
                            <span
                              className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                                STATUS_COLORS[h.status] || STATUS_COLORS.cached
                              }`}
                            >
                              {h.status}
                            </span>
                          </td>
                          <td className="py-1.5 pr-2 text-gray-700 text-xs">
                            {formatDuration(h.duration)}
                          </td>
                          <td className="py-1.5 text-gray-600 text-xs">
                            {formatTimestamp(h.timestamp)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
