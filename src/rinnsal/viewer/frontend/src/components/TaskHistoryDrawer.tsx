import { useState, useEffect, useMemo } from "react";
import { fetchTaskHistory, TaskHistoryEntry } from "../lib/api";

interface TaskHistoryDrawerProps {
  rootDir: string;
  flowName: string;
  taskName: string | null;
  onClose: () => void;
  onOpenRun?: (runPath: string) => void;
}

function parseParams(params?: string): Record<string, any> | null {
  if (!params) return null;
  try {
    return JSON.parse(params);
  } catch {
    return null;
  }
}

/** Extract all unique param keys and their values across history entries. */
function extractParamColumns(
  history: TaskHistoryEntry[]
): { key: string; values: Map<string, string> }[] {
  const keySet = new Map<string, Map<string, string>>();
  for (const h of history) {
    const p = parseParams(h.params);
    if (!p) continue;
    for (const [k, v] of Object.entries(p)) {
      if (!keySet.has(k)) keySet.set(k, new Map());
      const display =
        typeof v === "object" && v !== null
          ? JSON.stringify(v)
          : String(v ?? "");
      keySet.get(k)!.set(h.run_id, display);
    }
  }
  return [...keySet.entries()].map(([key, values]) => ({ key, values }));
}

const STATUS_COLORS: Record<string, string> = {
  success: "text-green-700 bg-green-100",
  failed: "text-red-700 bg-red-100",
  cached: "text-gray-700 bg-gray-100",
  running: "text-yellow-700 bg-yellow-100",
};

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "—";
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
  if (seconds < 60) return `${seconds.toFixed(2)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds - m * 60);
  return `${m}m${s}s`;
}

function formatTimestamp(ts: number): string {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString();
}

export default function TaskHistoryDrawer({
  rootDir,
  flowName,
  taskName,
  onClose,
  onOpenRun,
}: TaskHistoryDrawerProps) {
  const [history, setHistory] = useState<TaskHistoryEntry[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!taskName) {
      setHistory([]);
      return;
    }
    setIsLoading(true);
    setError(null);
    fetchTaskHistory(rootDir, flowName, taskName)
      .then(setHistory)
      .catch((e) => {
        console.error("Failed to fetch task history:", e);
        setError(String(e));
      })
      .finally(() => setIsLoading(false));
  }, [rootDir, flowName, taskName]);

  if (!taskName) return null;

  return (
    <div className="absolute top-0 right-0 h-full w-[480px] bg-white border-l border-gray-200 shadow-xl flex flex-col z-10">
      <header className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
        <div className="min-w-0">
          <div className="text-xs text-gray-500">{flowName}</div>
          <div className="font-medium text-gray-900 break-all">{taskName}</div>
        </div>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-700 text-xl leading-none px-2"
          title="Close"
        >
          ×
        </button>
      </header>

      <div className="flex-1 overflow-auto p-4">
        {isLoading && <p className="text-sm text-gray-500">Loading history...</p>}
        {error && <p className="text-sm text-red-500">{error}</p>}
        {!isLoading && !error && history.length === 0 && (
          <p className="text-sm text-gray-500">No history recorded.</p>
        )}
        {!isLoading && !error && history.length > 0 && (
          <HistoryTable history={history} onOpenRun={onOpenRun} />
        )}
      </div>
    </div>
  );
}

function HistoryTable({
  history,
  onOpenRun,
}: {
  history: TaskHistoryEntry[];
  onOpenRun?: (runPath: string) => void;
}) {
  const [paramFilter, setParamFilter] = useState("");
  const paramColumns = useMemo(() => extractParamColumns(history), [history]);

  // Filter by param values
  const filteredHistory = useMemo(() => {
    if (!paramFilter.trim()) return history;
    const lower = paramFilter.toLowerCase();
    return history.filter((h) => {
      const p = parseParams(h.params);
      if (!p) return false;
      return Object.values(p).some((v) => {
        const s = typeof v === "object" ? JSON.stringify(v) : String(v ?? "");
        return s.toLowerCase().includes(lower);
      });
    });
  }, [history, paramFilter]);

  return (
    <div>
      {paramColumns.length > 0 && (
        <div className="mb-3">
          <input
            type="text"
            value={paramFilter}
            onChange={(e) => setParamFilter(e.target.value)}
            placeholder="Filter by parameters..."
            className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-gray-500 uppercase tracking-wide">
              <th className="py-2 pr-2">Run</th>
              <th className="py-2 pr-2">Status</th>
              <th className="py-2 pr-2">Duration</th>
              {paramColumns.map((col) => (
                <th key={col.key} className="py-2 pr-2 text-blue-600">
                  {col.key}
                </th>
              ))}
              <th className="py-2">When</th>
            </tr>
          </thead>
          <tbody>
            {filteredHistory.map((h) => (
              <tr
                key={h.run_id}
                className="border-t border-gray-100 hover:bg-gray-50 cursor-pointer"
                onClick={() => onOpenRun?.(h.run_path)}
                title={h.error || "Click to open run"}
              >
                <td className="py-2 pr-2 font-mono text-xs break-all">
                  {h.run_id}
                </td>
                <td className="py-2 pr-2">
                  <span
                    className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                      STATUS_COLORS[h.status] || "text-gray-700 bg-gray-100"
                    }`}
                  >
                    {h.status}
                  </span>
                </td>
                <td className="py-2 pr-2 text-gray-700 text-xs">
                  {formatDuration(h.duration)}
                </td>
                {paramColumns.map((col) => (
                  <td
                    key={col.key}
                    className="py-2 pr-2 text-xs text-gray-600 font-mono max-w-[120px] truncate"
                    title={col.values.get(h.run_id) || ""}
                  >
                    {col.values.get(h.run_id) || "\u2014"}
                  </td>
                ))}
                <td className="py-2 text-gray-600 text-xs">
                  {formatTimestamp(h.timestamp)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {paramFilter && filteredHistory.length !== history.length && (
        <p className="text-xs text-gray-400 mt-2">
          Showing {filteredHistory.length} of {history.length} runs
        </p>
      )}
    </div>
  );
}
