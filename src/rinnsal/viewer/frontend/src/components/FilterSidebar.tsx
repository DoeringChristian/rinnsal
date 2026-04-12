import { useState, useEffect, useMemo } from "react";
import { fetchFlows, fetchTaskHistory } from "../lib/api";

interface FilterSidebarProps {
  rootDir: string;
  refreshKey?: number;
  onSelectRun: (runPath: string, label: string) => void;
}

function parseParams(params?: string): Record<string, any> | null {
  if (!params) return null;
  try {
    return JSON.parse(params);
  } catch {
    return null;
  }
}

function flattenParams(
  obj: Record<string, any>,
  prefix = ""
): { key: string; value: string }[] {
  const result: { key: string; value: string }[] = [];
  for (const [k, v] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === "object" && !Array.isArray(v) && !v._type) {
      result.push(...flattenParams(v, fullKey));
    } else {
      result.push({
        key: fullKey,
        value: typeof v === "object" ? JSON.stringify(v) : String(v ?? ""),
      });
    }
  }
  return result;
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

function formatRunId(name: string): string {
  const m = name.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/);
  if (!m) return name;
  return `${m[2]}-${m[3]} ${m[4]}:${m[5]}`;
}

interface RunWithParams {
  run_id: string;
  run_path: string;
  flow: string;
  task: string;
  status: string;
  duration: number;
  params: Record<string, any>;
  flatParams: { key: string; value: string }[];
}

export default function FilterSidebar({
  rootDir,
  refreshKey = 0,
  onSelectRun,
}: FilterSidebarProps) {
  const [allRuns, setAllRuns] = useState<RunWithParams[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [searchText, setSearchText] = useState("");

  // Fetch all flows and their task histories
  useEffect(() => {
    if (!rootDir) return;
    setIsLoading(true);
    fetchFlows(rootDir)
      .then(async (flowList) => {
        // Fetch history for every task in every flow
        const runs: RunWithParams[] = [];
        for (const flow of flowList) {
          for (const node of flow.nodes) {
            try {
              const history = await fetchTaskHistory(
                rootDir,
                flow.name,
                node.name
              );
              for (const h of history) {
                const params = parseParams(h.params) || {};
                runs.push({
                  run_id: h.run_id,
                  run_path: h.run_path,
                  flow: flow.name,
                  task: node.name,
                  status: h.status,
                  duration: h.duration,
                  params,
                  flatParams: flattenParams(params),
                });
              }
            } catch {
              // skip
            }
          }
        }
        setAllRuns(runs);
      })
      .catch((e) => console.error("Failed to fetch flows:", e))
      .finally(() => setIsLoading(false));
  }, [rootDir, refreshKey]);

  // Discover all unique param keys
  const paramKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const r of allRuns) {
      for (const fp of r.flatParams) {
        keys.add(fp.key);
      }
    }
    return [...keys].sort();
  }, [allRuns]);

  // Unique values per key (for quick filter suggestions)
  const paramValues = useMemo(() => {
    const vals = new Map<string, Set<string>>();
    for (const r of allRuns) {
      for (const fp of r.flatParams) {
        if (!vals.has(fp.key)) vals.set(fp.key, new Set());
        vals.get(fp.key)!.add(fp.value);
      }
    }
    return vals;
  }, [allRuns]);

  // Apply filters
  const filteredRuns = useMemo(() => {
    let runs = allRuns;

    // Text search
    if (searchText.trim()) {
      const lower = searchText.toLowerCase();
      runs = runs.filter(
        (r) =>
          r.flow.toLowerCase().includes(lower) ||
          r.task.toLowerCase().includes(lower) ||
          r.run_id.toLowerCase().includes(lower) ||
          r.flatParams.some(
            (fp) =>
              fp.key.toLowerCase().includes(lower) ||
              fp.value.toLowerCase().includes(lower)
          )
      );
    }

    // Param filters
    for (const [key, filterVal] of Object.entries(filters)) {
      if (!filterVal.trim()) continue;
      const lower = filterVal.toLowerCase();
      runs = runs.filter((r) =>
        r.flatParams.some(
          (fp) => fp.key === key && fp.value.toLowerCase().includes(lower)
        )
      );
    }

    return runs;
  }, [allRuns, searchText, filters]);

  // Deduplicate by run_path (a run appears once per task)
  const uniqueRuns = useMemo(() => {
    const seen = new Set<string>();
    return filteredRuns.filter((r) => {
      if (seen.has(r.run_path)) return false;
      seen.add(r.run_path);
      return true;
    });
  }, [filteredRuns]);

  if (!rootDir) {
    return (
      <p className="text-sm text-gray-500">Enter a directory path above.</p>
    );
  }

  if (isLoading) {
    return <p className="text-sm text-gray-500">Loading...</p>;
  }

  return (
    <div className="space-y-3">
      {/* Global search */}
      <div>
        <input
          type="text"
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          placeholder="Search everything..."
          className="w-full px-2 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {/* Per-parameter filters */}
      {paramKeys.length > 0 && (
        <div className="space-y-1.5">
          <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Parameters
          </div>
          {paramKeys.map((key) => {
            const uniqueVals = paramValues.get(key);
            const valCount = uniqueVals?.size || 0;
            return (
              <div key={key}>
                <label className="text-xs text-gray-600 block mb-0.5">
                  {key}{" "}
                  <span className="text-gray-400">({valCount} values)</span>
                </label>
                {valCount <= 8 ? (
                  // Show as clickable chips for few values
                  <div className="flex flex-wrap gap-1">
                    {[...(uniqueVals || [])].sort().map((val) => {
                      const isActive = filters[key] === val;
                      return (
                        <button
                          key={val}
                          onClick={() =>
                            setFilters((prev) => ({
                              ...prev,
                              [key]: isActive ? "" : val,
                            }))
                          }
                          className={`px-1.5 py-0.5 rounded text-xs transition-colors ${
                            isActive
                              ? "bg-blue-100 text-blue-700 font-medium"
                              : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                          }`}
                          title={val}
                        >
                          {val.length > 20 ? val.slice(0, 18) + "..." : val}
                        </button>
                      );
                    })}
                  </div>
                ) : (
                  // Text input for many values
                  <input
                    type="text"
                    value={filters[key] || ""}
                    onChange={(e) =>
                      setFilters((prev) => ({
                        ...prev,
                        [key]: e.target.value,
                      }))
                    }
                    placeholder={`Filter ${key}...`}
                    className="w-full px-2 py-1 border border-gray-200 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                )}
              </div>
            );
          })}
          {Object.values(filters).some((v) => v.trim()) && (
            <button
              onClick={() => setFilters({})}
              className="text-xs text-blue-600 hover:text-blue-800"
            >
              Clear all filters
            </button>
          )}
        </div>
      )}

      {/* Results */}
      <div>
        <div className="text-xs text-gray-500 mb-1">
          {uniqueRuns.length} run{uniqueRuns.length === 1 ? "" : "s"}
          {filteredRuns.length !== allRuns.length && (
            <span> (filtered from {allRuns.length})</span>
          )}
        </div>
        <div className="space-y-0.5">
          {uniqueRuns.map((r) => (
            <button
              key={r.run_path}
              onClick={() =>
                onSelectRun(r.run_path, `${r.flow} / ${r.run_id}`)
              }
              className="w-full text-left px-2 py-1.5 rounded text-xs hover:bg-blue-50 transition-colors flex items-center gap-2"
            >
              <span
                className={`px-1 py-0.5 rounded text-xs font-medium shrink-0 ${
                  STATUS_COLORS[r.status] || STATUS_COLORS.cached
                }`}
              >
                {r.status}
              </span>
              <span className="text-gray-700 truncate flex-1">
                {r.flow}
              </span>
              <span className="text-gray-400 shrink-0">
                {formatRunId(r.run_id)}
              </span>
              <span className="text-gray-400 shrink-0">
                {formatDuration(r.duration)}
              </span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
