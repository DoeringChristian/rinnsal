import { useState, useEffect, useMemo } from "react";
import { fetchRuns, RunInfo } from "../lib/api";

interface RunSelectorProps {
  rootDir: string;
  selectedRuns: string[];
  onSelectionChange: (runs: string[]) => void;
  refreshKey?: number;
}

const RUN_COLORS = [
  "#1f77b4",
  "#ff7f0e",
  "#2ca02c",
  "#d62728",
  "#9467bd",
  "#8c564b",
  "#e377c2",
  "#7f7f7f",
];

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

export function getRunColor(run: string): string {
  const idx = hashString(run);
  return RUN_COLORS[idx % RUN_COLORS.length];
}

const MAX_VISIBLE = 200;

export default function RunSelector({
  rootDir,
  selectedRuns,
  onSelectionChange,
  refreshKey = 0,
}: RunSelectorProps) {
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [filter, setFilter] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!rootDir) {
      setRuns([]);
      return;
    }
    setIsLoading(true);
    fetchRuns(rootDir)
      .then(setRuns)
      .catch((e) => console.error("Failed to fetch runs:", e))
      .finally(() => setIsLoading(false));
  }, [rootDir, refreshKey]);

  const { filteredRuns, filterError } = useMemo(() => {
    if (!filter) {
      return { filteredRuns: runs, filterError: null };
    }
    try {
      const regex = new RegExp(filter, "i");
      return { filteredRuns: runs.filter((r) => regex.test(r.name)), filterError: null };
    } catch (e) {
      return {
        filteredRuns: runs,
        filterError: e instanceof SyntaxError ? e.message : null,
      };
    }
  }, [runs, filter]);

  const visibleRuns = filteredRuns.slice(0, MAX_VISIBLE);
  const hasMore = filteredRuns.length > MAX_VISIBLE;

  const toggleRun = (runPath: string) => {
    if (selectedRuns.includes(runPath)) {
      onSelectionChange(selectedRuns.filter((r) => r !== runPath));
    } else {
      onSelectionChange([...selectedRuns, runPath]);
    }
  };

  const soloRun = (runPath: string) => {
    onSelectionChange([runPath]);
  };

  // Toggle all visible (filtered) runs
  const visiblePaths = visibleRuns.map((r) => r.path);
  const allVisibleSelected = visiblePaths.length > 0 &&
    visiblePaths.every((p) => selectedRuns.includes(p));

  const toggleAll = () => {
    if (allVisibleSelected) {
      // Deselect all visible, keep others
      const visibleSet = new Set(visiblePaths);
      onSelectionChange(selectedRuns.filter((r) => !visibleSet.has(r)));
    } else {
      // Select all visible, keep existing
      const existing = new Set(selectedRuns);
      const toAdd = visiblePaths.filter((p) => !existing.has(p));
      onSelectionChange([...selectedRuns, ...toAdd]);
    }
  };

  if (!rootDir) {
    return <p className="text-sm text-gray-500">Enter a directory path above.</p>;
  }

  if (isLoading) {
    return <p className="text-sm text-gray-500">Loading runs...</p>;
  }

  if (runs.length === 0) {
    return <p className="text-sm text-gray-500">No runs found.</p>;
  }

  return (
    <div className="space-y-3">
      <div>
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter (regex)"
          className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        {filterError && (
          <p className="text-xs text-red-500 mt-1">{filterError}</p>
        )}
      </div>

      <div className="flex items-center justify-between">
        <p className="text-xs text-gray-500">
          {filteredRuns.length}/{runs.length} runs
        </p>
        <button
          onClick={toggleAll}
          className="text-xs text-blue-600 hover:text-blue-800 transition-colors"
        >
          {allVisibleSelected ? "Deselect all" : "Select all"}
        </button>
      </div>

      <div className="space-y-0.5">
        {visibleRuns.map((run) => {
          const isSelected = selectedRuns.includes(run.path);
          const color = getRunColor(run.path);

          return (
            <div
              key={run.path}
              className="flex items-center group hover:bg-gray-50 rounded px-1 py-0.5"
            >
              <label className="flex items-center space-x-2 cursor-pointer flex-1 min-w-0">
                <input
                  type="checkbox"
                  checked={isSelected}
                  onChange={() => toggleRun(run.path)}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0"
                />
                <span
                  className="text-sm truncate"
                  style={{
                    color: isSelected ? color : undefined,
                    fontWeight: isSelected ? 600 : undefined,
                  }}
                  title={run.path}
                >
                  {run.name}
                </span>
              </label>
              <button
                onClick={() => soloRun(run.path)}
                title="Solo — deselect all others"
                className="opacity-0 group-hover:opacity-100 text-xs text-gray-400 hover:text-blue-600 px-1 shrink-0 transition-opacity"
              >
                solo
              </button>
            </div>
          );
        })}
        {hasMore && (
          <p className="text-xs text-gray-400 py-1">
            Showing {MAX_VISIBLE} of {filteredRuns.length} runs. Use filter to narrow down.
          </p>
        )}
      </div>
    </div>
  );
}
