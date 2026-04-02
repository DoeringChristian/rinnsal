import { useState, useEffect, useMemo } from "react";
import { fetchRuns, RunInfo } from "../lib/api";

interface RunSelectorProps {
  rootDir: string;
  selectedRuns: string[];
  onSelectionChange: (runs: string[]) => void;
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
}: RunSelectorProps) {
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [filter, setFilter] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  // Fetch runs when rootDir changes
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
  }, [rootDir]);

  // Filter runs by regex (pure computation, no state updates)
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

  // Limit rendered DOM nodes
  const visibleRuns = filteredRuns.slice(0, MAX_VISIBLE);
  const hasMore = filteredRuns.length > MAX_VISIBLE;

  const toggleRun = (runPath: string) => {
    if (selectedRuns.includes(runPath)) {
      onSelectionChange(selectedRuns.filter((r) => r !== runPath));
    } else {
      onSelectionChange([...selectedRuns, runPath]);
    }
  };

  if (!rootDir) {
    return (
      <p className="text-sm text-gray-500">Enter a directory path above.</p>
    );
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

      <p className="text-xs text-gray-500">
        {filteredRuns.length}/{runs.length} runs
      </p>

      <div className="space-y-1">
        {visibleRuns.map((run) => {
          const isSelected = selectedRuns.includes(run.path);
          const color = getRunColor(run.path);

          return (
            <label
              key={run.path}
              className="flex items-center space-x-2 cursor-pointer hover:bg-gray-50 p-1 rounded"
            >
              <input
                type="checkbox"
                checked={isSelected}
                onChange={() => toggleRun(run.path)}
                className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
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
