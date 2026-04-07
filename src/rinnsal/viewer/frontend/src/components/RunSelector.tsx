import { useState, useEffect, useMemo } from "react";
import { fetchRuns, RunInfo } from "../lib/api";

interface RunSelectorProps {
  rootDir: string;
  selectedRuns: string[];
  onSelectionChange: (runs: string[]) => void;
  refreshKey?: number;
}

/**
 * Generate visually distinct colors using golden-angle spacing in HSL.
 * Each run gets a unique hue based on its index in the full run list,
 * ensuring adjacent runs never share a color.
 */
const GOLDEN_ANGLE = 137.508;

// Cache: run path → assigned index (stable across renders)
const colorIndexMap = new Map<string, number>();
let nextColorIndex = 0;

function assignColorIndex(run: string): number {
  let idx = colorIndexMap.get(run);
  if (idx === undefined) {
    idx = nextColorIndex++;
    colorIndexMap.set(run, idx);
  }
  return idx;
}

export function getRunColor(run: string): string {
  const idx = assignColorIndex(run);
  const hue = (idx * GOLDEN_ANGLE) % 360;
  return `hsl(${hue}, 70%, 45%)`;
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

  const visiblePaths = visibleRuns.map((r) => r.path);
  const allVisibleSelected = visiblePaths.length > 0 &&
    visiblePaths.every((p) => selectedRuns.includes(p));

  const toggleAll = () => {
    if (allVisibleSelected) {
      const visibleSet = new Set(visiblePaths);
      onSelectionChange(selectedRuns.filter((r) => !visibleSet.has(r)));
    } else {
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
              className="flex items-start group hover:bg-gray-50 rounded py-0.5 gap-1"
            >
              {/* Solo button — round, left of checkbox */}
              <button
                onClick={() => soloRun(run.path)}
                title="Solo — show only this run"
                className="w-4 h-4 mt-0.5 rounded-full shrink-0 opacity-30 hover:opacity-100 transition-opacity border border-gray-300 hover:border-gray-500"
                style={{ backgroundColor: color }}
              />
              {/* Checkbox */}
              <input
                type="checkbox"
                checked={isSelected}
                onChange={() => toggleRun(run.path)}
                className="mt-0.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0 cursor-pointer"
              />
              {/* Run name — wrapping */}
              <span
                className="text-sm break-all leading-tight flex-1 cursor-pointer"
                style={{
                  color: isSelected ? color : undefined,
                }}
                title={run.path}
                onClick={() => toggleRun(run.path)}
              >
                {run.name}
              </span>
              {/* Color dot on right — always present to keep layout stable */}
              <span
                className="w-2 h-2 mt-1.5 rounded-full shrink-0"
                style={{ backgroundColor: isSelected ? color : "transparent" }}
              />
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
