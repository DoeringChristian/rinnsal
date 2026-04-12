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

/** Format a run timestamp like 20260412_151109 to a readable string. */
function formatRunName(name: string): string {
  const m = name.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/);
  if (!m) return name;
  return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:${m[6]}`;
}

function RunRow({
  run,
  isSelected,
  onToggle,
  onSolo,
}: {
  run: RunInfo;
  isSelected: boolean;
  onToggle: () => void;
  onSolo: () => void;
}) {
  const color = getRunColor(run.path);
  return (
    <div className="flex items-start group hover:bg-gray-50 rounded py-0.5 gap-1">
      <button
        onClick={onSolo}
        title="Solo — show only this run"
        className="w-4 h-4 mt-0.5 rounded-full shrink-0 opacity-30 hover:opacity-100 transition-opacity border border-gray-300 hover:border-gray-500"
        style={{ backgroundColor: color }}
      />
      <input
        type="checkbox"
        checked={isSelected}
        onChange={onToggle}
        className="mt-0.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0 cursor-pointer"
      />
      <span
        className="text-sm break-all leading-tight flex-1 cursor-pointer"
        style={{ color: isSelected ? color : undefined }}
        title={run.path}
        onClick={onToggle}
      >
        {formatRunName(run.name)}
      </span>
      <span
        className="w-2 h-2 mt-1.5 rounded-full shrink-0"
        style={{ backgroundColor: isSelected ? color : "transparent" }}
      />
    </div>
  );
}

export default function RunSelector({
  rootDir,
  selectedRuns,
  onSelectionChange,
  refreshKey = 0,
}: RunSelectorProps) {
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [filter, setFilter] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [collapsedFlows, setCollapsedFlows] = useState<Set<string>>(new Set());

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
      return {
        filteredRuns: runs.filter(
          (r) => regex.test(r.name) || (r.flow && regex.test(r.flow))
        ),
        filterError: null,
      };
    } catch (e) {
      return {
        filteredRuns: runs,
        filterError: e instanceof SyntaxError ? e.message : null,
      };
    }
  }, [runs, filter]);

  // Group runs by flow, with ungrouped runs under null
  const groups = useMemo(() => {
    const flowMap = new Map<string | null, RunInfo[]>();
    for (const r of filteredRuns) {
      const key = r.flow;
      if (!flowMap.has(key)) flowMap.set(key, []);
      flowMap.get(key)!.push(r);
    }
    // Sort: named flows first (alphabetical), then ungrouped
    const result: { flow: string | null; runs: RunInfo[] }[] = [];
    const sorted = [...flowMap.entries()].sort(([a], [b]) => {
      if (a === null && b === null) return 0;
      if (a === null) return 1;
      if (b === null) return -1;
      return a.localeCompare(b);
    });
    for (const [flow, flowRuns] of sorted) {
      result.push({ flow, runs: flowRuns });
    }
    return result;
  }, [filteredRuns]);

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

  const toggleFlowCollapsed = (flow: string) => {
    setCollapsedFlows((prev) => {
      const next = new Set(prev);
      if (next.has(flow)) next.delete(flow);
      else next.add(flow);
      return next;
    });
  };

  const toggleFlowAll = (flowRuns: RunInfo[]) => {
    const paths = flowRuns.map((r) => r.path);
    const allSelected = paths.every((p) => selectedRuns.includes(p));
    if (allSelected) {
      const pathSet = new Set(paths);
      onSelectionChange(selectedRuns.filter((r) => !pathSet.has(r)));
    } else {
      const existing = new Set(selectedRuns);
      const toAdd = paths.filter((p) => !existing.has(p));
      onSelectionChange([...selectedRuns, ...toAdd]);
    }
  };

  const allPaths = filteredRuns.map((r) => r.path);
  const allSelected =
    allPaths.length > 0 && allPaths.every((p) => selectedRuns.includes(p));

  const toggleAll = () => {
    if (allSelected) {
      const pathSet = new Set(allPaths);
      onSelectionChange(selectedRuns.filter((r) => !pathSet.has(r)));
    } else {
      const existing = new Set(selectedRuns);
      const toAdd = allPaths.filter((p) => !existing.has(p));
      onSelectionChange([...selectedRuns, ...toAdd]);
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

      <div className="flex items-center justify-between">
        <p className="text-xs text-gray-500">
          {filteredRuns.length}/{runs.length} runs
        </p>
        <button
          onClick={toggleAll}
          className="text-xs text-blue-600 hover:text-blue-800 transition-colors"
        >
          {allSelected ? "Deselect all" : "Select all"}
        </button>
      </div>

      <div className="space-y-1">
        {groups.map(({ flow, runs: flowRuns }) => {
          if (flow === null) {
            // Ungrouped runs — render flat
            return flowRuns.map((run) => (
              <RunRow
                key={run.path}
                run={run}
                isSelected={selectedRuns.includes(run.path)}
                onToggle={() => toggleRun(run.path)}
                onSolo={() => soloRun(run.path)}
              />
            ));
          }

          const isCollapsed = collapsedFlows.has(flow);
          const flowPaths = flowRuns.map((r) => r.path);
          const selectedCount = flowPaths.filter((p) =>
            selectedRuns.includes(p)
          ).length;

          return (
            <div key={flow} className="mb-1">
              <div className="flex items-center gap-1 py-0.5">
                <button
                  onClick={() => toggleFlowCollapsed(flow)}
                  className="text-xs text-gray-400 hover:text-gray-600 w-4 text-center"
                >
                  {isCollapsed ? "\u25B6" : "\u25BC"}
                </button>
                <button
                  onClick={() => toggleFlowAll(flowRuns)}
                  className="text-xs font-medium text-gray-700 hover:text-blue-600 flex-1 text-left truncate"
                  title={`${flow} (${flowRuns.length} runs)`}
                >
                  {flow}
                </button>
                <span className="text-xs text-gray-400">
                  {selectedCount > 0 && (
                    <span className="text-blue-600">{selectedCount}/</span>
                  )}
                  {flowRuns.length}
                </span>
              </div>
              {!isCollapsed && (
                <div className="ml-4 space-y-0.5">
                  {flowRuns.map((run) => (
                    <RunRow
                      key={run.path}
                      run={run}
                      isSelected={selectedRuns.includes(run.path)}
                      onToggle={() => toggleRun(run.path)}
                      onSolo={() => soloRun(run.path)}
                    />
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
