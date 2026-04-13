import { useState, useEffect, useCallback } from "react";
import RunSelector from "./components/RunSelector";
import CompareView, { useCompareGroups } from "./components/CompareView";
import FlowSidebar from "./components/FlowSidebar";
import FlowGraph from "./components/FlowGraph";
import TasksView from "./components/TasksView";
import RunDetailView from "./components/RunDetailView";
import FilterSidebar from "./components/FilterSidebar";
import { fetchConfig } from "./lib/api";

// ─── Types ──────────────────────────────────────────────────────

type SidebarTab = "flows" | "tasks" | "runs" | "filter" | "compare";

interface OpenRun {
  path: string;
  label: string;
}

// ─── Persistence ────────────────────────────────────────────────

const STORAGE_KEY = "rinnsal-viewer-state";

function loadPersistedState(): {
  rootDir?: string;
  sidebarTab?: SidebarTab;
  selectedFlow?: string | null;
  openRuns?: OpenRun[];
  activeRunIdx?: number;
} {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch {
    /* ignore */
  }
  return {};
}

function persistState(state: {
  rootDir: string;
  sidebarTab: SidebarTab;
  selectedFlow: string | null;
  openRuns: OpenRun[];
  activeRunIdx: number;
}) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    /* ignore */
  }
}

// ─── Sidebar nav items ──────────────────────────────────────────

const NAV_ITEMS: { key: SidebarTab; label: string; icon: string }[] = [
  { key: "flows", label: "Flows", icon: "\u2B21" },
  { key: "tasks", label: "Tasks", icon: "\u25A3" },
  { key: "runs", label: "Runs", icon: "\u25F6" },
  { key: "filter", label: "Filter", icon: "\u2AF6" },
  { key: "compare", label: "Compare", icon: "\u2194" },
];

// ─── App ────────────────────────────────────────────────────────

export default function App() {
  const [persisted] = useState(loadPersistedState);
  const [rootDir, setRootDir] = useState(persisted.rootDir || "");
  const [sidebarTab, setSidebarTab] = useState<SidebarTab>(
    persisted.sidebarTab || "flows"
  );
  const [selectedFlow, setSelectedFlow] = useState<string | null>(
    persisted.selectedFlow ?? null
  );
  // Open run tabs — multiple runs can be open at once
  const [openRuns, setOpenRuns] = useState<OpenRun[]>(
    persisted.openRuns || []
  );
  // -1 = no run tab active (show section overview), >=0 = index into openRuns
  const [activeRunIdx, setActiveRunIdx] = useState<number>(
    persisted.activeRunIdx ?? -1
  );
  const [refreshKey, setRefreshKey] = useState(0);

  const { groups, setGroups, addToGroup } = useCompareGroups();

  useEffect(() => {
    if (persisted.rootDir) return;
    fetchConfig()
      .then((config) => {
        if (config.logDir) setRootDir(config.logDir);
      })
      .catch((e) => console.error("Failed to fetch config:", e));
  }, []);

  useEffect(() => {
    persistState({
      rootDir,
      sidebarTab,
      selectedFlow,
      openRuns,
      activeRunIdx,
    });
  }, [rootDir, sidebarTab, selectedFlow, openRuns, activeRunIdx]);

  const handleRefresh = useCallback(() => {
    setRefreshKey((k) => k + 1);
  }, []);

  // Open a run tab (or focus it if already open)
  const handleOpenRun = useCallback(
    (runPath: string, label: string) => {
      setOpenRuns((prev) => {
        const existing = prev.findIndex((r) => r.path === runPath);
        if (existing >= 0) {
          setActiveRunIdx(existing);
          return prev;
        }
        const next = [...prev, { path: runPath, label }];
        setActiveRunIdx(next.length - 1);
        return next;
      });
    },
    []
  );

  // Close a run tab
  const handleCloseRun = useCallback(
    (idx: number) => {
      setOpenRuns((prev) => {
        const next = prev.filter((_, i) => i !== idx);
        setActiveRunIdx((cur) => {
          if (next.length === 0) return -1;
          if (cur === idx) return Math.min(idx, next.length - 1);
          if (cur > idx) return cur - 1;
          return cur;
        });
        return next;
      });
    },
    []
  );

  const switchSidebarTab = useCallback((tab: SidebarTab) => {
    setSidebarTab(tab);
    setActiveRunIdx(-1); // show section overview
  }, []);

  const activeRun =
    activeRunIdx >= 0 && activeRunIdx < openRuns.length
      ? openRuns[activeRunIdx]
      : null;

  // ─── Render ─────────────────────────────────────────────────

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Left nav rail */}
      <nav className="bg-gray-900 flex flex-col items-center py-2 w-14 shrink-0">
        <div className="mb-4 mt-1">
          <span className="text-white font-bold text-lg" title="Rinnsal">
            R
          </span>
        </div>
        {NAV_ITEMS.map((item) => (
          <button
            key={item.key}
            onClick={() => switchSidebarTab(item.key)}
            title={item.label}
            className={`w-10 h-10 rounded-lg flex items-center justify-center mb-1 transition-colors text-lg relative ${
              sidebarTab === item.key && activeRunIdx < 0
                ? "bg-blue-600 text-white"
                : "text-gray-400 hover:text-white hover:bg-gray-700"
            }`}
          >
            {item.icon}
            {item.key === "compare" && groups.length > 0 && (
              <span className="absolute -top-1 -right-1 text-xs bg-blue-500 text-white w-4 h-4 rounded-full flex items-center justify-center">
                {groups.length}
              </span>
            )}
          </button>
        ))}
        <div className="flex-1" />
        <button
          onClick={handleRefresh}
          title="Refresh"
          className="w-10 h-10 rounded-lg flex items-center justify-center text-gray-400 hover:text-white hover:bg-gray-700 transition-colors mb-2"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <polyline points="23 4 23 10 17 10" />
            <polyline points="1 20 1 14 7 14" />
            <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
          </svg>
        </button>
      </nav>

      {/* Sidebar panel */}
      {sidebarTab !== "compare" && (
        <aside
          className="bg-white border-r border-gray-200 flex flex-col overflow-hidden"
          style={{
            width: 260,
            minWidth: 180,
            maxWidth: 500,
            resize: "horizontal",
            overflow: "auto",
          }}
        >
          <div className="p-3 border-b border-gray-200">
            <input
              type="text"
              value={rootDir}
              onChange={(e) => setRootDir(e.target.value)}
              placeholder="/path/to/runs"
              className="w-full px-2 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <div className="flex-1 overflow-auto p-3">
            {sidebarTab === "flows" && (
              <FlowSidebar
                rootDir={rootDir}
                selectedFlow={selectedFlow}
                onSelectFlow={setSelectedFlow}
                onSelectRun={handleOpenRun}
                refreshKey={refreshKey}
              />
            )}
            {sidebarTab === "tasks" && (
              <div className="text-xs text-gray-500">
                Tasks are shown in the main area.
              </div>
            )}
            {sidebarTab === "runs" && (
              <RunSelector
                rootDir={rootDir}
                selectedRuns={activeRun ? [activeRun.path] : []}
                onSelectionChange={(paths) => {
                  if (paths.length > 0) {
                    const p = paths[paths.length - 1];
                    const name = p.split("/").pop() || p;
                    handleOpenRun(p, name);
                  }
                }}
                refreshKey={refreshKey}
              />
            )}
            {sidebarTab === "filter" && (
              <FilterSidebar
                rootDir={rootDir}
                refreshKey={refreshKey}
                onSelectRun={handleOpenRun}
              />
            )}
          </div>
        </aside>
      )}

      {/* Main content area */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {/* Run tabs bar — shown when there are open runs */}
        {openRuns.length > 0 && (
          <div className="bg-white border-b border-gray-200 px-2 flex items-center shrink-0 overflow-x-auto">
            {/* Section tab (back to overview) */}
            <button
              onClick={() => setActiveRunIdx(-1)}
              className={`py-2 px-3 text-xs font-medium border-b-2 transition-colors shrink-0 capitalize ${
                activeRunIdx < 0
                  ? "border-blue-500 text-blue-600"
                  : "border-transparent text-gray-500 hover:text-gray-700"
              }`}
            >
              {sidebarTab}
            </button>
            <div className="w-px h-5 bg-gray-200 mx-1" />
            {/* Open run tabs */}
            {openRuns.map((run, idx) => {
              const shortLabel =
                run.label.length > 30
                  ? "..." + run.label.slice(-27)
                  : run.label;
              return (
                <div
                  key={run.path}
                  className={`flex items-center border-b-2 transition-colors shrink-0 ${
                    activeRunIdx === idx
                      ? "border-blue-500"
                      : "border-transparent"
                  }`}
                >
                  <button
                    onClick={() => setActiveRunIdx(idx)}
                    className={`py-2 px-2 text-xs font-medium transition-colors ${
                      activeRunIdx === idx
                        ? "text-blue-600"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                    title={run.label}
                  >
                    {shortLabel}
                  </button>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleCloseRun(idx);
                    }}
                    className="text-gray-400 hover:text-red-500 text-xs px-1"
                    title="Close"
                  >
                    {"\u00D7"}
                  </button>
                </div>
              );
            })}
          </div>
        )}

        {/* Content */}
        {activeRun ? (
          <RunDetailView
            runPath={activeRun.path}
            runLabel={activeRun.label}
            onBack={() => setActiveRunIdx(-1)}
            compareGroups={groups}
            onAddToCompare={addToGroup}
            refreshKey={refreshKey}
          />
        ) : (
          <div className="flex-1 overflow-auto relative">
            {sidebarTab === "flows" && (
              <FlowGraph
                rootDir={rootDir}
                flowName={selectedFlow}
                refreshKey={refreshKey}
                onOpenRun={(runPath) => {
                  const name = runPath.split("/").pop() || runPath;
                  handleOpenRun(runPath, name);
                }}
              />
            )}
            {sidebarTab === "tasks" && (
              <div className="p-4">
                <TasksView
                  rootDir={rootDir}
                  refreshKey={refreshKey}
                  onSelectRun={handleOpenRun}
                />
              </div>
            )}
            {sidebarTab === "runs" && (
              <div className="text-center text-gray-500 mt-8">
                Select a run from the sidebar to view its data.
              </div>
            )}
            {sidebarTab === "filter" && (
              <div className="text-center text-gray-500 mt-8">
                Use the filter sidebar to search runs by parameters.
              </div>
            )}
            {sidebarTab === "compare" && (
              <div className="p-4">
                <CompareView
                  groups={groups}
                  onGroupsChange={setGroups}
                />
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
