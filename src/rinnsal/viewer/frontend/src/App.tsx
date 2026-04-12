import { useState, useEffect, useCallback } from "react";
import RunSelector from "./components/RunSelector";
import CompareView, { useCompareGroups } from "./components/CompareView";
import FlowSidebar from "./components/FlowSidebar";
import FlowGraph from "./components/FlowGraph";
import TasksView from "./components/TasksView";
import RunDetailView from "./components/RunDetailView";
import { fetchConfig } from "./lib/api";

// ─── Types ──────────────────────────────────────────────────────

type SidebarTab = "flows" | "tasks" | "runs" | "compare";

interface SelectedRun {
  path: string;
  label: string;
}

// ─── Persistence ────────────────────────────────────────────────

const STORAGE_KEY = "rinnsal-viewer-state";

function loadPersistedState(): {
  rootDir?: string;
  sidebarTab?: SidebarTab;
  selectedFlow?: string | null;
  selectedRun?: SelectedRun | null;
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
  selectedRun: SelectedRun | null;
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
  const [selectedRun, setSelectedRun] = useState<SelectedRun | null>(
    persisted.selectedRun ?? null
  );
  const [refreshKey, setRefreshKey] = useState(0);

  // Compare groups (persisted separately)
  const { groups, setGroups, addToGroup } = useCompareGroups();

  // Auto-load config on mount
  useEffect(() => {
    if (persisted.rootDir) return;
    fetchConfig()
      .then((config) => {
        if (config.logDir) setRootDir(config.logDir);
      })
      .catch((e) => console.error("Failed to fetch config:", e));
  }, []);

  // Persist state
  useEffect(() => {
    persistState({ rootDir, sidebarTab, selectedFlow, selectedRun });
  }, [rootDir, sidebarTab, selectedFlow, selectedRun]);

  const handleRefresh = useCallback(() => {
    setRefreshKey((k) => k + 1);
  }, []);

  const handleSelectRun = useCallback(
    (runPath: string, label: string) => {
      setSelectedRun({ path: runPath, label });
    },
    []
  );

  const handleBackFromDetail = useCallback(() => {
    setSelectedRun(null);
  }, []);

  const switchSidebarTab = useCallback((tab: SidebarTab) => {
    setSidebarTab(tab);
    setSelectedRun(null); // clear detail when switching sections
  }, []);

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
            className={`w-10 h-10 rounded-lg flex items-center justify-center mb-1 transition-colors text-lg ${
              sidebarTab === item.key
                ? "bg-blue-600 text-white"
                : "text-gray-400 hover:text-white hover:bg-gray-700"
            }`}
          >
            {item.icon}
            {item.key === "compare" && groups.length > 0 && (
              <span className="absolute ml-5 -mt-3 text-xs bg-blue-500 text-white px-1 rounded-full">
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

      {/* Sidebar panel — shows context for the active nav item */}
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
          {/* Root dir input */}
          <div className="p-3 border-b border-gray-200">
            <input
              type="text"
              value={rootDir}
              onChange={(e) => setRootDir(e.target.value)}
              placeholder="/path/to/runs"
              className="w-full px-2 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {/* Sidebar content per tab */}
          <div className="flex-1 overflow-auto p-3">
            {sidebarTab === "flows" && (
              <FlowSidebar
                rootDir={rootDir}
                selectedFlow={selectedFlow}
                onSelectFlow={setSelectedFlow}
                onSelectRun={handleSelectRun}
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
                selectedRuns={
                  selectedRun ? [selectedRun.path] : []
                }
                onSelectionChange={(paths) => {
                  if (paths.length > 0) {
                    const p = paths[paths.length - 1];
                    const name = p.split("/").pop() || p;
                    handleSelectRun(p, name);
                  } else {
                    setSelectedRun(null);
                  }
                }}
                refreshKey={refreshKey}
              />
            )}
          </div>
        </aside>
      )}

      {/* Main content area */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {selectedRun ? (
          /* ─── Detail view for a selected run ─── */
          <RunDetailView
            runPath={selectedRun.path}
            runLabel={selectedRun.label}
            onBack={handleBackFromDetail}
            compareGroups={groups}
            onAddToCompare={addToGroup}
          />
        ) : (
          /* ─── Section overview (no run selected) ─── */
          <div className="flex-1 overflow-auto relative">
            {sidebarTab === "flows" && (
              <FlowGraph
                rootDir={rootDir}
                flowName={selectedFlow}
                refreshKey={refreshKey}
                onOpenRun={(runPath) => {
                  const name = runPath.split("/").pop() || runPath;
                  handleSelectRun(runPath, name);
                }}
              />
            )}
            {sidebarTab === "tasks" && (
              <div className="p-4">
                <TasksView
                  rootDir={rootDir}
                  refreshKey={refreshKey}
                  onSelectRun={handleSelectRun}
                />
              </div>
            )}
            {sidebarTab === "runs" && (
              <div className="text-center text-gray-500 mt-8">
                Select a run from the sidebar to view its data.
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
