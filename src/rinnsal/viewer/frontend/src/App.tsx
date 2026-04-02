import { useState, useEffect, useCallback, useRef } from "react";
import RunSelector from "./components/RunSelector";
import ScalarChart from "./components/ScalarChart";
import TextLog from "./components/TextLog";
import FigureViewer from "./components/FigureViewer";
import CardViewer from "./components/CardViewer";
import { useEvents, Tab } from "./hooks/useEvents";
import { fetchConfig } from "./lib/api";

const STORAGE_KEY = "rinnsal-viewer-state";

function loadPersistedState(): {
  rootDir?: string;
  selectedRuns?: string[];
  activeTab?: Tab;
  scrollTop?: number;
} {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch { /* ignore */ }
  return {};
}

function persistState(state: {
  rootDir: string;
  selectedRuns: string[];
  activeTab: Tab;
  scrollTop: number;
}) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch { /* ignore */ }
}

export default function App() {
  const persisted = loadPersistedState();
  const [rootDir, setRootDir] = useState(persisted.rootDir || "");
  const [selectedRuns, setSelectedRuns] = useState<string[]>(
    persisted.selectedRuns || [],
  );
  const [activeTab, setActiveTab] = useState<Tab>(
    persisted.activeTab || "scalars",
  );
  const [refreshKey, setRefreshKey] = useState(0);
  const contentRef = useRef<HTMLDivElement>(null);
  const scrollTopRef = useRef(persisted.scrollTop || 0);

  const { scalars, text, figures, cards, isLoading, refresh: refreshData } =
    useEvents(selectedRuns, activeTab);

  // Load config from backend (only if no persisted rootDir)
  useEffect(() => {
    if (persisted.rootDir) return;
    fetchConfig()
      .then((config) => {
        if (config.logDir) setRootDir(config.logDir);
      })
      .catch((e) => console.error("Failed to fetch config:", e));
  }, []);

  // Persist state on every change
  useEffect(() => {
    persistState({ rootDir, selectedRuns, activeTab, scrollTop: scrollTopRef.current });
  }, [rootDir, selectedRuns, activeTab]);

  // Save scroll position periodically and on unload
  useEffect(() => {
    const el = contentRef.current;
    if (!el) return;
    const onScroll = () => { scrollTopRef.current = el.scrollTop; };
    el.addEventListener("scroll", onScroll, { passive: true });
    const onBeforeUnload = () => {
      persistState({ rootDir, selectedRuns, activeTab, scrollTop: scrollTopRef.current });
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => {
      el.removeEventListener("scroll", onScroll);
      window.removeEventListener("beforeunload", onBeforeUnload);
    };
  }, [rootDir, selectedRuns, activeTab]);

  // Restore scroll position after content loads
  useEffect(() => {
    if (!isLoading && contentRef.current && scrollTopRef.current > 0) {
      contentRef.current.scrollTop = scrollTopRef.current;
    }
  }, [isLoading]);

  const handleRefresh = useCallback(() => {
    setRefreshKey((k) => k + 1); // triggers RunSelector to re-fetch run list
    refreshData();
  }, [refreshData]);

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <aside className="w-72 bg-white border-r border-gray-200 flex flex-col">
        <header className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h1 className="text-xl font-semibold text-gray-800">Rinnsal</h1>
          <button
            onClick={handleRefresh}
            title="Refresh runs and data"
            className="p-1.5 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded transition-colors"
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="23 4 23 10 17 10" />
              <polyline points="1 20 1 14 7 14" />
              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
            </svg>
          </button>
        </header>

        <div className="p-4 border-b border-gray-200">
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Root Directory
          </label>
          <input
            type="text"
            value={rootDir}
            onChange={(e) => setRootDir(e.target.value)}
            placeholder="/path/to/runs"
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div className="flex-1 overflow-auto p-4">
          <RunSelector
            rootDir={rootDir}
            selectedRuns={selectedRuns}
            onSelectionChange={setSelectedRuns}
            refreshKey={refreshKey}
          />
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {/* Tabs */}
        <nav className="bg-white border-b border-gray-200 px-4">
          <div className="flex space-x-4">
            {(["scalars", "text", "figures", "cards"] as Tab[]).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`py-3 px-1 border-b-2 text-sm font-medium capitalize transition-colors ${
                  activeTab === tab
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                }`}
              >
                {tab}
              </button>
            ))}
          </div>
        </nav>

        {/* Content */}
        <div ref={contentRef} className="flex-1 overflow-auto p-4">
          {selectedRuns.length === 0 ? (
            <div className="text-center text-gray-500 mt-8">
              Select runs from the sidebar to view data.
            </div>
          ) : isLoading ? (
            <div className="text-center text-gray-500 mt-8">Loading...</div>
          ) : (
            <>
              {activeTab === "scalars" && (
                <ScalarChart data={scalars} />
              )}
              {activeTab === "text" && (
                <TextLog data={text} />
              )}
              {activeTab === "figures" && (
                <FigureViewer data={figures} selectedRuns={selectedRuns} />
              )}
              {activeTab === "cards" && (
                <CardViewer data={cards} />
              )}
            </>
          )}
        </div>
      </main>
    </div>
  );
}
