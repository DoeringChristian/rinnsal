import { useState, useEffect, useCallback, useRef } from "react";
import RunSelector from "./components/RunSelector";
import ScalarChart from "./components/ScalarChart";
import TextLog from "./components/TextLog";
import FigureViewer from "./components/FigureViewer";
import ImageViewer from "./components/ImageViewer";
import CardViewer from "./components/CardViewer";
import CompareView, { useCompareGroups } from "./components/CompareView";
import FlowSidebar from "./components/FlowSidebar";
import FlowGraph from "./components/FlowGraph";
import { useEvents, Tab } from "./hooks/useEvents";
import { fetchConfig } from "./lib/api";

const STORAGE_KEY = "rinnsal-viewer-state";

function loadPersistedState(): {
  rootDir?: string;
  selectedRuns?: string[];
  activeTab?: Tab;
  selectedFlow?: string | null;
  scrollTops?: Record<string, number>;
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
  selectedFlow: string | null;
  scrollTops: Record<string, number>;
}) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch { /* ignore */ }
}

export default function App() {
  const [persisted] = useState(loadPersistedState);
  const [rootDir, setRootDir] = useState(persisted.rootDir || "");
  const [selectedRuns, setSelectedRuns] = useState<string[]>(persisted.selectedRuns || []);
  const [activeTab, setActiveTab] = useState<Tab>(persisted.activeTab || "scalars");
  const [selectedFlow, setSelectedFlow] = useState<string | null>(persisted.selectedFlow ?? null);
  const [refreshKey, setRefreshKey] = useState(0);
  const contentRef = useRef<HTMLDivElement>(null);
  const scrollTopsRef = useRef<Record<string, number>>(persisted.scrollTops || {});

  const { scalars, text, figures, images, cards, isLoading, refresh: refreshData } =
    useEvents(selectedRuns, activeTab);

  const { groups, setGroups, addToGroup } = useCompareGroups();

  useEffect(() => {
    if (persisted.rootDir) return;
    fetchConfig()
      .then((config) => { if (config.logDir) setRootDir(config.logDir); })
      .catch((e) => console.error("Failed to fetch config:", e));
  }, []);

  useEffect(() => {
    persistState({ rootDir, selectedRuns, activeTab, selectedFlow, scrollTops: scrollTopsRef.current });
  }, [rootDir, selectedRuns, activeTab, selectedFlow]);

  // Save scroll position for the current tab on scroll and beforeunload.
  useEffect(() => {
    const el = contentRef.current;
    if (!el) return;
    const onScroll = () => { scrollTopsRef.current[activeTab] = el.scrollTop; };
    el.addEventListener("scroll", onScroll, { passive: true });
    const onBeforeUnload = () => {
      scrollTopsRef.current[activeTab] = el.scrollTop;
      persistState({ rootDir, selectedRuns, activeTab, selectedFlow, scrollTops: scrollTopsRef.current });
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => { el.removeEventListener("scroll", onScroll); window.removeEventListener("beforeunload", onBeforeUnload); };
  }, [rootDir, selectedRuns, activeTab, selectedFlow]);

  // Restore scroll position when switching tabs or after loading finishes.
  useEffect(() => {
    if (!isLoading && contentRef.current) {
      contentRef.current.scrollTop = scrollTopsRef.current[activeTab] || 0;
    }
  }, [isLoading, activeTab]);

  const handleRefresh = useCallback(() => {
    // Capture scroll before data re-fetches
    if (contentRef.current) {
      scrollTopsRef.current[activeTab] = contentRef.current.scrollTop;
    }
    setRefreshKey((k) => k + 1);
    refreshData();
  }, [refreshData, activeTab]);

  const switchTab = useCallback((tab: Tab) => {
    // Capture current tab's scroll before switching
    if (contentRef.current) {
      scrollTopsRef.current[activeTab] = contentRef.current.scrollTop;
    }
    setActiveTab(tab);
  }, [activeTab]);

  const tabs: Tab[] = ["scalars", "text", "figures", "images", "cards", "compare", "graph"];

  return (
    <div className="flex h-screen bg-gray-50">
      <aside
        className="bg-white border-r border-gray-200 flex flex-col overflow-hidden"
        style={{ width: 288, minWidth: 200, maxWidth: 600, resize: "horizontal", overflow: "auto" }}
      >
        <header className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h1 className="text-xl font-semibold text-gray-800">Rinnsal</h1>
          <button onClick={handleRefresh} title="Refresh runs and data" className="p-1.5 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded transition-colors">
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" />
              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
            </svg>
          </button>
        </header>
        <div className="p-4 border-b border-gray-200">
          <label className="block text-sm font-medium text-gray-700 mb-1">Root Directory</label>
          <input type="text" value={rootDir} onChange={(e) => setRootDir(e.target.value)} placeholder="/path/to/runs" className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
        </div>
        <div className="flex-1 overflow-auto p-4">
          {activeTab === "graph" ? (
            <FlowSidebar
              rootDir={rootDir}
              selectedFlow={selectedFlow}
              onSelectFlow={setSelectedFlow}
              refreshKey={refreshKey}
            />
          ) : (
            <RunSelector rootDir={rootDir} selectedRuns={selectedRuns} onSelectionChange={setSelectedRuns} refreshKey={refreshKey} />
          )}
        </div>
      </aside>

      <main className="flex-1 flex flex-col overflow-hidden">
        <nav className="bg-white border-b border-gray-200 px-4">
          <div className="flex space-x-4">
            {tabs.map((tab) => (
              <button
                key={tab}
                onClick={() => switchTab(tab)}
                className={`py-3 px-1 border-b-2 text-sm font-medium capitalize transition-colors ${
                  activeTab === tab
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                }`}
              >
                {tab}
                {tab === "compare" && groups.length > 0 && (
                  <span className="ml-1 text-xs bg-blue-100 text-blue-700 px-1 rounded">{groups.length}</span>
                )}
              </button>
            ))}
          </div>
        </nav>

        <div
          ref={contentRef}
          className={`flex-1 overflow-auto relative ${activeTab === "graph" ? "p-0" : "p-4"}`}
        >
          {activeTab === "graph" ? (
            <FlowGraph
              rootDir={rootDir}
              flowName={selectedFlow}
              refreshKey={refreshKey}
              onOpenRun={(runPath) => {
                setSelectedRuns([runPath]);
                switchTab("scalars");
              }}
            />
          ) : activeTab !== "compare" && selectedRuns.length === 0 ? (
            <div className="text-center text-gray-500 mt-8">Select runs from the sidebar to view data.</div>
          ) : isLoading && activeTab !== "compare" && scalars.size === 0 && text.size === 0 && figures.size === 0 && images.size === 0 && cards.size === 0 ? (
            <div className="text-center text-gray-500 mt-8">Loading...</div>
          ) : (
            <>
              {activeTab === "scalars" && <ScalarChart data={scalars} compareGroups={groups} onAddToCompare={addToGroup} />}
              {activeTab === "text" && <TextLog data={text} />}
              {activeTab === "figures" && (
                <FigureViewer data={figures} selectedRuns={selectedRuns} compareGroups={groups} onAddToCompare={addToGroup} />
              )}
              {activeTab === "images" && (
                <ImageViewer data={images} selectedRuns={selectedRuns} compareGroups={groups} onAddToCompare={addToGroup} />
              )}
              {activeTab === "cards" && <CardViewer data={cards} />}
              {activeTab === "compare" && <CompareView groups={groups} onGroupsChange={setGroups} />}
            </>
          )}
        </div>
      </main>
    </div>
  );
}
