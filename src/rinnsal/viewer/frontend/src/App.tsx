import { useState, useEffect } from "react";
import RunSelector from "./components/RunSelector";
import ScalarChart from "./components/ScalarChart";
import TextLog from "./components/TextLog";
import FigureViewer from "./components/FigureViewer";
import CardViewer from "./components/CardViewer";
import { useEvents, Tab } from "./hooks/useEvents";
import { fetchConfig } from "./lib/api";

export default function App() {
  const [rootDir, setRootDir] = useState("");
  const [selectedRuns, setSelectedRuns] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState<Tab>("scalars");

  const { scalars, text, figures, cards, isLoading } = useEvents(
    selectedRuns,
    activeTab,
  );

  useEffect(() => {
    fetchConfig()
      .then((config) => {
        if (config.logDir) setRootDir(config.logDir);
      })
      .catch((e) => console.error("Failed to fetch config:", e));
  }, []);

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <aside className="w-72 bg-white border-r border-gray-200 flex flex-col">
        <header className="p-4 border-b border-gray-200">
          <h1 className="text-xl font-semibold text-gray-800">Rinnsal</h1>
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
        <div className="flex-1 overflow-auto p-4">
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
