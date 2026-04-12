import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import ScalarChart from "./ScalarChart";
import TextLog from "./TextLog";
import FigureViewer from "./FigureViewer";
import ImageViewer from "./ImageViewer";
import CardViewer from "./CardViewer";
import {
  fetchScalars,
  fetchText,
  fetchFiguresMeta,
  fetchImagesMeta,
  fetchCards,
  ScalarData,
  TextData,
  FigureMetaData,
  ImageMetaData,
  CardData,
} from "../lib/api";
import type { CompareSlot } from "./CompareView";

type DetailTab = "scalars" | "text" | "figures" | "images" | "cards" | "console" | "system";

interface RunDetailViewProps {
  runPath: string;
  runLabel?: string;
  onBack: () => void;
  compareGroups?: any[];
  onAddToCompare?: (slot: CompareSlot, groupId: number | null) => void;
}

/** Tags that are special-purpose and shown in Console/System tabs. */
const CONSOLE_TAG_SUFFIXES = ["/stdout", "/stderr"];
const SYSTEM_TAGS = ["system/info"];
const FLOW_TAGS = ["flow/info", "flow/summary"];

function isSpecialTag(tag: string): boolean {
  if (SYSTEM_TAGS.includes(tag) || FLOW_TAGS.includes(tag)) return true;
  for (const suffix of CONSOLE_TAG_SUFFIXES) {
    if (tag.endsWith(suffix)) return true;
  }
  if (tag.match(/^task\/.*\/status$/)) return true;
  return false;
}

export default function RunDetailView({
  runPath,
  runLabel,
  onBack,
  compareGroups = [],
  onAddToCompare,
}: RunDetailViewProps) {
  const [activeTab, setActiveTab] = useState<DetailTab>("scalars");
  const [scalars, setScalars] = useState<Map<string, ScalarData>>(new Map());
  const [text, setText] = useState<Map<string, TextData>>(new Map());
  const [figures, setFigures] = useState<Map<string, FigureMetaData>>(new Map());
  const [images, setImages] = useState<Map<string, ImageMetaData>>(new Map());
  const [cards, setCards] = useState<Map<string, CardData>>(new Map());
  const [isLoading, setIsLoading] = useState(false);
  const loadedRef = useRef<Set<string>>(new Set());

  // The text tab needs to be loaded for console/system too
  const textNeededTabs: DetailTab[] = ["text", "console", "system"];

  const loadTab = useCallback(
    async (tab: DetailTab, force = false) => {
      const loadKey = textNeededTabs.includes(tab) ? "text" : tab;
      const key = `${runPath}:${loadKey}`;
      if (!force && loadedRef.current.has(key)) return;
      loadedRef.current.add(key);
      setIsLoading(true);
      try {
        switch (loadKey) {
          case "scalars": {
            const d = await fetchScalars(runPath);
            setScalars((prev) => new Map(prev).set(runPath, d));
            break;
          }
          case "text": {
            const d = await fetchText(runPath);
            setText((prev) => new Map(prev).set(runPath, d));
            break;
          }
          case "figures": {
            const d = await fetchFiguresMeta(runPath);
            setFigures((prev) => new Map(prev).set(runPath, d));
            break;
          }
          case "images": {
            const d = await fetchImagesMeta(runPath);
            setImages((prev) => new Map(prev).set(runPath, d));
            break;
          }
          case "cards": {
            const d = await fetchCards(runPath);
            setCards((prev) => new Map(prev).set(runPath, d));
            break;
          }
        }
      } catch (e) {
        console.error(`Failed to load ${tab}:`, e);
      } finally {
        setIsLoading(false);
      }
    },
    [runPath]
  );

  // Load current tab on mount and tab switch
  useEffect(() => {
    loadTab(activeTab);
  }, [activeTab, loadTab]);

  // Auto-refresh the active tab every 5 seconds for live data
  useEffect(() => {
    const interval = setInterval(() => {
      loadTab(activeTab, true);
    }, 5000);
    return () => clearInterval(interval);
  }, [activeTab, loadTab]);

  // Reset loaded cache when run changes
  useEffect(() => {
    loadedRef.current.clear();
    setScalars(new Map());
    setText(new Map());
    setFigures(new Map());
    setImages(new Map());
    setCards(new Map());
  }, [runPath]);

  // Split text data into user text, console output, and system info
  const { userText, consoleEntries, systemInfo } = useMemo(() => {
    const runText = text.get(runPath);
    if (!runText) return { userText: new Map<string, TextData>(), consoleEntries: [] as { task: string; stream: string; entries: { it: number; value: string }[] }[], systemInfo: "" };

    // User text: everything that isn't a special tag
    const filtered: TextData = {};
    for (const [tag, entries] of Object.entries(runText)) {
      if (!isSpecialTag(tag)) {
        filtered[tag] = entries;
      }
    }

    // Console: stdout/stderr grouped by task
    const consoleMap = new Map<string, { stdout: { it: number; value: string }[]; stderr: { it: number; value: string }[] }>();
    for (const [tag, entries] of Object.entries(runText)) {
      if (tag.endsWith("/stdout") || tag.endsWith("/stderr")) {
        const parts = tag.split("/");
        const stream = parts.pop()!; // "stdout" or "stderr"
        const taskName = parts.join("/");
        if (!consoleMap.has(taskName)) consoleMap.set(taskName, { stdout: [], stderr: [] });
        const bucket = consoleMap.get(taskName)!;
        if (stream === "stdout") bucket.stdout.push(...entries);
        else bucket.stderr.push(...entries);
      }
    }
    const consoleParts: { task: string; stream: string; entries: { it: number; value: string }[] }[] = [];
    for (const [task, { stdout, stderr }] of consoleMap) {
      if (stdout.length > 0) consoleParts.push({ task, stream: "stdout", entries: stdout });
      if (stderr.length > 0) consoleParts.push({ task, stream: "stderr", entries: stderr });
    }

    // System info
    let sysInfo = "";
    const sysEntries = runText["system/info"];
    if (sysEntries && sysEntries.length > 0) {
      sysInfo = sysEntries[sysEntries.length - 1].value;
    }
    // Add flow info
    const flowEntries = runText["flow/info"];
    if (flowEntries && flowEntries.length > 0) {
      sysInfo = flowEntries[flowEntries.length - 1].value + "\n" + sysInfo;
    }
    const summaryEntries = runText["flow/summary"];
    if (summaryEntries && summaryEntries.length > 0) {
      sysInfo += "\n\nresult: " + summaryEntries[summaryEntries.length - 1].value;
    }

    return {
      userText: new Map<string, TextData>([[runPath, filtered]]),
      consoleEntries: consoleParts,
      systemInfo: sysInfo,
    };
  }, [text, runPath]);

  const tabs: { key: DetailTab; label: string }[] = [
    { key: "scalars", label: "Scalars" },
    { key: "text", label: "Text" },
    { key: "figures", label: "Figures" },
    { key: "images", label: "Images" },
    { key: "cards", label: "Cards" },
    { key: "console", label: "Console" },
    { key: "system", label: "System" },
  ];

  const runName = runLabel || runPath.split("/").pop() || runPath;

  return (
    <div className="flex flex-col h-full">
      {/* Header with back + run name + sub-tabs */}
      <div className="bg-white border-b border-gray-200 px-4 shrink-0">
        <div className="flex items-center gap-3 py-2">
          <button
            onClick={onBack}
            className="text-gray-500 hover:text-gray-700 text-sm flex items-center gap-1 shrink-0"
          >
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <polyline points="15 18 9 12 15 6" />
            </svg>
            Back
          </button>
          <span className="text-sm font-medium text-gray-800 truncate">
            {runName}
          </span>
        </div>
        <div className="flex space-x-4">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`py-2 px-1 border-b-2 text-sm font-medium transition-colors ${
                activeTab === tab.key
                  ? "border-blue-500 text-blue-600"
                  : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-4">
        {isLoading && scalars.size === 0 && text.size === 0 ? (
          <div className="text-center text-gray-500 mt-8">Loading...</div>
        ) : (
          <>
            {activeTab === "scalars" && (
              <ScalarChart
                data={scalars}
                compareGroups={compareGroups}
                onAddToCompare={onAddToCompare}
              />
            )}
            {activeTab === "text" && <TextLog data={userText} />}
            {activeTab === "figures" && (
              <FigureViewer
                data={figures}
                selectedRuns={[runPath]}
                compareGroups={compareGroups}
                onAddToCompare={onAddToCompare || (() => {})}
              />
            )}
            {activeTab === "images" && (
              <ImageViewer
                data={images}
                selectedRuns={[runPath]}
                compareGroups={compareGroups}
                onAddToCompare={onAddToCompare || (() => {})}
              />
            )}
            {activeTab === "cards" && <CardViewer data={cards} />}
            {activeTab === "console" && (
              <ConsoleView entries={consoleEntries} />
            )}
            {activeTab === "system" && (
              <SystemView info={systemInfo} runPath={runPath} />
            )}
          </>
        )}
      </div>
    </div>
  );
}

// ─── Console View ───────────────────────────────────────────────

interface ConsoleEntry {
  task: string;
  stream: string;
  entries: { it: number; value: string }[];
}

function ConsoleView({ entries }: { entries: ConsoleEntry[] }) {
  if (entries.length === 0) {
    return (
      <p className="text-gray-500 text-center mt-8">
        No console output captured.
      </p>
    );
  }

  return (
    <div className="space-y-4 max-w-4xl mx-auto">
      {entries.map(({ task, stream, entries: lines }) => (
        <div
          key={`${task}-${stream}`}
          className="bg-white rounded-lg border border-gray-200 overflow-hidden"
        >
          <div className="flex items-center gap-2 px-4 py-2 bg-gray-50 border-b border-gray-200">
            <span className="font-medium text-sm text-gray-700">{task}</span>
            <span
              className={`text-xs px-1.5 py-0.5 rounded font-mono ${
                stream === "stderr"
                  ? "bg-red-100 text-red-700"
                  : "bg-gray-100 text-gray-600"
              }`}
            >
              {stream}
            </span>
          </div>
          <pre className="px-4 py-3 text-xs font-mono text-gray-800 bg-gray-900 text-gray-100 overflow-auto max-h-96 whitespace-pre-wrap">
            {lines.map((l) => l.value).join("")}
          </pre>
        </div>
      ))}
    </div>
  );
}

// ─── System View ────────────────────────────────────────────────

function SystemView({ info, runPath }: { info: string; runPath: string }) {
  if (!info) {
    return (
      <p className="text-gray-500 text-center mt-8">
        No system information recorded.
      </p>
    );
  }

  const lines = info.split("\n").filter((l) => l.trim());

  return (
    <div className="max-w-2xl mx-auto">
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-2 bg-gray-50 border-b border-gray-200">
          <span className="font-medium text-sm text-gray-700">
            Run Environment
          </span>
        </div>
        <div className="divide-y divide-gray-100">
          {lines.map((line, i) => {
            const colonIdx = line.indexOf(":");
            if (colonIdx > 0) {
              const key = line.slice(0, colonIdx).trim();
              const value = line.slice(colonIdx + 1).trim();
              return (
                <div key={i} className="flex px-4 py-2 text-sm">
                  <span className="text-gray-500 w-32 shrink-0">{key}</span>
                  <span className="text-gray-800 font-mono text-xs break-all">
                    {value}
                  </span>
                </div>
              );
            }
            return (
              <div key={i} className="px-4 py-2 text-sm text-gray-800">
                {line}
              </div>
            );
          })}
        </div>
        <div className="px-4 py-2 bg-gray-50 border-t border-gray-200">
          <span className="text-xs text-gray-400 font-mono break-all">
            {runPath}
          </span>
        </div>
      </div>
    </div>
  );
}
