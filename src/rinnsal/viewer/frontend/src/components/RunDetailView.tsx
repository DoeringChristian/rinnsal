import { useState, useEffect, useCallback, useRef } from "react";
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

type DetailTab = "scalars" | "text" | "figures" | "images" | "cards";

interface RunDetailViewProps {
  runPath: string;
  runLabel?: string;
  onBack: () => void;
  compareGroups?: any[];
  onAddToCompare?: (slot: CompareSlot, groupId: number | null) => void;
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

  const loadTab = useCallback(
    async (tab: DetailTab) => {
      const key = `${runPath}:${tab}`;
      if (loadedRef.current.has(key)) return;
      loadedRef.current.add(key);
      setIsLoading(true);
      try {
        switch (tab) {
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

  // Reset loaded cache when run changes
  useEffect(() => {
    loadedRef.current.clear();
    setScalars(new Map());
    setText(new Map());
    setFigures(new Map());
    setImages(new Map());
    setCards(new Map());
  }, [runPath]);

  const tabs: { key: DetailTab; label: string }[] = [
    { key: "scalars", label: "Scalars" },
    { key: "text", label: "Text" },
    { key: "figures", label: "Figures" },
    { key: "images", label: "Images" },
    { key: "cards", label: "Cards" },
  ];

  const runName =
    runLabel || runPath.split("/").pop() || runPath;

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
            {activeTab === "text" && <TextLog data={text} />}
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
          </>
        )}
      </div>
    </div>
  );
}
