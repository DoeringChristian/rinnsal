import { useMemo, useState } from "react";
import { FigureMetaData, figureImageUrl } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";
import { CompareGroup, CompareSlot, AddToCompareButton, setDragSlot } from "./CompareView";
import LazyImage from "./LazyImage";

interface FigureViewerProps {
  data: Map<string, FigureMetaData>;
  selectedRuns: string[];
  compareGroups: CompareGroup[];
  onAddToCompare: (slot: CompareSlot, groupId: number | null) => void;
}

export default function FigureViewer({ data, selectedRuns, compareGroups, onAddToCompare }: FigureViewerProps) {
  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const runData of data.values()) {
      for (const tag of Object.keys(runData)) tags.add(tag);
    }
    return Array.from(tags).sort();
  }, [data]);

  if (allTags.length === 0) {
    return <p className="text-gray-500 text-center mt-8">No figures logged in selected runs.</p>;
  }

  return (
    <div className="space-y-6">
      {allTags.map((tag) => (
        <CollapsibleSection key={tag} title={tag}>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {Array.from(data).map(([run, runData]) => {
              const figs = runData[tag];
              if (!figs || figs.length === 0) return null;
              return (
                <FigureRunCard
                  key={run}
                  run={run}
                  runPath={selectedRuns.find((r) => r === run) || run}
                  tag={tag}
                  figures={figs}
                  color={getRunColor(run)}
                  compareGroups={compareGroups}
                  onAddToCompare={onAddToCompare}
                />
              );
            })}
          </div>
        </CollapsibleSection>
      ))}
    </div>
  );
}

interface FigureRunCardProps {
  run: string;
  runPath: string;
  tag: string;
  figures: { it: number }[];
  color: string;
  compareGroups: CompareGroup[];
  onAddToCompare: (slot: CompareSlot, groupId: number | null) => void;
}

function FigureRunCard({ run, runPath, tag, figures, color, compareGroups, onAddToCompare }: FigureRunCardProps) {
  // Store selected iteration (not index) so position survives data refreshes.
  // null = "follow latest"
  const storageKey = `rinnsal-fig-it:${runPath}:${tag}`;
  const [selectedIt, setSelectedIt] = useState<number | null>(() => {
    try {
      const raw = sessionStorage.getItem(storageKey);
      return raw !== null ? Number(raw) : null;
    } catch { return null; }
  });
  const [sliderActive, setSliderActive] = useState(false);
  const runName = run.split("/").pop() || run;

  // During a drag the slider emits ``input`` events on every
  // intermediate value — updating ``src`` on each would queue hundreds
  // of requests. We track a transient ``draggingIdx`` for the label
  // and commit to ``selectedIt`` (which drives ``src``) only on
  // ``change`` (pointer release / keyboard commit).
  const [draggingIdx, setDraggingIdx] = useState<number | null>(null);

  let committedIdx: number;
  if (selectedIt === null) {
    committedIdx = figures.length - 1; // follow latest
  } else {
    const found = figures.findIndex((f) => f.it === selectedIt);
    committedIdx = found >= 0 ? found : figures.length - 1;
  }
  const displayIdx = draggingIdx ?? committedIdx;
  const currentFigure = figures[displayIdx];
  const committedFigure = figures[committedIdx];
  const imgUrl = figureImageUrl(runPath, tag, committedFigure.it);

  const handleSliderCommit = (newIdx: number) => {
    const it = figures[newIdx]?.it;
    const isLatest = newIdx >= figures.length - 1;
    const val = isLatest ? null : it;
    setSelectedIt(val);
    setDraggingIdx(null);
    try { sessionStorage.setItem(storageKey, val === null ? "" : String(val)); } catch {}
  };

  const slot: CompareSlot = {
    type: "figure", run, tag, iterations: figures.map((f) => f.it),
    linked: true, localIt: figures[figures.length - 1].it,
  };

  const handleDragStart = (e: React.DragEvent) => {
    e.dataTransfer.setData("application/json", JSON.stringify(slot));
    e.dataTransfer.effectAllowed = "copy";
    setDragSlot(slot);
  };

  return (
    <div
      className={`bg-white rounded-lg border border-gray-200 p-4 ${sliderActive ? "" : "cursor-grab active:cursor-grabbing"}`}
      draggable={!sliderActive}
      onDragStart={handleDragStart}
    >
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>{runName}</span>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">it:{currentFigure.it}</span>
          <AddToCompareButton groups={compareGroups} onAdd={(gid) => onAddToCompare(slot, gid)} />
        </div>
      </div>
      {figures.length > 1 && (
        <div className="mb-3" onMouseEnter={() => setSliderActive(true)} onMouseLeave={() => setSliderActive(false)}>
          <input
            type="range"
            min={0}
            max={figures.length - 1}
            value={displayIdx}
            onInput={(e) => setDraggingIdx(parseInt((e.target as HTMLInputElement).value))}
            onChange={(e) => handleSliderCommit(parseInt(e.target.value))}
            className="w-full"
            draggable={false}
          />
        </div>
      )}
      <LazyImage src={imgUrl} alt={`${runName} - ${tag} @ ${currentFigure.it}`} className="max-w-full rounded" />
    </div>
  );
}
