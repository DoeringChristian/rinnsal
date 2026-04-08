import { useMemo, useState } from "react";
import { FigureMetaData, figureImageUrl } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";
import { CompareGroup, CompareSlot, AddToCompareButton, setDragSlot } from "./CompareView";

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
  const [selectedIdx, setSelectedIdx] = useState(figures.length - 1);
  const [sliderActive, setSliderActive] = useState(false);
  const runName = run.split("/").pop() || run;
  const currentFigure = figures[selectedIdx];
  const imgUrl = figureImageUrl(runPath, tag, currentFigure.it);

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
          <input type="range" min={0} max={figures.length - 1} value={selectedIdx} onChange={(e) => setSelectedIdx(parseInt(e.target.value))} className="w-full" draggable={false} />
        </div>
      )}
      <img src={imgUrl} alt={`${runName} - ${tag} @ ${currentFigure.it}`} className="max-w-full rounded" loading="lazy" />
    </div>
  );
}
