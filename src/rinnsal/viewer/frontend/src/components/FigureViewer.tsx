import { useMemo, useState } from "react";
import { FigureMetaData, figureImageUrl } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";

interface FigureViewerProps {
  data: Map<string, FigureMetaData>;
  selectedRuns: string[];
}

export default function FigureViewer({ data, selectedRuns }: FigureViewerProps) {
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
}

function FigureRunCard({ run, runPath, tag, figures, color }: FigureRunCardProps) {
  const [selectedIdx, setSelectedIdx] = useState(figures.length - 1);

  const runName = run.split("/").pop() || run;
  const currentFigure = figures[selectedIdx];
  const imageUrl = figureImageUrl(runPath, tag, currentFigure.it);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>{runName}</span>
        <span className="text-sm text-gray-500">Iteration: {currentFigure.it}</span>
      </div>

      {figures.length > 1 && (
        <div className="mb-3">
          <input type="range" min={0} max={figures.length - 1} value={selectedIdx} onChange={(e) => setSelectedIdx(parseInt(e.target.value))} className="w-full" />
        </div>
      )}

      <img
        src={imageUrl}
        alt={`${runName} - iteration ${currentFigure.it}`}
        className="max-w-full rounded"
        loading="lazy"
      />
    </div>
  );
}
