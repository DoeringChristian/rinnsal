import { useMemo, useState } from "react";
import { GroupedEvents } from "../lib/events";
import { getRunColor } from "./RunSelector";

interface FigureViewerProps {
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

export default function FigureViewer({ events, selectedRuns }: FigureViewerProps) {
  // Collect all figure tags across all runs
  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const grouped of events.values()) {
      for (const tag of grouped.figures.keys()) {
        tags.add(tag);
      }
    }
    return Array.from(tags).sort();
  }, [events]);

  if (allTags.length === 0) {
    return (
      <p className="text-gray-500 text-center mt-8">
        No figures logged in selected runs.
      </p>
    );
  }

  return (
    <div className="space-y-6">
      {allTags.map((tag) => (
        <FigureTagSection
          key={tag}
          tag={tag}
          events={events}
          selectedRuns={selectedRuns}
        />
      ))}
    </div>
  );
}

interface FigureTagSectionProps {
  tag: string;
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

function FigureTagSection({ tag, events, selectedRuns }: FigureTagSectionProps) {
  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-gray-800">{tag}</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {selectedRuns.map((run) => {
          const grouped = events.get(run);
          if (!grouped) return null;

          const figures = grouped.figures.get(tag);
          if (!figures || figures.length === 0) return null;

          return (
            <FigureRunCard
              key={run}
              run={run}
              figures={figures}
              color={getRunColor(run, selectedRuns)}
            />
          );
        })}
      </div>
    </div>
  );
}

interface FigureRunCardProps {
  run: string;
  figures: { iteration: bigint; image: Uint8Array; data: Uint8Array; interactive: boolean }[];
  color: string;
}

function FigureRunCard({ run, figures, color }: FigureRunCardProps) {
  const [selectedIdx, setSelectedIdx] = useState(figures.length - 1);

  const runName = run.split("/").pop() || run;
  const currentFigure = figures[selectedIdx];

  // Convert image bytes to data URL
  const imageUrl = useMemo(() => {
    if (!currentFigure.image || currentFigure.image.length === 0) {
      return null;
    }
    // Create a new Uint8Array copy to ensure it's a standard ArrayBuffer
    const copy = new Uint8Array(currentFigure.image);
    const blob = new Blob([copy], { type: "image/png" });
    return URL.createObjectURL(blob);
  }, [currentFigure.image]);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>
          {runName}
        </span>
        <span className="text-sm text-gray-500">
          Iteration: {currentFigure.iteration.toString()}
        </span>
      </div>

      {figures.length > 1 && (
        <div className="mb-3">
          <input
            type="range"
            min={0}
            max={figures.length - 1}
            value={selectedIdx}
            onChange={(e) => setSelectedIdx(parseInt(e.target.value))}
            className="w-full"
          />
        </div>
      )}

      {imageUrl ? (
        <img
          src={imageUrl}
          alt={`${runName} - iteration ${currentFigure.iteration}`}
          className="max-w-full rounded"
        />
      ) : (
        <div className="bg-gray-100 rounded p-8 text-center text-gray-500">
          No image data
        </div>
      )}
    </div>
  );
}
