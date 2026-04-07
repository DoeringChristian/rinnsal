import { useMemo, useState } from "react";
import { ImageMetaData, imageUrl } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";
import { CompareGroup, CompareSlot, AddToCompareButton, setDragSlot } from "./CompareView";

interface ImageViewerProps {
  data: Map<string, ImageMetaData>;
  selectedRuns: string[];
  compareGroups: CompareGroup[];
  onAddToCompare: (slot: CompareSlot, groupId: number | null) => void;
}

export default function ImageViewer({ data, selectedRuns, compareGroups, onAddToCompare }: ImageViewerProps) {
  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const runData of data.values()) {
      for (const tag of Object.keys(runData)) tags.add(tag);
    }
    return Array.from(tags).sort();
  }, [data]);

  if (allTags.length === 0) {
    return <p className="text-gray-500 text-center mt-8">No images logged in selected runs.</p>;
  }

  return (
    <div className="space-y-6">
      {allTags.map((tag) => (
        <CollapsibleSection key={tag} title={tag}>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {Array.from(data).map(([run, runData]) => {
              const imgs = runData[tag];
              if (!imgs || imgs.length === 0) return null;
              return (
                <ImageRunCard
                  key={run}
                  run={run}
                  runPath={selectedRuns.find((r) => r === run) || run}
                  tag={tag}
                  images={imgs}
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

interface ImageRunCardProps {
  run: string;
  runPath: string;
  tag: string;
  images: { it: number; width: number; height: number }[];
  color: string;
  compareGroups: CompareGroup[];
  onAddToCompare: (slot: CompareSlot, groupId: number | null) => void;
}

function ImageRunCard({ run, runPath, tag, images, color, compareGroups, onAddToCompare }: ImageRunCardProps) {
  const [selectedIdx, setSelectedIdx] = useState(images.length - 1);
  const runName = run.split("/").pop() || run;
  const current = images[selectedIdx];
  const imgSrc = imageUrl(runPath, tag, current.it);

  const slot: CompareSlot = {
    type: "image", run, tag, iterations: images.map((i) => i.it),
    linked: true, localIt: images[images.length - 1].it,
  };

  const handleDragStart = (e: React.DragEvent) => {
    e.dataTransfer.setData("application/json", JSON.stringify(slot));
    e.dataTransfer.effectAllowed = "copy";
    setDragSlot(slot);
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 cursor-grab active:cursor-grabbing" draggable onDragStart={handleDragStart}>
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>{runName}</span>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">it:{current.it}</span>
          <AddToCompareButton groups={compareGroups} onAdd={(gid) => onAddToCompare(slot, gid)} />
        </div>
      </div>
      {images.length > 1 && (
        <div className="mb-3">
          <input type="range" min={0} max={images.length - 1} value={selectedIdx} onChange={(e) => setSelectedIdx(parseInt(e.target.value))} className="w-full" />
        </div>
      )}
      <img src={imgSrc} alt={`${runName} - ${tag} @ ${current.it}`} className="max-w-full rounded" loading="lazy" />
    </div>
  );
}
