import { useMemo, useState } from "react";
import { ImageMetaData, imageUrl } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";
import { CompareGroup, CompareSlot, AddToCompareButton, setDragSlot } from "./CompareView";
import LazyImage from "./LazyImage";

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
  // Store selected iteration (not index) so position survives data refreshes.
  // null = "follow latest"
  const storageKey = `rinnsal-img-it:${runPath}:${tag}`;
  const [selectedIt, setSelectedIt] = useState<number | null>(() => {
    try {
      const raw = sessionStorage.getItem(storageKey);
      return raw !== null && raw !== "" ? Number(raw) : null;
    } catch { return null; }
  });
  const [sliderActive, setSliderActive] = useState(false);
  const runName = run.split("/").pop() || run;

  // During a drag the slider emits ``input`` events on every
  // intermediate value — updating the real src on each would queue
  // hundreds of requests and render the wrong frame when older
  // responses land after the newer target. We track a transient
  // ``draggingIdx`` for visual feedback and only commit to
  // ``selectedIt`` (which drives src) on ``change`` (pointer release /
  // keyboard commit).
  const [draggingIdx, setDraggingIdx] = useState<number | null>(null);

  let committedIdx: number;
  if (selectedIt === null) {
    committedIdx = images.length - 1;
  } else {
    const found = images.findIndex((img) => img.it === selectedIt);
    committedIdx = found >= 0 ? found : images.length - 1;
  }
  const displayIdx = draggingIdx ?? committedIdx;
  const currentImg = images[displayIdx];
  // The image <img> src is driven by the *committed* idx, not the
  // drag idx — avoids mid-drag fetches. With Cache-Control: immutable
  // + ETag the browser serves the same URL from cache on revisit.
  const committedImg = images[committedIdx];
  const imgSrc = imageUrl(runPath, tag, committedImg.it);

  const handleSliderCommit = (newIdx: number) => {
    const it = images[newIdx]?.it;
    const isLatest = newIdx >= images.length - 1;
    const val = isLatest ? null : it;
    setSelectedIt(val);
    setDraggingIdx(null);
    try { sessionStorage.setItem(storageKey, val === null ? "" : String(val)); } catch {}
  };

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
    <div
      className={`bg-white rounded-lg border border-gray-200 p-4 ${sliderActive ? "" : "cursor-grab active:cursor-grabbing"}`}
      draggable={!sliderActive}
      onDragStart={handleDragStart}
    >
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>{runName}</span>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">it:{currentImg.it}</span>
          <AddToCompareButton groups={compareGroups} onAdd={(gid) => onAddToCompare(slot, gid)} />
        </div>
      </div>
      {images.length > 1 && (
        <div className="mb-3" onMouseEnter={() => setSliderActive(true)} onMouseLeave={() => setSliderActive(false)}>
          <input
            type="range"
            min={0}
            max={images.length - 1}
            value={displayIdx}
            onInput={(e) => setDraggingIdx(parseInt((e.target as HTMLInputElement).value))}
            onChange={(e) => handleSliderCommit(parseInt(e.target.value))}
            className="w-full"
            draggable={false}
          />
        </div>
      )}
      <LazyImage src={imgSrc} alt={`${runName} - ${tag} @ ${currentImg.it}`} className="max-w-full rounded" />
    </div>
  );
}
