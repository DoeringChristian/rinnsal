import { useMemo, useState } from "react";
import { GroupedEvents } from "../lib/events";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";

interface TextLogProps {
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

export default function TextLog({ events, selectedRuns }: TextLogProps) {
  // Collect all text tags across all runs
  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const grouped of events.values()) {
      for (const tag of grouped.text.keys()) {
        tags.add(tag);
      }
    }
    return Array.from(tags).sort();
  }, [events]);

  if (allTags.length === 0) {
    return (
      <p className="text-gray-500 text-center mt-8">
        No text logged in selected runs.
      </p>
    );
  }

  return (
    <div className="space-y-6">
      {allTags.map((tag) => (
        <CollapsibleSection key={tag} title={tag}>
          <TextTagSection
            tag={tag}
            events={events}
            selectedRuns={selectedRuns}
          />
        </CollapsibleSection>
      ))}
    </div>
  );
}

interface TextTagSectionProps {
  tag: string;
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

function TextTagSection({ tag, events, selectedRuns }: TextTagSectionProps) {
  return (
    <div className="space-y-4">
      {selectedRuns.map((run) => {
        const grouped = events.get(run);
        if (!grouped) return null;

        const texts = grouped.text.get(tag);
        if (!texts || texts.length === 0) return null;

        return (
          <TextRunCard
            key={run}
            run={run}
            texts={texts}
            color={getRunColor(run)}
          />
        );
      })}
    </div>
  );
}

interface TextRunCardProps {
  run: string;
  texts: { iteration: bigint; value: string }[];
  color: string;
}

function TextRunCard({ run, texts, color }: TextRunCardProps) {
  const [selectedIdx, setSelectedIdx] = useState(texts.length - 1);
  const [copied, setCopied] = useState(false);

  const runName = run.split("/").pop() || run;
  const currentText = texts[selectedIdx];

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(currentText.value);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (e) {
      console.error("Failed to copy:", e);
    }
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>
          {runName}
        </span>
        <button
          onClick={handleCopy}
          className={`px-3 py-1 text-sm rounded border transition-colors ${
            copied
              ? "bg-green-100 border-green-300 text-green-700"
              : "bg-gray-50 border-gray-300 text-gray-600 hover:bg-gray-100"
          }`}
        >
          {copied ? "Copied!" : "Copy"}
        </button>
      </div>

      {texts.length > 1 && (
        <div className="mb-3">
          <label className="block text-sm text-gray-600 mb-1">
            Iteration: {currentText.iteration.toString()}
          </label>
          <input
            type="range"
            min={0}
            max={texts.length - 1}
            value={selectedIdx}
            onChange={(e) => setSelectedIdx(parseInt(e.target.value))}
            className="w-full"
          />
        </div>
      )}

      <pre className="bg-gray-50 p-3 rounded text-sm overflow-x-auto whitespace-pre-wrap font-mono">
        {currentText.value}
      </pre>
    </div>
  );
}
