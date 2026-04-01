import { useState } from "react";

interface CollapsibleSectionProps {
  title: string;
  defaultExpanded?: boolean;
  children: React.ReactNode;
}

export function CollapsibleSection({
  title,
  defaultExpanded = true,
  children,
}: CollapsibleSectionProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <div className="space-y-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center space-x-2 text-lg font-semibold text-gray-800 hover:text-gray-600 transition-colors"
      >
        <span className="text-sm text-gray-500 w-4">
          {expanded ? "\u25BC" : "\u25B6"}
        </span>
        <span>{title}</span>
      </button>
      {expanded && children}
    </div>
  );
}
