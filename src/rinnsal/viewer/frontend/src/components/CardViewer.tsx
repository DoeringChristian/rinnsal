import { useMemo } from "react";
import { GroupedEvents, CardEvent } from "../lib/events";
import { getRunColor } from "./RunSelector";

interface CardViewerProps {
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

export default function CardViewer({ events, selectedRuns }: CardViewerProps) {
  // Collect all task names across all runs
  const allTasks = useMemo(() => {
    const tasks = new Set<string>();
    for (const grouped of events.values()) {
      for (const task of grouped.cards.keys()) {
        tasks.add(task);
      }
    }
    return Array.from(tasks).sort();
  }, [events]);

  if (allTasks.length === 0) {
    return (
      <p className="text-gray-500 text-center mt-8">
        No cards logged in selected runs.
      </p>
    );
  }

  return (
    <div className="space-y-6">
      {allTasks.map((task) => (
        <TaskCardSection
          key={task}
          task={task}
          events={events}
          selectedRuns={selectedRuns}
        />
      ))}
    </div>
  );
}

interface TaskCardSectionProps {
  task: string;
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

function TaskCardSection({ task, events, selectedRuns }: TaskCardSectionProps) {
  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-gray-800">{task}</h3>
      <div className="grid grid-cols-1 gap-4">
        {selectedRuns.map((run) => {
          const grouped = events.get(run);
          if (!grouped) return null;

          const cards = grouped.cards.get(task);
          if (!cards || cards.length === 0) return null;

          return (
            <TaskRunCard
              key={run}
              run={run}
              cards={cards}
              color={getRunColor(run, selectedRuns)}
            />
          );
        })}
      </div>
    </div>
  );
}

interface TaskRunCardProps {
  run: string;
  cards: CardEvent[];
  color: string;
}

function TaskRunCard({ run, cards, color }: TaskRunCardProps) {
  const runName = run.split("/").pop() || run;

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center mb-3">
        <span className="font-medium" style={{ color }}>
          {runName}
        </span>
      </div>

      <div className="space-y-3">
        {cards.map((card, idx) => (
          <CardItemView key={idx} card={card} />
        ))}
      </div>
    </div>
  );
}

interface CardItemViewProps {
  card: CardEvent;
}

function CardItemView({ card }: CardItemViewProps) {
  // Render based on card kind
  if (card.kind === "text") {
    return (
      <div className="border-l-4 border-blue-400 pl-3">
        {card.title && (
          <div className="text-sm font-medium text-gray-700 mb-1">
            {card.title}
          </div>
        )}
        <div className="text-sm text-gray-600 whitespace-pre-wrap">
          {card.content}
        </div>
      </div>
    );
  }

  if (card.kind === "image") {
    const imageUrl = useMemo(() => {
      if (!card.image || card.image.length === 0) return null;
      const copy = new Uint8Array(card.image);
      const blob = new Blob([copy], { type: "image/png" });
      return URL.createObjectURL(blob);
    }, [card.image]);

    return (
      <div className="border-l-4 border-green-400 pl-3">
        {card.title && (
          <div className="text-sm font-medium text-gray-700 mb-1">
            {card.title}
          </div>
        )}
        {imageUrl ? (
          <img src={imageUrl} alt={card.title || "Card image"} className="max-w-full rounded" />
        ) : (
          <div className="bg-gray-100 rounded p-4 text-center text-gray-500 text-sm">
            No image data
          </div>
        )}
      </div>
    );
  }

  if (card.kind === "table") {
    // Parse table data from JSON content
    let tableData: { headers: string[] | null; rows: unknown[][] } | null = null;
    try {
      tableData = JSON.parse(card.content);
    } catch {
      // Not valid JSON, show as text
    }

    if (tableData && tableData.rows) {
      return (
        <div className="border-l-4 border-purple-400 pl-3">
          {card.title && (
            <div className="text-sm font-medium text-gray-700 mb-1">
              {card.title}
            </div>
          )}
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              {tableData.headers && (
                <thead>
                  <tr className="bg-gray-50">
                    {tableData.headers.map((h, i) => (
                      <th key={i} className="px-2 py-1 text-left font-medium text-gray-700 border-b">
                        {String(h)}
                      </th>
                    ))}
                  </tr>
                </thead>
              )}
              <tbody>
                {tableData.rows.map((row, i) => (
                  <tr key={i} className={i % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    {(row as unknown[]).map((cell, j) => (
                      <td key={j} className="px-2 py-1 border-b border-gray-100">
                        {String(cell)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      );
    }

    // Fallback to text display
    return (
      <div className="border-l-4 border-purple-400 pl-3">
        {card.title && (
          <div className="text-sm font-medium text-gray-700 mb-1">
            {card.title}
          </div>
        )}
        <pre className="text-sm text-gray-600 overflow-x-auto">{card.content}</pre>
      </div>
    );
  }

  if (card.kind === "html") {
    return (
      <div className="border-l-4 border-orange-400 pl-3">
        {card.title && (
          <div className="text-sm font-medium text-gray-700 mb-1">
            {card.title}
          </div>
        )}
        <div
          className="text-sm"
          dangerouslySetInnerHTML={{ __html: card.content }}
        />
      </div>
    );
  }

  // Unknown kind - show as text
  return (
    <div className="border-l-4 border-gray-400 pl-3">
      {card.title && (
        <div className="text-sm font-medium text-gray-700 mb-1">
          {card.title}
        </div>
      )}
      <div className="text-sm text-gray-600">{card.content}</div>
    </div>
  );
}
