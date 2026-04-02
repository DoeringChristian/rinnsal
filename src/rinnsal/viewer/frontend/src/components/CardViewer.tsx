import { useMemo } from "react";
import { CardData } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";

interface CardViewerProps {
  data: Map<string, CardData>;
}

export default function CardViewer({ data }: CardViewerProps) {
  const allTasks = useMemo(() => {
    const tasks = new Set<string>();
    for (const runData of data.values()) {
      for (const task of Object.keys(runData)) tasks.add(task);
    }
    return Array.from(tasks).sort();
  }, [data]);

  if (allTasks.length === 0) {
    return <p className="text-gray-500 text-center mt-8">No cards logged in selected runs.</p>;
  }

  return (
    <div className="space-y-6">
      {allTasks.map((task) => (
        <CollapsibleSection key={task} title={task}>
          <div className="grid grid-cols-1 gap-4">
            {Array.from(data).map(([run, runData]) => {
              const cards = runData[task];
              if (!cards || cards.length === 0) return null;
              return <TaskRunCard key={run} run={run} cards={cards} color={getRunColor(run)} />;
            })}
          </div>
        </CollapsibleSection>
      ))}
    </div>
  );
}

interface CardItem {
  it: number;
  kind: string;
  title: string;
  content: string;
  image?: string; // base64
}

function TaskRunCard({ run, cards, color }: { run: string; cards: CardItem[]; color: string }) {
  const runName = run.split("/").pop() || run;
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center mb-3">
        <span className="font-medium" style={{ color }}>{runName}</span>
      </div>
      <div className="space-y-3">
        {cards.map((card, idx) => (
          <CardItemView key={idx} card={card} />
        ))}
      </div>
    </div>
  );
}

function CardItemView({ card }: { card: CardItem }) {
  if (card.kind === "image" && card.image) {
    return (
      <div className="border-l-4 border-green-400 pl-3">
        {card.title && <div className="text-sm font-medium text-gray-700 mb-1">{card.title}</div>}
        <img src={`data:image/png;base64,${card.image}`} alt={card.title || "Card image"} className="max-w-full rounded" />
      </div>
    );
  }

  if (card.kind === "table") {
    let tableData: { headers: string[] | null; rows: unknown[][] } | null = null;
    try { tableData = JSON.parse(card.content); } catch { /* ignore */ }

    if (tableData && tableData.rows) {
      return (
        <div className="border-l-4 border-purple-400 pl-3">
          {card.title && <div className="text-sm font-medium text-gray-700 mb-1">{card.title}</div>}
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              {tableData.headers && (
                <thead>
                  <tr className="bg-gray-50">
                    {tableData.headers.map((h, i) => (
                      <th key={i} className="px-2 py-1 text-left font-medium text-gray-700 border-b">{String(h)}</th>
                    ))}
                  </tr>
                </thead>
              )}
              <tbody>
                {tableData.rows.map((row, i) => (
                  <tr key={i} className={i % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    {(row as unknown[]).map((cell, j) => (
                      <td key={j} className="px-2 py-1 border-b border-gray-100">{String(cell)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      );
    }
  }

  if (card.kind === "html") {
    return (
      <div className="border-l-4 border-orange-400 pl-3">
        {card.title && <div className="text-sm font-medium text-gray-700 mb-1">{card.title}</div>}
        <div className="text-sm" dangerouslySetInnerHTML={{ __html: card.content }} />
      </div>
    );
  }

  // Default: text
  return (
    <div className="border-l-4 border-blue-400 pl-3">
      {card.title && <div className="text-sm font-medium text-gray-700 mb-1">{card.title}</div>}
      <div className="text-sm text-gray-600 whitespace-pre-wrap">{card.content}</div>
    </div>
  );
}
