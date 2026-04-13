import { useEffect, useMemo, useState, lazy, Suspense } from "react";
import {
  CardComponentPayload,
  CardIndexEntry,
  CardSnapshot,
  blobUrl,
  fetchCard,
} from "../lib/api";
import { CollapsibleSection } from "./CollapsibleSection";

// Lazy-loaded so the ~3MB Plotly bundle only ships when a Plotly card is opened.
const PlotlyComponent = lazy(() => import("./renderers/PlotlyRenderer"));

interface CardViewerProps {
  /** Per-run card indexes, keyed by run path. */
  data: Map<string, CardIndexEntry[]>;
}

export default function CardViewer({ data }: CardViewerProps) {
  // Group by (task, name) across runs so cards with the same identity
  // sit together; one row per (task, name).
  const grouped = useMemo(() => {
    const out = new Map<string, { task: string; name: string; runs: { run: string; entry: CardIndexEntry }[] }>();
    for (const [run, entries] of data) {
      for (const entry of entries) {
        const key = `${entry.task}\0${entry.name}`;
        if (!out.has(key)) out.set(key, { task: entry.task, name: entry.name, runs: [] });
        out.get(key)!.runs.push({ run, entry });
      }
    }
    return Array.from(out.values()).sort((a, b) =>
      `${a.task}/${a.name}`.localeCompare(`${b.task}/${b.name}`),
    );
  }, [data]);

  if (grouped.length === 0) {
    return <p className="text-gray-500 text-center mt-8">No cards in selected runs.</p>;
  }

  return (
    <div className="space-y-6">
      {grouped.map((g) => (
        <CollapsibleSection key={`${g.task}\0${g.name}`} title={g.task ? `${g.task} · ${g.name}` : g.name}>
          <div className="grid grid-cols-1 gap-4">
            {g.runs.map(({ run, entry }) => (
              <CardPanel key={run} run={run} entry={entry} />
            ))}
          </div>
        </CollapsibleSection>
      ))}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Card panel: one card from one run, with iteration slider
// ────────────────────────────────────────────────────────────────────

function CardPanel({ run, entry }: { run: string; entry: CardIndexEntry }) {
  const runName = run.split("/").pop() || run;
  const [snapIdx, setSnapIdx] = useState(entry.iterations.length - 1);
  const [snap, setSnap] = useState<CardSnapshot | null>(null);
  const [loading, setLoading] = useState(false);

  const it = entry.iterations[snapIdx] ?? null;

  useEffect(() => {
    if (it === null) return;
    let cancelled = false;
    setLoading(true);
    fetchCard(run, entry.name, entry.task, it)
      .then((s) => { if (!cancelled) setSnap(s); })
      .catch((e) => { if (!cancelled) console.error("fetchCard failed", e); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [run, entry.name, entry.task, it]);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium text-gray-700">{runName}</span>
        {entry.iterations.length > 1 && (
          <div className="flex items-center gap-2 text-xs text-gray-600">
            <span>iter</span>
            <input
              type="range"
              min={0}
              max={entry.iterations.length - 1}
              value={snapIdx}
              onChange={(e) => setSnapIdx(Number(e.target.value))}
              className="w-48"
            />
            <span className="font-mono w-12 text-right">{it}</span>
          </div>
        )}
      </div>
      {loading && !snap && <div className="text-sm text-gray-400">Loading…</div>}
      {snap && (
        <div className="space-y-3">
          {snap.components.map((c, i) => (
            <ComponentView key={i} run={run} component={c} />
          ))}
        </div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Per-kind renderers
// ────────────────────────────────────────────────────────────────────

function ComponentView({ run, component }: { run: string; component: CardComponentPayload }) {
  switch (component.kind) {
    case "markdown":
      return <MarkdownRenderer content={component.content || ""} />;
    case "text":
      return <pre className="text-sm bg-gray-50 p-2 rounded whitespace-pre-wrap">{component.content || ""}</pre>;
    case "scalar":
      return (
        <div className="text-sm">
          <span className="text-gray-500">{component.tag}: </span>
          <code className="font-mono">{component.value}</code>
        </div>
      );
    case "table":
      return <TableRenderer headersJson={component.headers_json || ""} rowsJson={component.rows_json || ""} />;
    case "code":
      return (
        <pre className="text-xs bg-gray-900 text-gray-100 p-3 rounded overflow-x-auto">
          <code>{component.source || ""}</code>
        </pre>
      );
    case "progress":
      return <ProgressRenderer value={component.value || 0} total={component.total || 1} label={component.label || ""} />;
    case "image":
    case "figure":
      return <ImageRenderer run={run} component={component} />;
    case "plotly":
      return (
        <Suspense fallback={<div className="text-sm text-gray-400">Loading plotly…</div>}>
          <PlotlyComponent run={run} component={component} />
        </Suspense>
      );
    case "artifact":
      return <ArtifactRenderer component={component} />;
    default:
      return <div className="text-xs text-gray-400">Unknown component: {component.kind}</div>;
  }
}

function MarkdownRenderer({ content }: { content: string }) {
  // Minimal renderer: preserve newlines, render bold/italics/code spans.
  // Avoids adding a heavy markdown lib for a small win; if more shape is
  // needed we can swap in react-markdown later.
  const html = useMemo(() => {
    const escape = (s: string) =>
      s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    let s = escape(content);
    s = s.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    s = s.replace(/\*([^*]+)\*/g, "<em>$1</em>");
    s = s.replace(/`([^`]+)`/g, "<code>$1</code>");
    s = s.replace(/^### (.*)$/gm, "<h3>$1</h3>");
    s = s.replace(/^## (.*)$/gm, "<h2>$1</h2>");
    s = s.replace(/^# (.*)$/gm, "<h1>$1</h1>");
    s = s.replace(/\n/g, "<br/>");
    return s;
  }, [content]);
  return <div className="text-sm prose prose-sm" dangerouslySetInnerHTML={{ __html: html }} />;
}

function TableRenderer({ headersJson, rowsJson }: { headersJson: string; rowsJson: string }) {
  const { headers, rows } = useMemo(() => {
    let h: string[] = [];
    let r: unknown[][] = [];
    try { h = JSON.parse(headersJson) || []; } catch { /* */ }
    try { r = JSON.parse(rowsJson) || []; } catch { /* */ }
    return { headers: h, rows: r };
  }, [headersJson, rowsJson]);
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        {headers.length > 0 && (
          <thead>
            <tr className="bg-gray-50">
              {headers.map((h, i) => (
                <th key={i} className="px-2 py-1 text-left font-medium text-gray-700 border-b">{String(h)}</th>
              ))}
            </tr>
          </thead>
        )}
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={i % 2 === 0 ? "bg-white" : "bg-gray-50"}>
              {row.map((cell, j) => (
                <td key={j} className="px-2 py-1 border-b border-gray-100">{String(cell)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ProgressRenderer({ value, total, label }: { value: number; total: number; label: string }) {
  const pct = total > 0 ? Math.min(100, (value / total) * 100) : 0;
  return (
    <div className="text-xs">
      <div className="flex justify-between mb-1">
        <span className="text-gray-700">{label}</span>
        <span className="text-gray-500">{value} / {total}</span>
      </div>
      <div className="w-full bg-gray-200 rounded h-2 overflow-hidden">
        <div className="bg-blue-500 h-2" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function ImageRenderer({ run, component }: { run: string; component: CardComponentPayload }) {
  let src: string | null = null;
  if (component.inline_b64) {
    src = `data:image/png;base64,${component.inline_b64}`;
  } else if (component.image_blob_hash) {
    src = blobUrl(run, component.image_blob_hash);
  } else if (component.blob_hash) {
    src = blobUrl(run, component.blob_hash);
  }
  if (!src) return <div className="text-xs text-gray-400">[image unavailable]</div>;
  return <img src={src} alt={component.tag || "image"} className="max-w-full rounded" />;
}

function ArtifactRenderer({ component }: { component: CardComponentPayload }) {
  return (
    <div className="text-sm">
      <div>
        <span className="font-medium text-gray-700">{component.type_name || "artifact"}</span>
        {component.description && (
          <span className="text-gray-500"> — </span>
        )}
        {component.description && (
          <span className="font-mono text-gray-600">{component.description}</span>
        )}
      </div>
    </div>
  );
}
