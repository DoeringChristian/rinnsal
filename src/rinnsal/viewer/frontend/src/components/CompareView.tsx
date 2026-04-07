import { useMemo, useState, useCallback, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { figureImageUrl, imageUrl, fetchScalars, fetchText } from "../lib/api";
import { getRunColor } from "./RunSelector";

// ─── Types (exported for use by other components) ────────────────

export type SlotType = "figure" | "image" | "scalar" | "text" | "card";

export interface CompareSlot {
  type: SlotType;
  run: string;
  tag: string;
  iterations: number[];
  linked: boolean;
  localIt: number;
  width?: number;
  scalarPanelId?: number; // groups scalar slots into shared charts
}

export interface CompareGroup {
  id: number;
  name: string;
  slots: CompareSlot[];
  collapsed: boolean;
}

// ─── Helpers ─────────────────────────────────────────────────────

function closestIt(iterations: number[], target: number): number {
  let best = iterations[0];
  let bestDist = Math.abs(best - target);
  for (let i = 1; i < iterations.length; i++) {
    const dist = Math.abs(iterations[i] - target);
    if (dist < bestDist) { best = iterations[i]; bestDist = dist; }
  }
  return best;
}

let nextGroupId = 1;

const DRAG_KEY = "__rinnsal_drag_slot__";
export function setDragSlot(slot: CompareSlot) { (window as any)[DRAG_KEY] = slot; }
function getDragSlot(): CompareSlot | null { return (window as any)[DRAG_KEY] ?? null; }
function clearDragSlot() { delete (window as any)[DRAG_KEY]; }

function groupHasSlot(group: CompareGroup, slot: CompareSlot): boolean {
  return group.slots.some((s) => s.run === slot.run && s.tag === slot.tag && s.type === slot.type);
}

// ─── Persistence ─────────────────────────────────────────────────

const COMPARE_STORAGE_KEY = "rinnsal-compare-groups";

function loadGroups(): CompareGroup[] {
  try {
    const raw = sessionStorage.getItem(COMPARE_STORAGE_KEY);
    if (raw) {
      const groups: CompareGroup[] = JSON.parse(raw);
      for (const g of groups) { if (g.id >= nextGroupId) nextGroupId = g.id + 1; }
      return groups;
    }
  } catch { /* ignore */ }
  return [];
}

function saveGroups(groups: CompareGroup[]) {
  try { sessionStorage.setItem(COMPARE_STORAGE_KEY, JSON.stringify(groups)); } catch { /* ignore */ }
}

// ─── Slot Renderer ───────────────────────────────────────────────

function SlotContent({ slot, it }: { slot: CompareSlot; it: number }) {
  const runName = slot.run.split("/").pop() || slot.run;

  if (slot.type === "figure") {
    return <img src={figureImageUrl(slot.run, slot.tag, it)} alt={`${runName} / ${slot.tag} @ ${it}`} className="w-full h-auto rounded object-contain" loading="lazy" />;
  }
  if (slot.type === "image") {
    return <img src={imageUrl(slot.run, slot.tag, it)} alt={`${runName} / ${slot.tag} @ ${it}`} className="w-full h-auto rounded object-contain" loading="lazy" />;
  }
  if (slot.type === "scalar") {
    return null; // Rendered in scalar panels
  }
  if (slot.type === "text") {
    return <TextSlotContent run={slot.run} tag={slot.tag} it={it} />;
  }
  // card — show as text for now
  return <div className="text-xs text-gray-500">Card: {slot.tag} @ it:{it}</div>;
}

/** Renders multiple scalar series in one uPlot chart with iteration marker. */
function ScalarGroupChart({ slots, globalIt }: { slots: { run: string; tag: string; it: number }[]; globalIt: number }) {
  const [allData, setAllData] = useState<Map<string, { it: number; value: number }[]>>(new Map());
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const prevCountRef = useRef(0);

  // Fetch data for all scalar slots
  useEffect(() => {
    const toFetch = new Map<string, string[]>(); // run → [tags]
    for (const s of slots) {
      if (!toFetch.has(s.run)) toFetch.set(s.run, []);
      toFetch.get(s.run)!.push(s.tag);
    }
    Promise.all(
      Array.from(toFetch.entries()).map(async ([run, tags]) => {
        const d = await fetchScalars(run);
        return { run, tags, d };
      })
    ).then((results) => {
      const next = new Map<string, { it: number; value: number }[]>();
      for (const { run, tags, d } of results) {
        for (const tag of tags) {
          if (d[tag]) next.set(`${run}\0${tag}`, d[tag]);
        }
      }
      setAllData(next);
    }).catch(() => {});
  }, [slots.map((s) => `${s.run}\0${s.tag}`).join("\n")]);

  // Build and render chart
  useEffect(() => {
    if (allData.size === 0 || !containerRef.current) return;

    // Merge all x values
    const xSet = new Set<number>();
    for (const points of allData.values()) {
      for (const p of points) xSet.add(p.it);
    }
    const sortedX = Array.from(xSet).sort((a, b) => a - b);

    // Build aligned data
    const aligned: (number | null)[][] = [sortedX];
    const seriesCfg: uPlot.Series[] = [{ label: "Iteration" }];

    for (const s of slots) {
      const key = `${s.run}\0${s.tag}`;
      const points = allData.get(key);
      if (!points) { aligned.push(sortedX.map(() => null)); } else {
        const map = new Map(points.map((p) => [p.it, p.value]));
        aligned.push(sortedX.map((x) => map.get(x) ?? null) as (number | null)[]);
      }
      const runName = s.run.split("/").pop() || s.run;
      seriesCfg.push({
        label: `${runName} / ${s.tag}`,
        stroke: getRunColor(s.run),
        width: 2,
        spanGaps: true,
      });
    }

    const drawMarker = (u: uPlot) => {
      const ctx = u.ctx;
      const xPos = u.valToPos(globalIt, "x", true);
      if (xPos < u.bbox.left || xPos > u.bbox.left + u.bbox.width) return;
      ctx.save();
      ctx.strokeStyle = "#ef4444";
      ctx.lineWidth = 2;
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.moveTo(xPos, u.bbox.top);
      ctx.lineTo(xPos, u.bbox.top + u.bbox.height);
      ctx.stroke();
      ctx.restore();
    };

    const needsRecreate = !chartRef.current || slots.length !== prevCountRef.current;
    prevCountRef.current = slots.length;

    if (chartRef.current && !needsRecreate) {
      chartRef.current.setData(aligned as uPlot.AlignedData);
      chartRef.current.redraw();
      return;
    }

    if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
    containerRef.current.innerHTML = "";
    chartRef.current = new uPlot(
      {
        width: containerRef.current.clientWidth || 400,
        height: 250,
        scales: { x: { auto: true }, y: { auto: true } },
        axes: [
          { grid: { show: true, stroke: "#eee" }, values: (_u: uPlot, vals: number[]) => vals.map((v) => String(Math.round(v))) },
          { grid: { show: true, stroke: "#eee" } },
        ],
        series: seriesCfg,
        cursor: { show: true, focus: { prox: 30 } },
        focus: { alpha: 0.3 },
        hooks: { draw: [drawMarker] },
      },
      aligned as uPlot.AlignedData,
      containerRef.current,
    );

    return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
  }, [allData, slots.length]);

  // Redraw marker on iteration change
  useEffect(() => { if (chartRef.current) chartRef.current.redraw(); }, [globalIt]);

  if (allData.size === 0) return <div className="text-xs text-gray-400">Loading scalar data...</div>;

  return <div ref={containerRef} style={{ height: 250, overflow: "hidden" }} />;
}

function TextSlotContent({ run, tag, it }: { run: string; tag: string; it: number }) {
  const [data, setData] = useState<{ it: number; value: string }[] | null>(null);
  useEffect(() => {
    fetchText(run).then((d) => {
      if (d[tag]) setData(d[tag]);
    }).catch(() => {});
  }, [run, tag]);

  if (!data) return <div className="text-xs text-gray-400">Loading...</div>;
  const entry = data.find((d) => d.it === it) || data[data.length - 1];
  return (
    <pre className="text-xs bg-gray-50 p-2 rounded overflow-auto max-h-40 whitespace-pre-wrap">
      {entry?.value}
    </pre>
  );
}

// ─── Compare Group Panel ─────────────────────────────────────────

interface CompareGroupPanelProps {
  group: CompareGroup;
  onUpdate: (group: CompareGroup) => void;
  onDelete: () => void;
  onPopout: () => void;
  onDropSlot: (slot: CompareSlot) => void;
}

function CompareGroupPanel({ group, onUpdate, onDelete, onPopout, onDropSlot }: CompareGroupPanelProps) {
  const [overlay, setOverlay] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState(group.name);
  const [dragFromIdx, setDragFromIdx] = useState<number | null>(null);
  const [dragOverIdx, setDragOverIdx] = useState<number | null>(null);
  const [sliderActiveIdx, setSliderActiveIdx] = useState<number | null>(null);
  const [mergeTargetPid, setMergeTargetPid] = useState<number | null>(null);
  const [panelDropIdx, setPanelDropIdx] = useState<{ idx: number; side: "left" | "right" } | null>(null);
  const [cardDropIdx, setCardDropIdx] = useState<{ idx: number; side: "left" | "right" } | null>(null);
  const { slots } = group;

  const commitName = () => {
    const trimmed = editName.trim();
    if (trimmed && trimmed !== group.name) onUpdate({ ...group, name: trimmed });
    setEditing(false);
  };

  const linkedIterations = useMemo(() => {
    const set = new Set<number>();
    for (const s of slots) { if (s.linked) for (const it of s.iterations) set.add(it); }
    return Array.from(set).sort((a, b) => a - b);
  }, [slots]);

  const [globalIdx, setGlobalIdx] = useState(linkedIterations.length > 0 ? linkedIterations.length - 1 : 0);
  const safeGlobalIdx = Math.min(globalIdx, Math.max(0, linkedIterations.length - 1));
  const globalIt = linkedIterations[safeGlobalIdx] ?? 0;

  const removeSlot = (idx: number) => onUpdate({ ...group, slots: slots.filter((_, i) => i !== idx) });
  const toggleLink = (idx: number) => onUpdate({ ...group, slots: slots.map((s, i) => i === idx ? { ...s, linked: !s.linked } : s) });
  const setLocalIt = (idx: number, it: number) => onUpdate({ ...group, slots: slots.map((s, i) => i === idx ? { ...s, localIt: it } : s) });
  const toggleCollapsed = () => onUpdate({ ...group, collapsed: !group.collapsed });

  const handleDragOver = (e: React.DragEvent) => { e.preventDefault(); setDragOver(true); };
  const handleDragLeave = () => setDragOver(false);
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault(); setDragOver(false);
    let slotData: CompareSlot | null = null;
    try { const raw = e.dataTransfer.getData("application/json"); if (raw) slotData = JSON.parse(raw); } catch {}
    if (!slotData) slotData = getDragSlot();
    if (slotData) { onDropSlot(slotData); clearDragSlot(); }
  };

  const handleSlotDragStart = (e: React.DragEvent, slot: CompareSlot, idx: number) => {
    e.dataTransfer.setData("application/json", JSON.stringify(slot));
    e.dataTransfer.setData("text/x-group-id", String(group.id));
    e.dataTransfer.effectAllowed = "copy";
    setDragSlot(slot);
    setDragFromIdx(idx);
  };

  const handleSlotDrop = (e: React.DragEvent, targetIdx: number) => {
    e.preventDefault(); e.stopPropagation(); setDragOverIdx(null);
    const sourceGroupId = e.dataTransfer.getData("text/x-group-id");
    if (sourceGroupId === String(group.id) && dragFromIdx !== null && dragFromIdx !== targetIdx) {
      const newSlots = [...slots];
      const [moved] = newSlots.splice(dragFromIdx, 1);
      const insertIdx = targetIdx > dragFromIdx ? targetIdx - 1 : targetIdx;
      newSlots.splice(insertIdx, 0, moved);
      onUpdate({ ...group, slots: newSlots });
    } else {
      let slotData: CompareSlot | null = null;
      try { const raw = e.dataTransfer.getData("application/json"); if (raw) slotData = JSON.parse(raw); } catch {}
      if (!slotData) slotData = getDragSlot();
      if (slotData) { onDropSlot(slotData); clearDragSlot(); }
    }
    setDragFromIdx(null);
  };

  const handleSlotDragEnd = () => { setDragFromIdx(null); setDragOverIdx(null); setMergeTargetPid(null); setPanelDropIdx(null); setCardDropIdx(null); };

  // Shift+resize sync
  const shiftHeldRef = useRef(false);
  const slotRefsRef = useRef<Map<number, HTMLDivElement>>(new Map());
  const cleanupRef = useRef<(() => void) | null>(null);

  const containerRefCallback = useCallback((el: HTMLDivElement | null) => {
    if (cleanupRef.current) { cleanupRef.current(); cleanupRef.current = null; }
    if (!el) return;
    const ow = el.ownerDocument?.defaultView || window;
    const down = (e: KeyboardEvent) => { if (e.key === "Shift") shiftHeldRef.current = true; };
    const up = (e: KeyboardEvent) => { if (e.key === "Shift") shiftHeldRef.current = false; };
    ow.addEventListener("keydown", down); ow.addEventListener("keyup", up);

    let syncing = false;
    const RO = (ow as any).ResizeObserver || ResizeObserver;
    const observer = new RO((entries: ResizeObserverEntry[]) => {
      if (!shiftHeldRef.current || syncing) return;
      for (const entry of entries) {
        const tw = entry.target.getBoundingClientRect().width;
        syncing = true;
        slotRefsRef.current.forEach((el) => { if (el !== entry.target) el.style.width = `${Math.round(tw)}px`; });
        requestAnimationFrame(() => { syncing = false; });
        break;
      }
    });
    slotRefsRef.current.forEach((el) => observer.observe(el));

    const onMouseUp = () => {
      let changed = false;
      const updatedSlots = [...slots];
      slotRefsRef.current.forEach((el, idx) => {
        const w = Math.round(el.getBoundingClientRect().width);
        if (idx < updatedSlots.length && updatedSlots[idx].width !== w) {
          updatedSlots[idx] = { ...updatedSlots[idx], width: w };
          changed = true;
          if (shiftHeldRef.current) {
            for (let i = 0; i < updatedSlots.length; i++) updatedSlots[i] = { ...updatedSlots[i], width: w };
            slotRefsRef.current.forEach((o) => { if (o !== el) o.style.width = `${w}px`; });
          }
        }
      });
      if (changed) onUpdate({ ...group, slots: updatedSlots });
    };
    ow.addEventListener("mouseup", onMouseUp);

    cleanupRef.current = () => { ow.removeEventListener("keydown", down); ow.removeEventListener("keyup", up); ow.removeEventListener("mouseup", onMouseUp); observer.disconnect(); };
  }, [slots, group, onUpdate]);

  const setSlotRef = (idx: number, el: HTMLDivElement | null) => { if (el) slotRefsRef.current.set(idx, el); else slotRefsRef.current.delete(idx); };

  const content = (
    <div ref={containerRefCallback}>
      {!group.collapsed && linkedIterations.length > 1 && (
        <div className="flex items-center gap-2 mb-3">
          <span className="text-xs text-gray-500 shrink-0">it: {globalIt}</span>
          <input type="range" min={0} max={linkedIterations.length - 1} value={safeGlobalIdx} onChange={(e) => setGlobalIdx(parseInt(e.target.value))} className="flex-1" />
        </div>
      )}
      {!group.collapsed && (() => {
        // Build scalar panels map
        const scalarPanels = new Map<number, number[]>(); // panelId → [slot indices]
        slots.forEach((s, i) => {
          if (s.type !== "scalar") return;
          const pid = s.scalarPanelId ?? 0;
          if (!scalarPanels.has(pid)) scalarPanels.set(pid, []);
          scalarPanels.get(pid)!.push(i);
        });
        const renderedPanels = new Set<number>();

        return (
          <div className="flex flex-wrap items-start gap-2">
            {slots.map((slot, idx) => {
              // --- SCALAR: render as part of a panel ---
              if (slot.type === "scalar") {
                const pid = slot.scalarPanelId ?? 0;
                if (renderedPanels.has(pid)) return null; // already rendered
                renderedPanels.add(pid);

                const panelIndices = scalarPanels.get(pid) || [idx];
                const panelSlotData = panelIndices.map((i) => slots[i]);

                const lastPanelIdx = panelIndices[panelIndices.length - 1];

                return (
                  <div
                    key={`sp-${pid}`}
                    className="w-full bg-white rounded-lg border border-gray-200 p-3"
                    style={{
                      borderLeft: panelDropIdx?.idx === pid && panelDropIdx.side === "left" ? "3px solid #3b82f6" : undefined,
                      borderRight: panelDropIdx?.idx === pid && panelDropIdx.side === "right" ? "3px solid #3b82f6" : undefined,
                    }}
                    onDragOver={(e) => {
                      e.preventDefault();
                      if (mergeTargetPid !== null) return;
                      setCardDropIdx(null); // clear card-level indicator
                      const r = e.currentTarget.getBoundingClientRect();
                      const side = e.clientY < r.top + r.height / 2 ? "left" : "right";
                      setPanelDropIdx({ idx: pid, side });
                    }}
                    onDragLeave={() => setPanelDropIdx(null)}
                    onDrop={(e) => {
                      e.preventDefault();
                      const side = panelDropIdx?.side || "left";
                      setPanelDropIdx(null);
                      if (mergeTargetPid !== null) return;

                      const dragged = getDragSlot();
                      // Scalar card from THIS panel onto chart area = separate
                      if (dragged && dragged.type === "scalar" && (dragged.scalarPanelId ?? 0) === pid && dragFromIdx !== null && panelIndices.length > 1) {
                        clearDragSlot();
                        const maxPid = Math.max(0, ...slots.filter((x) => x.type === "scalar").map((x) => x.scalarPanelId ?? 0));
                        const newSlots = [...slots];
                        const [moved] = newSlots.splice(dragFromIdx, 1);
                        const movedWithNewPid = { ...moved, scalarPanelId: maxPid + 1 };
                        const insertAt = side === "left" ? Math.min(idx, newSlots.length) : Math.min(lastPanelIdx, newSlots.length);
                        newSlots.splice(insertAt, 0, movedWithNewPid);
                        onUpdate({ ...group, slots: newSlots });
                        setDragFromIdx(null);
                        return;
                      }

                      // Normal reorder
                      const insertAt = side === "left" ? idx : lastPanelIdx + 1;
                      handleSlotDrop(e, insertAt);
                    }}
                  >
                    {/* Drag handle for moving the whole panel */}
                    <div
                      className="cursor-grab active:cursor-grabbing text-gray-300 hover:text-gray-500 text-center text-xs mb-1 select-none"
                      draggable
                      onDragStart={(e) => {
                        e.stopPropagation();
                        e.dataTransfer.setData("text/x-panel-id", String(pid));
                        e.dataTransfer.setData("text/x-group-id", String(group.id));
                        e.dataTransfer.effectAllowed = "move";
                        setDragFromIdx(idx);
                      }}
                      onDragEnd={handleSlotDragEnd}
                    >
                      {"⋮⋮⋮"}
                    </div>
                    {/* Chart area — dropping a same-panel card here separates it */}
                    <div
                      onDragOver={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setCardDropIdx(null);
                        setMergeTargetPid(null);
                        const r = e.currentTarget.getBoundingClientRect();
                        const side = e.clientY < r.top + r.height / 2 ? "left" : "right";
                        setPanelDropIdx({ idx: pid, side });
                      }}
                      onDragLeave={() => setPanelDropIdx(null)}
                      onDrop={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        const side = panelDropIdx?.side || "left";
                        setPanelDropIdx(null);
                        setCardDropIdx(null);

                        const dragged = getDragSlot();
                        if (dragged && dragged.type === "scalar" && (dragged.scalarPanelId ?? 0) === pid && dragFromIdx !== null && panelIndices.length > 1) {
                          clearDragSlot();
                          const maxPid = Math.max(0, ...slots.filter((x) => x.type === "scalar").map((x) => x.scalarPanelId ?? 0));
                          const newSlots = [...slots];
                          const [moved] = newSlots.splice(dragFromIdx, 1);
                          const movedWithNewPid = { ...moved, scalarPanelId: maxPid + 1 };
                          const insertAt = side === "left" ? Math.min(idx, newSlots.length) : Math.min(lastPanelIdx, newSlots.length);
                          newSlots.splice(insertAt, 0, movedWithNewPid);
                          onUpdate({ ...group, slots: newSlots });
                          setDragFromIdx(null);
                          return;
                        }

                        // External drop: merge if scalar, otherwise reorder
                        if (dragged && dragged.type === "scalar") {
                          clearDragSlot();
                          const alreadyHere = panelSlotData.some((x) => x.run === dragged.run && x.tag === dragged.tag);
                          if (!alreadyHere) {
                            const updated = slots.map((s) =>
                              s.type === "scalar" && s.run === dragged.run && s.tag === dragged.tag
                                ? { ...s, scalarPanelId: pid } : s
                            );
                            onUpdate({ ...group, slots: updated });
                          }
                          setDragFromIdx(null);
                          return;
                        }

                        const insertAt = side === "left" ? idx : lastPanelIdx + 1;
                        handleSlotDrop(e, insertAt);
                      }}
                    >
                      <ScalarGroupChart
                        slots={panelSlotData.map((s) => ({
                          run: s.run, tag: s.tag,
                          it: s.linked ? closestIt(s.iterations, globalIt) : s.localIt,
                        }))}
                        globalIt={globalIt}
                      />
                    </div>
                    {/* Cards area — this is the drop target for merging scalars */}
                    <div
                      className={`flex flex-wrap gap-2 mt-2 p-1 rounded transition-colors ${mergeTargetPid === pid ? "bg-blue-50 ring-2 ring-blue-400" : ""}`}
                      onDragOver={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        // Only show merge highlight if dragging a scalar from a DIFFERENT panel
                        const dragged = getDragSlot();
                        if (dragged && dragged.type === "scalar" && (dragged.scalarPanelId ?? 0) !== pid) {
                          setMergeTargetPid(pid);
                        }
                      }}
                      onDragLeave={() => setMergeTargetPid(null)}
                      onDrop={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setMergeTargetPid(null);
                        setDragOverIdx(null);

                        const dragged = getDragSlot();
                        if (dragged && dragged.type === "scalar") {
                          clearDragSlot();
                          // Don't merge if already in this panel
                          const alreadyHere = panelSlotData.some((s) => s.run === dragged.run && s.tag === dragged.tag);
                          if (!alreadyHere) {
                            const updated = slots.map((s) =>
                              s.type === "scalar" && s.run === dragged.run && s.tag === dragged.tag
                                ? { ...s, scalarPanelId: pid } : s
                            );
                            onUpdate({ ...group, slots: updated });
                          }
                          setDragFromIdx(null);
                          return;
                        }
                      }}
                    >
                      {panelIndices.map((sIdx) => {
                        const s = slots[sIdx];
                        const runName = s.run.split("/").pop() || s.run;
                        const color = getRunColor(s.run);
                        const curIt = s.linked ? closestIt(s.iterations, globalIt) : s.localIt;
                        const cardShowLeft = cardDropIdx?.idx === sIdx && cardDropIdx.side === "left";
                        const cardShowRight = cardDropIdx?.idx === sIdx && cardDropIdx.side === "right";
                        return (
                          <div
                            key={`sc-${sIdx}`}
                            className="bg-gray-50 rounded border border-gray-200 px-2.5 py-1.5 cursor-grab active:cursor-grabbing hover:border-blue-300 transition-colors"
                            style={{
                              borderLeft: cardShowLeft ? "3px solid #3b82f6" : undefined,
                              borderRight: cardShowRight ? "3px solid #3b82f6" : undefined,
                            }}
                            draggable
                            onDragStart={(e) => {
                              e.stopPropagation();
                              e.dataTransfer.setData("application/json", JSON.stringify(s));
                              e.dataTransfer.setData("text/x-group-id", String(group.id));
                              e.dataTransfer.effectAllowed = "move";
                              setDragSlot(s);
                              setDragFromIdx(sIdx);
                            }}
                            onDragOver={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              setPanelDropIdx(null); // clear panel-level indicator
                              const dragged = getDragSlot();
                              if (dragged && dragged.type === "scalar" && (dragged.scalarPanelId ?? 0) === pid) {
                                // Same panel: show reorder indicator on card
                                setMergeTargetPid(null);
                                const r = e.currentTarget.getBoundingClientRect();
                                const side = e.clientX < r.left + r.width / 2 ? "left" : "right";
                                setCardDropIdx({ idx: sIdx, side });
                              } else if (dragged && dragged.type === "scalar") {
                                // Different panel: show merge
                                setCardDropIdx(null);
                                setMergeTargetPid(pid);
                              }
                            }}
                            onDragLeave={() => { setCardDropIdx(null); setMergeTargetPid(null); }}
                            onDrop={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              setCardDropIdx(null);
                              setPanelDropIdx(null);
                              setMergeTargetPid(null);

                              const dragged = getDragSlot();
                              if (!dragged) return;

                              if (dragged.type === "scalar" && (dragged.scalarPanelId ?? 0) === pid) {
                                // Same panel: reorder
                                clearDragSlot();
                                if (dragFromIdx !== null && dragFromIdx !== sIdx) {
                                  const r = e.currentTarget.getBoundingClientRect();
                                  const insertAt = e.clientX < r.left + r.width / 2 ? sIdx : sIdx + 1;
                                  const newSlots = [...slots];
                                  const [moved] = newSlots.splice(dragFromIdx, 1);
                                  const adj = insertAt > dragFromIdx ? insertAt - 1 : insertAt;
                                  newSlots.splice(adj, 0, moved);
                                  onUpdate({ ...group, slots: newSlots });
                                }
                                setDragFromIdx(null);
                              } else if (dragged.type === "scalar") {
                                // Different panel: merge
                                clearDragSlot();
                                const alreadyHere = panelSlotData.some((x) => x.run === dragged.run && x.tag === dragged.tag);
                                if (!alreadyHere) {
                                  const updated = slots.map((x) =>
                                    x.type === "scalar" && x.run === dragged.run && x.tag === dragged.tag
                                      ? { ...x, scalarPanelId: pid } : x
                                  );
                                  onUpdate({ ...group, slots: updated });
                                }
                                setDragFromIdx(null);
                              }
                            }}
                            onDragEnd={() => { handleSlotDragEnd(); setMergeTargetPid(null); }}
                          >
                            <div className="flex items-center gap-1.5">
                              <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color }} />
                              <div className="flex-1 min-w-0">
                                <div className="text-xs font-medium truncate" style={{ color }}>{runName}</div>
                                <div className="text-xs text-gray-500 truncate">{s.tag}</div>
                              </div>
                              <span className="text-xs text-gray-400 font-mono shrink-0">it:{curIt}</span>
                              <button onClick={(ev) => { ev.stopPropagation(); toggleLink(sIdx); }} className={`px-1 py-0.5 text-xs rounded ${s.linked ? "bg-blue-100 text-blue-700" : "bg-gray-200 text-gray-500"}`}>
                                {s.linked ? "\uD83D\uDD17" : "\u26D3\uFE0F"}
                              </button>
                              <button onClick={(ev) => { ev.stopPropagation(); removeSlot(sIdx); }} className="text-gray-400 hover:text-red-500 text-sm">{"\u00D7"}</button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              }

              // --- NON-SCALAR: render as before ---
              const it = slot.linked ? closestIt(slot.iterations, globalIt) : slot.localIt;
              const runName = slot.run.split("/").pop() || slot.run;
              const color = getRunColor(slot.run);
              const showLeft = dragOverIdx === idx;
              const showRight = dragOverIdx === idx + 1 && idx === slots.length - 1;

              return (
                <div
                  key={`${slot.type}-${slot.run}-${slot.tag}-${idx}`}
                  ref={(el) => setSlotRef(idx, el)}
                  className={`bg-white rounded-lg border border-gray-200 p-3 relative ${dragFromIdx === idx ? "opacity-40" : ""} ${sliderActiveIdx === idx ? "" : "cursor-grab active:cursor-grabbing"}`}
                  style={{ resize: "horizontal", overflow: "auto", minWidth: 250, width: slot.width ? `${slot.width}px` : undefined, borderLeft: showLeft ? "3px solid #3b82f6" : undefined, borderRight: showRight ? "3px solid #3b82f6" : undefined }}
                  draggable={sliderActiveIdx !== idx}
                  onDragStart={(e) => handleSlotDragStart(e, slot, idx)}
                  onDragEnd={handleSlotDragEnd}
                  onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); const r = e.currentTarget.getBoundingClientRect(); setDragOverIdx(e.clientX < r.left + r.width / 2 ? idx : idx + 1); }}
                  onDragLeave={() => setDragOverIdx(null)}
                  onDrop={(e) => {
                    const r = e.currentTarget.getBoundingClientRect();
                    const insertAt = e.clientX < r.left + r.width / 2 ? idx : idx + 1;
                    // If a scalar card is dropped on a non-scalar item, separate it
                    let slotData: CompareSlot | null = null;
                    try { const raw = e.dataTransfer.getData("application/json"); if (raw) slotData = JSON.parse(raw); } catch {}
                    if (!slotData) slotData = getDragSlot();
                    if (slotData && slotData.type === "scalar" && dragFromIdx !== null) {
                      e.preventDefault(); e.stopPropagation();
                      clearDragSlot(); setDragOverIdx(null);
                      const maxPid = Math.max(0, ...slots.filter((x) => x.type === "scalar").map((x) => x.scalarPanelId ?? 0));
                      const newSlots = [...slots];
                      const [moved] = newSlots.splice(dragFromIdx, 1);
                      const adj = insertAt > dragFromIdx ? insertAt - 1 : insertAt;
                      newSlots.splice(adj, 0, { ...moved, scalarPanelId: maxPid + 1 });
                      onUpdate({ ...group, slots: newSlots });
                      setDragFromIdx(null);
                      return;
                    }
                    handleSlotDrop(e, insertAt);
                  }}
                >
                  <div className="flex items-center justify-between mb-2 gap-1">
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-medium truncate" style={{ color }}>{runName}</div>
                      <div className="text-xs text-gray-500 truncate">{slot.tag} <span className="text-gray-300">({slot.type})</span></div>
                    </div>
                    <div className="flex items-center gap-1 shrink-0">
                      <span className="text-xs text-gray-400">it:{it}</span>
                      <button onClick={() => toggleLink(idx)} title={slot.linked ? "Unlink" : "Link"} className={`px-1.5 py-0.5 text-xs rounded transition-colors ${slot.linked ? "bg-blue-100 text-blue-700" : "bg-gray-100 text-gray-500"}`}>
                        {slot.linked ? "\uD83D\uDD17" : "\u26D3\uFE0F"}
                      </button>
                      <button onClick={() => removeSlot(idx)} className="text-gray-400 hover:text-red-500 text-sm px-1" title="Remove">{"\u00D7"}</button>
                    </div>
                  </div>
                  {!slot.linked && slot.iterations.length > 1 && (
                    <div className="mb-2" onMouseEnter={() => setSliderActiveIdx(idx)} onMouseLeave={() => setSliderActiveIdx(null)}>
                      <input type="range" min={0} max={slot.iterations.length - 1} value={slot.iterations.indexOf(slot.localIt)} onChange={(e) => setLocalIt(idx, slot.iterations[parseInt(e.target.value)])} className="w-full" />
                    </div>
                  )}
                  <SlotContent slot={slot} it={it} />
                </div>
              );
            })}
            {slots.length === 0 && <p className="text-xs text-gray-400 text-center py-4 w-full">Drag items here or use + buttons on other tabs</p>}
          </div>
        );
      })()}
    </div>
  );

  if (overlay) {
    return (
      <div className="fixed inset-0 z-50 bg-black/80 overflow-auto p-8" onClick={() => setOverlay(false)}>
        <div className="max-w-6xl mx-auto" onClick={(e) => e.stopPropagation()}>
          <div className="bg-gray-100 rounded-lg border border-gray-300 p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-white">{group.name}</h3>
              <button onClick={() => setOverlay(false)} className="px-2 py-1 text-xs bg-gray-700 text-white rounded hover:bg-gray-600">Exit fullscreen</button>
            </div>
            {content}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className={`rounded-lg border p-4 mb-4 transition-colors ${dragOver ? "bg-blue-50 border-blue-300" : "bg-gray-100 border-gray-300"}`}
      onDragOver={handleDragOver} onDragLeave={handleDragLeave} onDrop={handleDrop}
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-1">
          <button onClick={toggleCollapsed} className="text-xs text-gray-400 hover:text-gray-600">{group.collapsed ? "\u25B6" : "\u25BC"}</button>
          {editing ? (
            <input autoFocus value={editName} onChange={(e) => setEditName(e.target.value)} onBlur={commitName} onKeyDown={(e) => { if (e.key === "Enter") commitName(); if (e.key === "Escape") setEditing(false); }} className="text-sm font-semibold text-gray-700 border-b border-blue-400 outline-none bg-transparent w-40" />
          ) : (
            <span className="text-sm font-semibold text-gray-700 cursor-text hover:text-gray-900" onDoubleClick={() => { setEditName(group.name); setEditing(true); }} title="Double-click to rename">{group.name}</span>
          )}
          <span className="text-xs text-gray-400 font-normal">({slots.length})</span>
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => setOverlay(true)} title="Fullscreen" className="px-1.5 py-0.5 text-xs text-gray-500 hover:text-gray-700 rounded hover:bg-gray-200 transition-colors">{"\u26F6"}</button>
          <button onClick={onPopout} title="Open in new window" className="px-1.5 py-0.5 text-xs text-gray-500 hover:text-gray-700 rounded hover:bg-gray-200 transition-colors">{"\u2197"}</button>
          <button onClick={onDelete} title="Delete comparison" className="px-1.5 py-0.5 text-xs text-gray-400 hover:text-red-500 rounded hover:bg-gray-200 transition-colors">{"\u00D7"}</button>
        </div>
      </div>
      {content}
    </div>
  );
}

// ─── Popout Window ───────────────────────────────────────────────

function PopoutWindow({ group, onUpdate, onClose }: { group: CompareGroup; onUpdate: (g: CompareGroup) => void; onClose: () => void }) {
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const windowRef = useRef<Window | null>(null);
  const [popoutCollapsed, setPopoutCollapsed] = useState(false);

  useEffect(() => {
    const w = window.open("", `compare-${group.id}`, "width=1200,height=900");
    if (!w) { onClose(); return; }
    windowRef.current = w;
    w.document.write("<!DOCTYPE html><html><head></head><body><div id='root'></div></body></html>");
    w.document.close();
    for (const sheet of document.styleSheets) { try { if (sheet.href) { const l = w.document.createElement("link"); l.rel = "stylesheet"; l.href = sheet.href; w.document.head.appendChild(l); } } catch {} }
    const s = w.document.createElement("style"); s.textContent = "body{font-family:system-ui,sans-serif;background:#f9fafb;margin:0;padding:16px}"; w.document.head.appendChild(s);
    setContainer(w.document.getElementById("root") as HTMLDivElement);

    const onDragOver = (e: DragEvent) => { e.preventDefault(); if (e.dataTransfer) e.dataTransfer.dropEffect = "copy"; };
    w.document.addEventListener("dragover", onDragOver);
    w.addEventListener("beforeunload", onClose);
    return () => { w.document.removeEventListener("dragover", onDragOver); w.removeEventListener("beforeunload", onClose); w.close(); };
  }, []);

  if (!container) return null;

  const merged = { ...group, collapsed: popoutCollapsed };
  const handleUpdate = (u: CompareGroup) => { setPopoutCollapsed(u.collapsed); onUpdate({ ...u, collapsed: group.collapsed }); };
  const handleDrop = (slot: CompareSlot) => { if (!groupHasSlot(group, slot)) onUpdate({ ...group, slots: [...group.slots, slot] }); };

  return createPortal(
    <CompareGroupPanel group={merged} onUpdate={handleUpdate} onDelete={onClose} onPopout={() => {}} onDropSlot={handleDrop} />,
    container,
  );
}

// ─── Add-to-Compare Dropdown ─────────────────────────────────────

export interface AddToCompareProps {
  groups: CompareGroup[];
  onAdd: (groupId: number | null) => void;
}

export function AddToCompareButton({ groups, onAdd }: AddToCompareProps) {
  const [open, setOpen] = useState(false);
  return (
    <div className="relative">
      <button onClick={() => setOpen(!open)} title="Add to comparison" className="w-6 h-6 flex items-center justify-center rounded-full border border-gray-300 text-gray-400 hover:border-blue-400 hover:text-blue-600 hover:bg-blue-50 transition-colors text-sm">+</button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className="absolute right-0 top-8 z-50 bg-white border border-gray-200 rounded-lg shadow-lg py-1 min-w-[160px]">
            {groups.map((g) => (
              <button key={g.id} onClick={() => { onAdd(g.id); setOpen(false); }} className="block w-full text-left px-3 py-1.5 text-xs text-gray-700 hover:bg-blue-50 hover:text-blue-700">{g.name} ({g.slots.length})</button>
            ))}
            <hr className="my-1 border-gray-100" />
            <button onClick={() => { onAdd(null); setOpen(false); }} className="block w-full text-left px-3 py-1.5 text-xs text-gray-500 hover:bg-gray-50">+ New comparison</button>
          </div>
        </>
      )}
    </div>
  );
}

// ─── Main Compare View (Tab) ─────────────────────────────────────

export interface CompareViewProps {
  groups: CompareGroup[];
  onGroupsChange: (groups: CompareGroup[]) => void;
}

export default function CompareView({ groups, onGroupsChange }: CompareViewProps) {
  const [poppedOut, setPoppedOut] = useState<Set<number>>(new Set());

  const updateGroup = useCallback((updated: CompareGroup) => {
    onGroupsChange(groups.map((g) => g.id === updated.id ? updated : g));
  }, [groups, onGroupsChange]);

  const deleteGroup = useCallback((id: number) => {
    onGroupsChange(groups.filter((g) => g.id !== id));
  }, [groups, onGroupsChange]);

  const handleDropSlot = useCallback((targetGroupId: number, slot: CompareSlot) => {
    onGroupsChange(groups.map((g) => {
      if (g.id !== targetGroupId) return g;
      if (groupHasSlot(g, slot)) return g;
      return { ...g, slots: [...g.slots, slot] };
    }));
  }, [groups, onGroupsChange]);

  const newGroup = useCallback(() => {
    const id = nextGroupId++;
    onGroupsChange([...groups, { id, name: `Comparison ${id}`, slots: [], collapsed: false }]);
  }, [groups, onGroupsChange]);

  const popout = useCallback((id: number) => setPoppedOut((p) => new Set(p).add(id)), []);
  const closePopout = useCallback((id: number) => setPoppedOut((p) => { const n = new Set(p); n.delete(id); return n; }), []);

  if (groups.length === 0) {
    return (
      <div className="text-center text-gray-500 mt-8">
        <p>No comparisons yet.</p>
        <p className="text-sm mt-2">Use the + buttons on other tabs to add items to a comparison.</p>
        <button onClick={newGroup} className="mt-4 px-3 py-1.5 text-sm rounded border border-gray-300 text-gray-600 hover:bg-gray-50">+ New comparison</button>
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center gap-2 mb-4">
        <button onClick={newGroup} className="px-2 py-1 text-xs rounded border border-dashed border-gray-300 text-gray-400 hover:text-gray-600 hover:border-gray-400 transition-colors">+ New comparison</button>
      </div>
      {groups.map((g) => (
        <CompareGroupPanel key={g.id} group={g} onUpdate={updateGroup} onDelete={() => deleteGroup(g.id)} onPopout={() => popout(g.id)} onDropSlot={(slot) => handleDropSlot(g.id, slot)} />
      ))}
      {groups.filter((g) => poppedOut.has(g.id)).map((g) => (
        <PopoutWindow key={`pop-${g.id}`} group={g} onUpdate={updateGroup} onClose={() => closePopout(g.id)} />
      ))}
    </div>
  );
}

// ─── Hook for managing comparison groups (shared state) ──────────

export function useCompareGroups() {
  const [groups, setGroups] = useState<CompareGroup[]>(loadGroups);

  useEffect(() => { saveGroups(groups); }, [groups]);

  const addToGroup = useCallback((slot: CompareSlot, groupId: number | null) => {
    setGroups((prev) => {
      if (groupId !== null) {
        return prev.map((g) => {
          if (g.id !== groupId) return g;
          if (groupHasSlot(g, slot)) return g;
          return { ...g, slots: [...g.slots, slot] };
        });
      }
      const id = nextGroupId++;
      return [...prev, { id, name: `Comparison ${id}`, slots: [slot], collapsed: false }];
    });
  }, []);

  return { groups, setGroups, addToGroup };
}
