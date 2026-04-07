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
  scalarMultiplier?: number; // multiplier for scalar values (default 1)
  scalarOffset?: number; // additive offset for scalar values (default 0)
  scalarYLinked?: boolean; // share y-axis scale with other scalars in panel (default true)
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

/** Number input that supports drag-to-adjust. Drag up/down to change value in log space (for multiplier) or linear (for offset). */
function DragNumberInput({ value, onChange, logScale: logDrag = false, title, prefix }: {
  value: number;
  onChange: (v: number) => void;
  logScale?: boolean;
  title?: string;
  prefix?: string;
}) {
  const dragRef = useRef<{ startY: number; startVal: number } | null>(null);

  const handleMouseDown = (e: React.MouseEvent) => {
    e.stopPropagation();
    e.preventDefault();
    dragRef.current = { startY: e.clientY, startVal: value };

    const handleMouseMove = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      const dy = dragRef.current.startY - ev.clientY; // up = positive
      if (logDrag) {
        // Log-space: each 50px = 10x
        const factor = Math.pow(10, dy / 100);
        onChange(parseFloat((dragRef.current.startVal * factor).toPrecision(4)));
      } else {
        // Linear: scale by current magnitude
        const mag = Math.max(Math.abs(dragRef.current.startVal), 1) * 0.01;
        onChange(parseFloat((dragRef.current.startVal + dy * mag).toPrecision(4)));
      }
    };

    const handleMouseUp = () => {
      dragRef.current = null;
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
  };

  return (
    <div className="flex items-center gap-0.5" title={title}>
      {prefix && (
        <span
          className="text-xs text-gray-400 select-none cursor-ns-resize"
          onMouseDown={handleMouseDown}
        >
          {prefix}
        </span>
      )}
      <input
        type="number"
        step="any"
        value={value}
        onClick={(ev) => ev.stopPropagation()}
        onMouseDown={(ev) => ev.stopPropagation()}
        onPointerDown={(ev) => ev.stopPropagation()}
        onChange={(ev) => { const v = parseFloat(ev.target.value); if (!isNaN(v)) onChange(v); }}
        className="w-14 text-xs border border-gray-300 rounded px-1 py-0.5 text-center"
        draggable={false}
      />
    </div>
  );
}

/** Shows the scalar value at a given iteration. Fetches data (browser-cached). */
function ScalarValueAt({ run, tag, it }: { run: string; tag: string; it: number }) {
  const [val, setVal] = useState<number | null>(null);
  useEffect(() => {
    fetchScalars(run).then((d) => {
      const points = d[tag];
      if (!points || points.length === 0) return;
      // Find closest iteration
      let best = points[0];
      for (const p of points) {
        if (Math.abs(p.it - it) < Math.abs(best.it - it)) best = p;
      }
      setVal(best.value);
    }).catch(() => {});
  }, [run, tag, it]);

  if (val === null) return <span className="text-xs text-gray-300">...</span>;
  return <span className="text-xs font-mono text-gray-600">{val.toPrecision(4)}</span>;
}

interface ScalarGroupSlot {
  run: string;
  tag: string;
  it: number;
  multiplier: number;
  offset: number;
  yLinked: boolean;
}

/** Renders multiple scalar series in one uPlot chart with iteration marker. */
function ScalarGroupChart({ slots, globalIt, onSetIteration, onSlotScaleChange }: {
  slots: ScalarGroupSlot[];
  globalIt: number;
  onSetIteration?: (it: number) => void;
  onSlotScaleChange?: (slotIdx: number, multiplier: number, offset: number) => void;
}) {
  const [allData, setAllData] = useState<Map<string, { it: number; value: number; ts: number }[]>>(new Map());
  const [logScale, setLogScale] = useState(false);
  const [relativeTime, setRelativeTime] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const prevCountRef = useRef(0);
  const prevLogRef = useRef(false);
  const prevSlotKeyRef = useRef("");
  const globalItRef = useRef(globalIt);
  globalItRef.current = globalIt;
  const onSlotScaleChangeRef = useRef(onSlotScaleChange);
  onSlotScaleChangeRef.current = onSlotScaleChange;
  const slotsRef = useRef(slots);
  slotsRef.current = slots;

  // Fetch data for all scalar slots
  useEffect(() => {
    const toFetch = new Map<string, string[]>();
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
      const next = new Map<string, { it: number; value: number; ts: number }[]>();
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

    // Build per-series x/y arrays, using ts or it for x-axis
    const perSeries: { x: number[]; y: number[] }[] = [];
    const xSet = new Set<number>();

    for (const s of slots) {
      const key = `${s.run}\0${s.tag}`;
      const points = allData.get(key);
      if (!points || points.length === 0) { perSeries.push({ x: [], y: [] }); continue; }

      let xArr: number[];
      if (relativeTime) {
        const t0 = points[0].ts;
        xArr = points.map((p) => p.ts - t0);
      } else {
        xArr = points.map((p) => p.it);
      }
      // Only apply multiplier/offset when y-axis is unlinked
      const mult = s.yLinked ? 1 : (s.multiplier || 1);
      const off = s.yLinked ? 0 : (s.offset || 0);
      const yArr = points.map((p) => p.value * mult + off);
      for (const x of xArr) xSet.add(x);
      perSeries.push({ x: xArr, y: yArr });
    }

    const sortedX = Array.from(xSet).sort((a, b) => a - b);

    // Build aligned data
    const aligned: (number | null)[][] = [sortedX];
    const seriesCfg: uPlot.Series[] = [{ label: relativeTime ? "Time (s)" : "Iteration" }];

    for (let si = 0; si < slots.length; si++) {
      const s = slots[si];
      const { x, y } = perSeries[si];
      if (x.length === 0) { aligned.push(sortedX.map(() => null)); } else {
        const map = new Map<number, number>();
        for (let i = 0; i < x.length; i++) map.set(x[i], y[i]);
        aligned.push(sortedX.map((xv) => map.get(xv) ?? null) as (number | null)[]);
      }
      const runName = s.run.split("/").pop() || s.run;
      let label = `${runName} / ${s.tag}`;
      if (!s.yLinked) {
        const m = s.multiplier || 1;
        const o = s.offset || 0;
        if (m !== 1 || o !== 0) {
          const parts: string[] = [];
          if (m !== 1) parts.push(`×${m}`);
          if (o !== 0) parts.push(`${o >= 0 ? "+" : ""}${o}`);
          label += ` (${parts.join(" ")})`;
        }
      }
      seriesCfg.push({
        label,
        stroke: getRunColor(s.run),
        width: 2,
        spanGaps: true,
        scale: "y", // all series on same scale
      });
    }

    // Single y scale for all series
    const scales: Record<string, any> = {
      x: { auto: false, min: 0, max: 1 }, // will be set below
      y: { auto: true, distr: logScale ? 3 : 1 },
    };

    // Build axes: main y-axis, plus right-side axes showing raw values for unlinked series
    const axesCfg: uPlot.Axis[] = [
      {
        label: relativeTime ? "Time (s)" : "Iteration",
        grid: { show: true, stroke: "#eee" },
        values: relativeTime
          ? (_u: uPlot, vals: number[]) => vals.map((v) => v.toFixed(1))
          : (_u: uPlot, vals: number[]) => vals.map((v) => String(Math.round(v))),
      },
      { label: "Value", grid: { show: true, stroke: "#eee" }, scale: "y" },
    ];
    for (let si = 0; si < slots.length; si++) {
      if (!slots[si].yLinked) {
        const mult = slots[si].multiplier || 1;
        const off = slots[si].offset || 0;
        const color = getRunColor(slots[si].run);
        axesCfg.push({
          scale: "y", // same scale, different tick labels
          side: 1,
          grid: { show: false },
          stroke: color,
          ticks: { stroke: color },
          // Show original raw values: raw = (displayed - offset) / multiplier
          values: (_u: uPlot, vals: number[]) => vals.map((v) => {
            const raw = (v - off) / mult;
            return raw.toPrecision(3);
          }),
        } as uPlot.Axis);
      }
    }

    // Draw iteration marker using ref so it always reads the latest value
    const drawMarker = (u: uPlot) => {
      const curIt = globalItRef.current;
      const ctx = u.ctx;
      const xPos = u.valToPos(curIt, "x", true);
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

    const logChanged = logScale !== prevLogRef.current;
    prevLogRef.current = logScale;
    // Force recreate when structure changes (series count, log scale, y-link, multiplier)
    const slotKey = slots.map((s) => `${s.yLinked}:${s.multiplier}:${s.offset}`).join(",");
    const needsRecreate = !chartRef.current || slots.length !== prevCountRef.current || logChanged || slotKey !== prevSlotKeyRef.current;
    prevSlotKeyRef.current = slotKey;
    prevCountRef.current = slots.length;

    if (chartRef.current && !needsRecreate) {
      chartRef.current.setData(aligned as uPlot.AlignedData);
      // Auto-fit with tolerance
      if (sortedX.length > 0) {
        const xPad = (sortedX[sortedX.length - 1] - sortedX[0]) * 0.02 || 1;
        chartRef.current.setScale("x", { min: sortedX[0] - xPad, max: sortedX[sortedX.length - 1] + xPad });
      }
      chartRef.current.redraw();
      return;
    }

    if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
    containerRef.current.innerHTML = "";

    // Compute initial scale with tolerance
    let xMin = sortedX[0], xMax = sortedX[sortedX.length - 1];
    const xPad = (xMax - xMin) * 0.02 || 1;
    xMin -= xPad; xMax += xPad;

    chartRef.current = new uPlot(
      {
        width: containerRef.current.clientWidth || 400,
        height: 250,
        legend: { show: false },
        scales: { ...scales, x: { auto: false, min: xMin, max: xMax } },
        axes: axesCfg,
        series: seriesCfg,
        cursor: {
          show: true,
          focus: { prox: 30 },
          drag: { x: true, y: true },
          bind: {
            mousedown: (_u: uPlot, _targ: HTMLElement, handler: Function) => {
              return (e: MouseEvent) => { if (e.altKey) return null; return handler(e); };
            },
          },
        },
        focus: { alpha: 0.3 },
        hooks: {
          draw: [drawMarker],
          setSelect: [
            (u: uPlot) => {
              const { left, width } = u.select;
              if (width > 0) {
                u.setScale("x", { min: u.posToVal(left, "x"), max: u.posToVal(left + width, "x") });
              }
              u.setSelect({ left: 0, width: 0, top: 0, height: 0 }, false);
            },
          ],
          init: [
            (u: uPlot) => {
              const over = u.over;
              // Double-click to set iteration
              over.addEventListener("dblclick", (e) => {
                if (!onSetIteration) return;
                const left = e.clientX - over.getBoundingClientRect().left;
                const xVal = u.posToVal(left, "x");
                const xData = u.data[0] as number[];
                let bestIt = xData[0], bestDist = Math.abs(bestIt - xVal);
                for (let i = 1; i < xData.length; i++) {
                  const dist = Math.abs(xData[i] - xVal);
                  if (dist < bestDist) { bestIt = xData[i]; bestDist = dist; }
                }
                onSetIteration(bestIt);
              });
              // Alt+wheel zoom
              let isPanning = false, panStartX = 0, panStartY = 0, panXMin = 0, panXMax = 0, panYMin = 0, panYMax = 0;
              over.addEventListener("wheel", (e: WheelEvent) => {
                if (!e.altKey) return;
                e.preventDefault();
                const factor = e.deltaY > 0 ? 1.1 : 1 / 1.1;
                const cl = u.cursor.left!, ct = u.cursor.top!;
                const xMn = u.scales.x.min!, xMx = u.scales.x.max!, yMn = u.scales.y.min!, yMx = u.scales.y.max!;
                const xR = xMx - xMn, yR = yMx - yMn;
                const xP = u.posToVal(cl, "x"), yP = u.posToVal(ct, "y");
                const xRat = (xP - xMn) / xR, yRat = (yP - yMn) / yR;
                const nxR = xR * factor, nyR = yR * factor;
                u.batch(() => {
                  u.setScale("x", { min: xP - xRat * nxR, max: xP + (1 - xRat) * nxR });
                  u.setScale("y", { min: yP - yRat * nyR, max: yP + (1 - yRat) * nyR });
                });
              }, { passive: false });
              // Alt+drag pan
              over.addEventListener("mousedown", (e: MouseEvent) => {
                if (!e.altKey) return; e.preventDefault();
                isPanning = true; panStartX = e.clientX; panStartY = e.clientY;
                panXMin = u.scales.x.min!; panXMax = u.scales.x.max!; panYMin = u.scales.y.min!; panYMax = u.scales.y.max!;
                over.style.cursor = "grabbing";
              });
              window.addEventListener("mousemove", (e: MouseEvent) => {
                if (!isPanning) return;
                const pxW = u.bbox.width / devicePixelRatio, pxH = u.bbox.height / devicePixelRatio;
                const dx = (e.clientX - panStartX) / pxW * (panXMax - panXMin);
                const dy = (e.clientY - panStartY) / pxH * (panYMax - panYMin);
                u.batch(() => { u.setScale("x", { min: panXMin - dx, max: panXMax - dx }); u.setScale("y", { min: panYMin + dy, max: panYMax + dy }); });
              });
              window.addEventListener("mouseup", () => { if (isPanning) { isPanning = false; over.style.cursor = ""; } });

              // Make right-side axis ticks draggable for scale/offset adjustment
              // uPlot axes are direct children of the root element, axes[2+] are right-side
              const root = u.root;
              const axisEls = root.querySelectorAll(".u-axis");
              // axes[0] = x, axes[1] = y-left, axes[2+] = right-side (unlinked)
              let unlinkedIdx = 0;
              for (let ai = 2; ai < axisEls.length; ai++) {
                const axEl = axisEls[ai] as HTMLElement;
                const capturedIdx = unlinkedIdx;
                axEl.style.cursor = "ns-resize";

                axEl.addEventListener("mousedown", (e: MouseEvent) => {
                  e.preventDefault();
                  e.stopPropagation();
                  const curSlots = slotsRef.current;
                  // Find the capturedIdx-th unlinked slot
                  let count = 0;
                  let targetSi = -1;
                  for (let si = 0; si < curSlots.length; si++) {
                    if (!curSlots[si].yLinked) {
                      if (count === capturedIdx) { targetSi = si; break; }
                      count++;
                    }
                  }
                  if (targetSi < 0) return;

                  const startY = e.clientY;
                  const startMult = curSlots[targetSi].multiplier;
                  const startOff = curSlots[targetSi].offset;
                  const mode = e.shiftKey ? "offset" : "mult";

                  const onMove = (ev: MouseEvent) => {
                    const dy = startY - ev.clientY;
                    if (mode === "mult") {
                      const factor = Math.pow(10, dy / 150);
                      onSlotScaleChangeRef.current?.(targetSi, parseFloat((startMult * factor).toPrecision(4)), startOff);
                    } else {
                      const mag = Math.max(Math.abs(startOff), 1) * 0.01;
                      onSlotScaleChangeRef.current?.(targetSi, startMult, parseFloat((startOff + dy * mag).toPrecision(4)));
                    }
                  };
                  const onUp = () => {
                    window.removeEventListener("mousemove", onMove);
                    window.removeEventListener("mouseup", onUp);
                    document.body.style.cursor = "";
                  };
                  document.body.style.cursor = "ns-resize";
                  window.addEventListener("mousemove", onMove);
                  window.addEventListener("mouseup", onUp);
                });
                unlinkedIdx++;
              }
            },
          ],
        },
      },
      aligned as uPlot.AlignedData,
      containerRef.current,
    );

    return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
  }, [allData, slots.length, logScale, relativeTime,
      slots.map((s) => `${s.yLinked}:${s.multiplier}:${s.offset}`).join(",")]);  // rebuild on yLinked/multiplier changes

  // Redraw marker on iteration change
  useEffect(() => { if (chartRef.current) chartRef.current.redraw(); }, [globalIt]);

  const resetZoom = useCallback(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const xData = chart.data[0] as number[];
    if (xData.length > 0) {
      const xPad = (xData[xData.length - 1] - xData[0]) * 0.02 || 1;
      chart.setScale("x", { min: xData[0] - xPad, max: xData[xData.length - 1] + xPad });
    }
    let yMin = Infinity, yMax = -Infinity;
    for (let s = 1; s < chart.data.length; s++) {
      for (const v of chart.data[s]) { if (v != null) { if (v < yMin) yMin = v; if (v > yMax) yMax = v; } }
    }
    if (yMin < yMax) { const pad = (yMax - yMin) * 0.05 || 1; chart.setScale("y", { min: yMin - pad, max: yMax + pad }); }
  }, []);

  if (allData.size === 0) return <div className="text-xs text-gray-400">Loading scalar data...</div>;

  return (
    <div>
      <div className="flex items-center justify-end space-x-2 mb-1">
        <button onClick={resetZoom} className="px-2 py-0.5 text-xs rounded border bg-white border-gray-300 text-gray-600 hover:bg-gray-50 transition-colors">Reset Zoom</button>
        <button onClick={() => setLogScale(!logScale)} className={`px-2 py-0.5 text-xs rounded border transition-colors ${logScale ? "bg-blue-100 border-blue-300 text-blue-700" : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"}`}>{logScale ? "Linear Y" : "Log Y"}</button>
        <button onClick={() => setRelativeTime(!relativeTime)} className={`px-2 py-0.5 text-xs rounded border transition-colors ${relativeTime ? "bg-blue-100 border-blue-300 text-blue-700" : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"}`}>{relativeTime ? "Iteration" : "Rel. Time"}</button>
      </div>
      <div ref={containerRef} style={{ height: 250, overflow: "hidden" }} />
    </div>
  );
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
  const [dragPanelPid, setDragPanelPid] = useState<number | null>(null); // set when dragging a whole panel
  const { slots } = group;

  const commitName = () => {
    const trimmed = editName.trim();
    if (trimmed && trimmed !== group.name) onUpdate({ ...group, name: trimmed });
    setEditing(false);
  };

  /** Merge a scalar slot into a target panel: change its panelId and move it adjacent to the panel's slots. */
  const mergeScalarIntoPanel = (dragged: CompareSlot, targetPid: number) => {
    const newSlots = slots.filter((s) => !(s.type === "scalar" && s.run === dragged.run && s.tag === dragged.tag));
    // Find last slot of target panel
    let lastTargetIdx = -1;
    newSlots.forEach((s, i) => {
      if (s.type === "scalar" && (s.scalarPanelId ?? 0) === targetPid) lastTargetIdx = i;
    });
    const merged = { ...dragged, scalarPanelId: targetPid };
    if (lastTargetIdx >= 0) {
      newSlots.splice(lastTargetIdx + 1, 0, merged);
    } else {
      newSlots.push(merged);
    }
    onUpdate({ ...group, slots: newSlots });
    setDragFromIdx(null);
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

  const handleSlotDragEnd = () => { setDragFromIdx(null); setDragOverIdx(null); setMergeTargetPid(null); setPanelDropIdx(null); setCardDropIdx(null); setDragPanelPid(null); };

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
                        setDragPanelPid(pid);
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
                            mergeScalarIntoPanel(dragged, pid);
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
                          multiplier: s.scalarMultiplier ?? 1,
                          offset: s.scalarOffset ?? 0,
                          yLinked: s.scalarYLinked !== false,
                        }))}
                        globalIt={globalIt}
                        onSetIteration={(it) => {
                          // Find closest index in linkedIterations and set the global slider
                          let bestIdx = 0;
                          let bestDist = Math.abs(linkedIterations[0] - it);
                          for (let i = 1; i < linkedIterations.length; i++) {
                            const dist = Math.abs(linkedIterations[i] - it);
                            if (dist < bestDist) { bestIdx = i; bestDist = dist; }
                          }
                          setGlobalIdx(bestIdx);
                        }}
                        onSlotScaleChange={(si, mult, off) => {
                          // Map panel slot index back to group slot index
                          const sIdx = panelIndices[si];
                          if (sIdx === undefined) return;
                          const updated = [...slots];
                          updated[sIdx] = { ...updated[sIdx], scalarMultiplier: mult, scalarOffset: off };
                          onUpdate({ ...group, slots: updated });
                        }}
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
                          const alreadyHere = panelSlotData.some((s) => s.run === dragged.run && s.tag === dragged.tag);
                          if (!alreadyHere) {
                            mergeScalarIntoPanel(dragged, pid);
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
                                  mergeScalarIntoPanel(dragged, pid);
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
                              <ScalarValueAt run={s.run} tag={s.tag} it={curIt} />
                              <span className="text-xs text-gray-400 font-mono shrink-0">it:{curIt}</span>
                              <button
                                onClick={(ev) => {
                                  ev.stopPropagation();
                                  const updated = [...slots];
                                  updated[sIdx] = { ...updated[sIdx], scalarYLinked: !(s.scalarYLinked !== false) };
                                  onUpdate({ ...group, slots: updated });
                                }}
                                title={s.scalarYLinked !== false ? "Unlink Y-axis (separate scale)" : "Link Y-axis (shared scale)"}
                                className={`px-1 py-0.5 text-xs rounded ${s.scalarYLinked !== false ? "bg-blue-100 text-blue-700" : "bg-orange-100 text-orange-700"}`}
                              >
                                {s.scalarYLinked !== false ? "Y" : "Y\u2082"}
                              </button>
                              {s.scalarYLinked === false && (
                                <>
                                  <DragNumberInput
                                    value={s.scalarMultiplier ?? 1}
                                    onChange={(v) => {
                                      const updated = [...slots];
                                      updated[sIdx] = { ...updated[sIdx], scalarMultiplier: v };
                                      onUpdate({ ...group, slots: updated });
                                    }}
                                    logScale={true}
                                    title="Multiplier (drag label to adjust in log space)"
                                    prefix={"\u00D7"}
                                  />
                                  <DragNumberInput
                                    value={s.scalarOffset ?? 0}
                                    onChange={(v) => {
                                      const updated = [...slots];
                                      updated[sIdx] = { ...updated[sIdx], scalarOffset: v };
                                      onUpdate({ ...group, slots: updated });
                                    }}
                                    logScale={false}
                                    title="Offset (drag label to adjust)"
                                    prefix={"+"}
                                  />
                                </>
                              )}
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
                    e.preventDefault(); e.stopPropagation();
                    const r = e.currentTarget.getBoundingClientRect();
                    const insertAt = e.clientX < r.left + r.width / 2 ? idx : idx + 1;
                    setDragOverIdx(null);

                    // Whole scalar panel being dragged
                    if (dragPanelPid !== null) {
                      const panelSlotIndices = slots.map((s, i) => ({ s, i })).filter(({ s }) => s.type === "scalar" && (s.scalarPanelId ?? 0) === dragPanelPid).map(({ i }) => i);
                      if (panelSlotIndices.length > 0) {
                        // Remove panel slots and reinsert at target position
                        const panelItems = panelSlotIndices.map((i) => slots[i]);
                        const newSlots = slots.filter((_, i) => !panelSlotIndices.includes(i));
                        // Adjust insert position for removed items before it
                        let adj = insertAt;
                        for (const pi of panelSlotIndices) { if (pi < insertAt) adj--; }
                        newSlots.splice(adj, 0, ...panelItems);
                        onUpdate({ ...group, slots: newSlots });
                        setDragFromIdx(null);
                        setDragPanelPid(null);
                        return;
                      }
                    }

                    // Single scalar card dropped on non-scalar = separate
                    let slotData: CompareSlot | null = null;
                    try { const raw = e.dataTransfer.getData("application/json"); if (raw) slotData = JSON.parse(raw); } catch {}
                    if (!slotData) slotData = getDragSlot();
                    if (slotData && slotData.type === "scalar" && dragFromIdx !== null) {
                      clearDragSlot();
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
