import { useMemo, useRef, useState, useEffect, useCallback } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { ScalarData } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";
import { LazyRender } from "./LazyRender";

interface ScalarChartProps {
  data: Map<string, ScalarData>;
}

export default function ScalarChart({ data }: ScalarChartProps) {
  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const runData of data.values()) {
      for (const tag of Object.keys(runData)) {
        tags.add(tag);
      }
    }
    return Array.from(tags).sort();
  }, [data]);

  if (allTags.length === 0) {
    return (
      <p className="text-gray-500 text-center mt-8">
        No scalars logged in selected runs.
      </p>
    );
  }

  return (
    <div className="space-y-6">
      {allTags.map((tag) => (
        <CollapsibleSection key={tag} title={tag}>
          <LazyRender>
            <ScalarTagChart tag={tag} data={data} />
          </LazyRender>
        </CollapsibleSection>
      ))}
    </div>
  );
}

interface ScalarTagChartProps {
  tag: string;
  data: Map<string, ScalarData>;
}

function buildChartData(
  data: Map<string, ScalarData>,
  tag: string,
  relativeTime: boolean,
) {
  const runs: string[] = [];
  for (const [run, runData] of data) {
    if (tag in runData && runData[tag].length > 0) runs.push(run);
  }
  runs.sort();

  if (runs.length === 0) return null;

  const runXY: { run: string; x: number[]; y: number[] }[] = [];
  for (const run of runs) {
    const points = data.get(run)![tag];
    let x: number[];
    if (relativeTime) {
      const t0 = points[0].ts;
      x = points.map((p) => p.ts - t0);
    } else {
      x = points.map((p) => p.it);
    }
    runXY.push({ run, x, y: points.map((p) => p.value) });
  }

  // Merge x values
  const xSet = new Set<number>();
  for (const rd of runXY) for (const x of rd.x) xSet.add(x);
  const sortedX = Array.from(xSet).sort((a, b) => a - b);

  // Build aligned data
  const aligned: (number | null)[][] = [sortedX];
  for (const rd of runXY) {
    const map = new Map<number, number>();
    for (let i = 0; i < rd.x.length; i++) map.set(rd.x[i], rd.y[i]);
    aligned.push(sortedX.map((x) => map.get(x) ?? null) as (number | null)[]);
  }

  return { data: aligned, runs };
}

function ScalarTagChart({ tag, data }: ScalarTagChartProps) {
  const [logScale, setLogScale] = useState(false);
  const [relativeTime, setRelativeTime] = useState(false);

  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const chartRunsRef = useRef<string[]>([]);
  const prevLogScaleRef = useRef(logScale);

  const chartData = useMemo(
    () => buildChartData(data, tag, relativeTime),
    [data, tag, relativeTime],
  );

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    if (!chartData) {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
        chartRunsRef.current = [];
      }
      return;
    }

    const { data: aligned, runs } = chartData;
    const chart = chartRef.current;
    const prevRuns = chartRunsRef.current;

    // Force full rebuild when logScale changes (distr can't be updated incrementally)
    const logScaleChanged = logScale !== prevLogScaleRef.current;
    prevLogScaleRef.current = logScale;

    if (!chart || logScaleChanged) {
      // First render or full rebuild (e.g. logScale changed)
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
      const series: uPlot.Series[] = [
        { label: relativeTime ? "Time (s)" : "Iteration" },
        ...runs.map((run) => ({
          label: run.split("/").pop() || run,
          stroke: getRunColor(run),
          width: 2,
          spanGaps: true,
        })),
      ];

      container.innerHTML = "";

      // Alt+wheel zoom and Alt+drag pan state
      let isPanning = false;
      let panStartX = 0;
      let panStartScaleMin = 0;
      let panStartScaleMax = 0;

      chartRef.current = new uPlot(
        {
          width: container.clientWidth || 800,
          height: 300,
          scales: {
            x: { auto: true },
            y: { auto: true, distr: logScale ? 3 : 1 },
          },
          axes: [
            {
              label: relativeTime ? "Time (s)" : "Iteration",
              grid: { show: true, stroke: "#eee" },
              values: relativeTime
                ? (_u: uPlot, vals: number[]) => vals.map((v) => v.toFixed(1))
                : (_u: uPlot, vals: number[]) => vals.map((v) => String(Math.round(v))),
            },
            { label: "Value", grid: { show: true, stroke: "#eee" } },
          ],
          series,
          cursor: {
            drag: { x: true, y: true },
            // Disable drag selection when Alt is held (pan mode)
            bind: {
              mousedown: (_u: uPlot, _targ: HTMLElement, handler: Function) => {
                return (e: MouseEvent) => {
                  if (e.altKey) return null;
                  return handler(e);
                };
              },
            },
          },
          hooks: {
            setSelect: [
              (u: uPlot) => {
                if (isPanning) {
                  u.setSelect({ left: 0, width: 0, top: 0, height: 0 }, false);
                  return;
                }
                const { left, width } = u.select;
                if (width > 0) {
                  u.setScale("x", {
                    min: u.posToVal(left, "x"),
                    max: u.posToVal(left + width, "x"),
                  });
                }
                u.setSelect({ left: 0, width: 0, top: 0, height: 0 }, false);
              },
            ],
            init: [
              (u: uPlot) => {
                const over = u.over;

                // Alt+wheel = zoom both axes around cursor
                over.addEventListener("wheel", (e: WheelEvent) => {
                  if (!e.altKey) return;
                  e.preventDefault();

                  const factor = e.deltaY > 0 ? 1.1 : 1 / 1.1;

                  // Zoom x around cursor
                  const cursorLeft = u.cursor.left!;
                  const xMin = u.scales.x.min!;
                  const xMax = u.scales.x.max!;
                  const xRange = xMax - xMin;
                  const xPos = u.posToVal(cursorLeft, "x");
                  const xRatio = (xPos - xMin) / xRange;
                  const newXRange = xRange * factor;

                  // Zoom y around cursor
                  const cursorTop = u.cursor.top!;
                  const yMin = u.scales.y.min!;
                  const yMax = u.scales.y.max!;
                  const yRange = yMax - yMin;
                  const yPos = u.posToVal(cursorTop, "y");
                  const yRatio = (yPos - yMin) / yRange;
                  const newYRange = yRange * factor;

                  u.batch(() => {
                    u.setScale("x", {
                      min: xPos - xRatio * newXRange,
                      max: xPos + (1 - xRatio) * newXRange,
                    });
                    u.setScale("y", {
                      min: yPos - yRatio * newYRange,
                      max: yPos + (1 - yRatio) * newYRange,
                    });
                  });
                }, { passive: false });

                // Alt+drag = pan both axes
                let panStartY = 0;
                let panStartYMin = 0;
                let panStartYMax = 0;

                over.addEventListener("mousedown", (e: MouseEvent) => {
                  if (!e.altKey) return;
                  e.preventDefault();
                  isPanning = true;
                  panStartX = e.clientX;
                  panStartY = e.clientY;
                  panStartScaleMin = u.scales.x.min!;
                  panStartScaleMax = u.scales.x.max!;
                  panStartYMin = u.scales.y.min!;
                  panStartYMax = u.scales.y.max!;
                  over.style.cursor = "grabbing";
                });

                window.addEventListener("mousemove", (e: MouseEvent) => {
                  if (!isPanning) return;

                  const dx = e.clientX - panStartX;
                  const dy = e.clientY - panStartY;
                  const pxW = u.bbox.width / devicePixelRatio;
                  const pxH = u.bbox.height / devicePixelRatio;
                  const xRange = panStartScaleMax - panStartScaleMin;
                  const yRange = panStartYMax - panStartYMin;
                  const valDx = (dx / pxW) * xRange;
                  const valDy = (dy / pxH) * yRange;

                  u.batch(() => {
                    u.setScale("x", {
                      min: panStartScaleMin - valDx,
                      max: panStartScaleMax - valDx,
                    });
                    // y is inverted (screen y goes down, value goes up)
                    u.setScale("y", {
                      min: panStartYMin + valDy,
                      max: panStartYMax + valDy,
                    });
                  });
                });

                window.addEventListener("mouseup", () => {
                  if (isPanning) {
                    isPanning = false;
                    over.style.cursor = "";
                  }
                });
              },
            ],
          },
        },
        aligned as uPlot.AlignedData,
        container,
      );
      chartRunsRef.current = runs;
      return;
    }

    // Incremental update
    const nextSet = new Set(runs);

    // Remove deselected series (backwards for stable indices)
    for (let i = prevRuns.length - 1; i >= 0; i--) {
      if (!nextSet.has(prevRuns[i])) {
        chart.delSeries(i + 1);
      }
    }

    // Add new series
    const remaining = new Set(prevRuns.filter((r) => nextSet.has(r)));
    for (const run of runs) {
      if (!remaining.has(run)) {
        chart.addSeries({
          label: run.split("/").pop() || run,
          stroke: getRunColor(run),
          width: 2,
          spanGaps: true,
        });
      }
    }

    chart.setData(aligned as uPlot.AlignedData);

    // Auto-fit axes to show all data after structural changes
    const xVals = aligned[0] as number[];
    if (xVals.length > 0) {
      chart.setScale("x", { min: xVals[0], max: xVals[xVals.length - 1] });
    }

    chartRunsRef.current = runs;
  }, [chartData, logScale]);

  useEffect(() => {
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, []);

  const resetZoom = useCallback(() => {
    const chart = chartRef.current;
    if (!chart || !chartData) return;
    const xVals = chartData.data[0] as number[];
    if (xVals.length > 0) {
      chart.setScale("x", { min: xVals[0], max: xVals[xVals.length - 1] });
    }
    // Let y auto-range by setting to the data extent
    let yMin = Infinity, yMax = -Infinity;
    for (let s = 1; s < chartData.data.length; s++) {
      for (const v of chartData.data[s]) {
        if (v != null) {
          if (v < yMin) yMin = v;
          if (v > yMax) yMax = v;
        }
      }
    }
    if (yMin < yMax) {
      const pad = (yMax - yMin) * 0.05 || 1;
      chart.setScale("y", { min: yMin - pad, max: yMax + pad });
    }
  }, [chartData]);

  if (!chartData) return null;

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center justify-end space-x-2 mb-2">
        <button onClick={resetZoom} className="px-2 py-1 text-xs rounded border bg-white border-gray-300 text-gray-600 hover:bg-gray-50 transition-colors">Reset Zoom</button>
        <button onClick={() => setLogScale(!logScale)} className={`px-2 py-1 text-xs rounded border transition-colors ${logScale ? "bg-blue-100 border-blue-300 text-blue-700" : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"}`}>{logScale ? "Linear Y" : "Log Y"}</button>
        <button onClick={() => setRelativeTime(!relativeTime)} className={`px-2 py-1 text-xs rounded border transition-colors ${relativeTime ? "bg-blue-100 border-blue-300 text-blue-700" : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"}`}>{relativeTime ? "Iteration" : "Rel. Time"}</button>
      </div>
      <div ref={containerRef} />
    </div>
  );
}
