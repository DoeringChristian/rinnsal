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

    if (!chart) {
      // First render
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
      chartRef.current = new uPlot(
        {
          width: container.clientWidth || 800,
          height: 300,
          scales: { y: { distr: logScale ? 3 : 1 } },
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
          cursor: { drag: { x: true, y: true } },
          hooks: {
            setSelect: [
              (u: uPlot) => {
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
    chartRunsRef.current = runs;
  }, [chartData]);

  useEffect(() => {
    if (chartRef.current) {
      chartRef.current.setScale("y", { distr: logScale ? 3 : 1 } as any);
    }
  }, [logScale]);

  useEffect(() => {
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, []);

  const resetZoom = useCallback(() => {
    if (chartRef.current) {
      chartRef.current.setScale("x", { min: undefined!, max: undefined! });
      chartRef.current.setScale("y", { min: undefined!, max: undefined! });
    }
  }, []);

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
