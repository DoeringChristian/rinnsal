import { useMemo, useState } from "react";
import UplotReact from "uplot-react";
import "uplot/dist/uPlot.min.css";
import { GroupedEvents } from "../lib/events";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";

interface ScalarChartProps {
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

export default function ScalarChart({ events, selectedRuns }: ScalarChartProps) {
  // Collect all scalar tags across all runs
  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const grouped of events.values()) {
      for (const tag of grouped.scalars.keys()) {
        tags.add(tag);
      }
    }
    return Array.from(tags).sort();
  }, [events]);

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
          <ScalarTagChart
            tag={tag}
            events={events}
            selectedRuns={selectedRuns}
          />
        </CollapsibleSection>
      ))}
    </div>
  );
}

interface ScalarTagChartProps {
  tag: string;
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

function ScalarTagChart({
  tag,
  events,
  selectedRuns,
}: ScalarTagChartProps) {
  const [logScale, setLogScale] = useState(false);
  const [relativeTime, setRelativeTime] = useState(false);

  const { data, opts } = useMemo(() => {
    // Collect data for each run
    const runData: { run: string; x: number[]; y: number[] }[] = [];

    for (const run of selectedRuns) {
      const grouped = events.get(run);
      if (!grouped) continue;

      const scalars = grouped.scalars.get(tag);
      if (!scalars || scalars.length === 0) continue;

      let x: number[];
      if (relativeTime) {
        const startTime = scalars[0].timestamp;
        x = scalars.map((s) => s.timestamp - startTime);
      } else {
        x = scalars.map((s) => Number(s.iteration));
      }
      const y = scalars.map((s) => s.value);

      runData.push({ run, x, y });
    }

    if (runData.length === 0) {
      return { data: null, series: [], opts: null };
    }

    // Build aligned data for uPlot
    // uPlot expects [xValues, ...yValuesPerSeries]
    // We need to merge x values from all runs and align y values

    // Collect all unique x values
    const allX = new Set<number>();
    for (const rd of runData) {
      for (const x of rd.x) {
        allX.add(x);
      }
    }
    const sortedX = Array.from(allX).sort((a, b) => a - b);

    // Create maps for quick lookup
    const xToY: Map<string, Map<number, number>> = new Map();
    for (const rd of runData) {
      const map = new Map<number, number>();
      for (let i = 0; i < rd.x.length; i++) {
        map.set(rd.x[i], rd.y[i]);
      }
      xToY.set(rd.run, map);
    }

    // Build data arrays
    const data: (number | null)[][] = [sortedX];
    for (const rd of runData) {
      const yMap = xToY.get(rd.run)!;
      const yArr = sortedX.map((x) => yMap.get(x) ?? null);
      data.push(yArr as (number | null)[]);
    }

    // Build series config
    const series: uPlot.Series[] = [
      { label: relativeTime ? "Time (s)" : "Iteration" },
      ...runData.map((rd) => ({
        label: rd.run.split("/").pop() || rd.run,
        stroke: getRunColor(rd.run),
        width: 2,
        spanGaps: true,
      })),
    ];

    // Build options
    const opts: uPlot.Options = {
      width: 800,
      height: 300,
      scales: {
        y: {
          distr: logScale ? 3 : 1, // 3 = log
        },
      },
      axes: [
        {
          label: relativeTime ? "Time (s)" : "Iteration",
          grid: { show: true, stroke: "#eee" },
          values: relativeTime
            ? (_u, vals) => vals.map((v) => v.toFixed(1))
            : (_u, vals) => vals.map((v) => String(Math.round(v))),
        },
        {
          label: "Value",
          grid: { show: true, stroke: "#eee" },
        },
      ],
      series,
      cursor: {
        drag: {
          x: true,
          y: true,
        },
      },
      hooks: {
        setSelect: [
          (u) => {
            const { left, width } = u.select;
            if (width > 0) {
              const min = u.posToVal(left, "x");
              const max = u.posToVal(left + width, "x");
              u.setScale("x", { min, max });
            }
            u.setSelect({ left: 0, width: 0, top: 0, height: 0 }, false);
          },
        ],
      },
    };

    return { data, series, opts };
  }, [events, selectedRuns, tag, logScale, relativeTime]);

  if (!data || !opts) {
    return null;
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center justify-end space-x-2 mb-2">
        <button
          onClick={() => setLogScale(!logScale)}
          className={`px-2 py-1 text-xs rounded border transition-colors ${
            logScale
              ? "bg-blue-100 border-blue-300 text-blue-700"
              : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"
          }`}
        >
          {logScale ? "Linear Y" : "Log Y"}
        </button>
        <button
          onClick={() => setRelativeTime(!relativeTime)}
          className={`px-2 py-1 text-xs rounded border transition-colors ${
            relativeTime
              ? "bg-blue-100 border-blue-300 text-blue-700"
              : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"
          }`}
        >
          {relativeTime ? "Iteration" : "Rel. Time"}
        </button>
      </div>
      <UplotReact options={opts} data={data as uPlot.AlignedData} />
    </div>
  );
}
