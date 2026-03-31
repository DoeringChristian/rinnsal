import { useMemo, useState } from "react";
import UplotReact from "uplot-react";
import "uplot/dist/uPlot.min.css";
import { GroupedEvents } from "../lib/events";
import { getRunColor } from "./RunSelector";

interface ScalarChartProps {
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
}

export default function ScalarChart({ events, selectedRuns }: ScalarChartProps) {
  const [logScale, setLogScale] = useState(false);
  const [relativeTime, setRelativeTime] = useState(false);

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
      {/* Controls */}
      <div className="flex items-center space-x-4">
        <button
          onClick={() => setLogScale(!logScale)}
          className={`px-3 py-1.5 text-sm rounded-md border transition-colors ${
            logScale
              ? "bg-blue-100 border-blue-300 text-blue-700"
              : "bg-white border-gray-300 text-gray-700 hover:bg-gray-50"
          }`}
        >
          {logScale ? "Linear Y" : "Log Y"}
        </button>
        <button
          onClick={() => setRelativeTime(!relativeTime)}
          className={`px-3 py-1.5 text-sm rounded-md border transition-colors ${
            relativeTime
              ? "bg-blue-100 border-blue-300 text-blue-700"
              : "bg-white border-gray-300 text-gray-700 hover:bg-gray-50"
          }`}
        >
          {relativeTime ? "Iteration" : "Rel. Time"}
        </button>
      </div>

      {/* Charts */}
      {allTags.map((tag) => (
        <ScalarTagChart
          key={tag}
          tag={tag}
          events={events}
          selectedRuns={selectedRuns}
          logScale={logScale}
          relativeTime={relativeTime}
        />
      ))}
    </div>
  );
}

interface ScalarTagChartProps {
  tag: string;
  events: Map<string, GroupedEvents>;
  selectedRuns: string[];
  logScale: boolean;
  relativeTime: boolean;
}

function ScalarTagChart({
  tag,
  events,
  selectedRuns,
  logScale,
  relativeTime,
}: ScalarTagChartProps) {
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
        stroke: getRunColor(rd.run, selectedRuns),
        width: 2,
        spanGaps: true,
      })),
    ];

    // Build options
    const opts: uPlot.Options = {
      width: 800,
      height: 300,
      title: tag,
      scales: {
        y: {
          distr: logScale ? 3 : 1, // 3 = log
        },
      },
      axes: [
        {
          label: relativeTime ? "Time (s)" : "Iteration",
          grid: { show: true, stroke: "#eee" },
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
      <UplotReact options={opts} data={data as uPlot.AlignedData} />
    </div>
  );
}
