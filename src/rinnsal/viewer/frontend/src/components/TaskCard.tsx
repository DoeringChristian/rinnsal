import { memo } from "react";
import { Handle, Position, NodeProps } from "reactflow";

export interface TaskCardData {
  name: string;
  status: string;
  duration: number;
  timestamp: number;
  run_count: number;
  durations: number[]; // history for sparkline (oldest first)
  onClick: (name: string) => void;
}

const STATUS_STYLES: Record<string, { dot: string; border: string; label: string }> = {
  success: { dot: "#16a34a", border: "#86efac", label: "success" },
  failed: { dot: "#dc2626", border: "#fca5a5", label: "failed" },
  cached: { dot: "#6b7280", border: "#d1d5db", label: "cached" },
  running: { dot: "#d97706", border: "#fcd34d", label: "running" },
};

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return "—";
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds - m * 60);
  return `${m}m${s}s`;
}

function formatRelativeTime(ts: number): string {
  if (!ts) return "";
  const diff = Date.now() / 1000 - ts;
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function Sparkline({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) {
    return <div className="w-16 h-5" />;
  }
  const w = 64;
  const h = 20;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = w / (values.length - 1);
  const points = values
    .map((v, i) => `${(i * step).toFixed(1)},${(h - ((v - min) / range) * (h - 2) - 1).toFixed(1)}`)
    .join(" ");
  return (
    <svg width={w} height={h} className="shrink-0">
      <polyline
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
        points={points}
      />
    </svg>
  );
}

function TaskCardInner({ data }: NodeProps<TaskCardData>) {
  const style = STATUS_STYLES[data.status] || STATUS_STYLES.cached;

  return (
    <div
      className="bg-white rounded-lg shadow-sm hover:shadow-md transition-shadow cursor-pointer px-3 py-2 border-2"
      style={{ borderColor: style.border, minWidth: 200 }}
      onClick={() => data.onClick(data.name)}
    >
      <Handle type="target" position={Position.Top} className="!bg-gray-400" />
      <div className="flex items-center gap-2 mb-1">
        <span
          className="w-2.5 h-2.5 rounded-full shrink-0"
          style={{ backgroundColor: style.dot }}
          title={style.label}
        />
        <span className="font-medium text-sm text-gray-900 break-all leading-tight flex-1">
          {data.name}
        </span>
      </div>
      <div className="flex items-center justify-between gap-2">
        <div className="text-xs text-gray-500 leading-tight">
          <div>{formatDuration(data.duration)}</div>
          <div>{formatRelativeTime(data.timestamp)}</div>
          <div>
            {data.run_count} run{data.run_count === 1 ? "" : "s"}
          </div>
        </div>
        <Sparkline values={data.durations} color={style.dot} />
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-gray-400" />
    </div>
  );
}

export default memo(TaskCardInner);
