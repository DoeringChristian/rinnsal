import { useState, useEffect, useMemo, useCallback } from "react";
import ReactFlow, {
  Background,
  Controls,
  Node,
  Edge,
  MarkerType,
  ReactFlowProvider,
} from "reactflow";
import "reactflow/dist/style.css";
import dagre from "dagre";
import TaskCard, { TaskCardData } from "./TaskCard";
import TaskHistoryDrawer from "./TaskHistoryDrawer";
import {
  fetchFlows,
  fetchTaskHistory,
  FlowInfo,
  TaskHistoryEntry,
} from "../lib/api";

interface FlowGraphProps {
  rootDir: string;
  flowName: string | null;
  refreshKey?: number;
  onOpenRun?: (runPath: string) => void;
}

const NODE_WIDTH = 220;
const NODE_HEIGHT = 110;

function layoutWithDagre(nodes: Node[], edges: Edge[]): Node[] {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "TB", nodesep: 40, ranksep: 60 });

  nodes.forEach((n) => {
    g.setNode(n.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });
  edges.forEach((e) => {
    g.setEdge(e.source, e.target);
  });

  dagre.layout(g);

  return nodes.map((n) => {
    const pos = g.node(n.id);
    return {
      ...n,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 },
    };
  });
}

const nodeTypes = { taskCard: TaskCard };

function FlowGraphInner({ rootDir, flowName, refreshKey = 0, onOpenRun }: FlowGraphProps) {
  const [flows, setFlows] = useState<FlowInfo[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<string | null>(null);
  const [historyCache, setHistoryCache] = useState<Record<string, TaskHistoryEntry[]>>({});

  useEffect(() => {
    if (!rootDir) {
      setFlows([]);
      return;
    }
    setIsLoading(true);
    setError(null);
    fetchFlows(rootDir)
      .then(setFlows)
      .catch((e) => {
        console.error("Failed to fetch flows:", e);
        setError(String(e));
      })
      .finally(() => setIsLoading(false));
  }, [rootDir, refreshKey]);

  const flow = useMemo(
    () => flows.find((f) => f.name === flowName) || null,
    [flows, flowName],
  );

  // Fetch history for every task in the selected flow so we can draw sparklines
  useEffect(() => {
    if (!flow) return;
    const toFetch = flow.nodes
      .map((n) => n.name)
      .filter((name) => !(name in historyCache));
    if (toFetch.length === 0) return;
    let cancelled = false;
    Promise.all(
      toFetch.map((name) =>
        fetchTaskHistory(rootDir, flow.name, name)
          .then<[string, TaskHistoryEntry[]]>((h) => [name, h])
          .catch<[string, TaskHistoryEntry[]]>(() => [name, []]),
      ),
    ).then((results) => {
      if (cancelled) return;
      setHistoryCache((prev) => {
        const next = { ...prev };
        for (const [name, h] of results) next[name] = h;
        return next;
      });
    });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flow?.name, rootDir, refreshKey]);

  const onTaskClick = useCallback((name: string) => {
    setSelectedTask(name);
  }, []);

  const { nodes, edges } = useMemo(() => {
    if (!flow) return { nodes: [] as Node[], edges: [] as Edge[] };

    const rawNodes: Node[] = flow.nodes.map((n) => {
      const history = historyCache[n.name] || [];
      // Oldest to newest durations for the sparkline
      const durations = [...history]
        .sort((a, b) => a.timestamp - b.timestamp)
        .map((h) => h.duration)
        .filter((d) => Number.isFinite(d) && d > 0);

      const data: TaskCardData = {
        name: n.name,
        status: n.status,
        duration: n.duration,
        timestamp: n.timestamp,
        run_count: n.run_count,
        durations,
        onClick: onTaskClick,
      };
      return {
        id: n.name,
        type: "taskCard",
        position: { x: 0, y: 0 },
        data,
      };
    });

    const rawEdges: Edge[] = flow.edges.map((e, i) => ({
      id: `e${i}-${e.from}-${e.to}`,
      source: e.from,
      target: e.to,
      animated: false,
      markerEnd: { type: MarkerType.ArrowClosed, color: "#94a3b8" },
      style: { stroke: "#94a3b8", strokeWidth: 1.5 },
    }));

    return { nodes: layoutWithDagre(rawNodes, rawEdges), edges: rawEdges };
  }, [flow, historyCache, onTaskClick]);

  if (!flowName) {
    return (
      <div className="text-center text-gray-500 mt-8">
        Select a flow from the sidebar to view its DAG.
      </div>
    );
  }

  if (isLoading) {
    return <div className="text-center text-gray-500 mt-8">Loading flow...</div>;
  }

  if (error) {
    return <div className="text-center text-red-500 mt-8">{error}</div>;
  }

  if (!flow) {
    return <div className="text-center text-gray-500 mt-8">Flow not found.</div>;
  }

  if (flow.nodes.length === 0) {
    return (
      <div className="text-center text-gray-500 mt-8">
        No task data recorded for this flow yet.
      </div>
    );
  }

  return (
    <div className="relative w-full h-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={true}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls showInteractive={false} />
      </ReactFlow>
      <TaskHistoryDrawer
        rootDir={rootDir}
        flowName={flow.name}
        taskName={selectedTask}
        onClose={() => setSelectedTask(null)}
        onOpenRun={onOpenRun}
      />
    </div>
  );
}

export default function FlowGraph(props: FlowGraphProps) {
  return (
    <ReactFlowProvider>
      <FlowGraphInner {...props} />
    </ReactFlowProvider>
  );
}
