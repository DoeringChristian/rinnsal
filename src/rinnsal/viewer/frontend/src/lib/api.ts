/**
 * API client for the rinnsal viewer backend.
 */

export interface Config {
  logDir: string;
}

export interface RunInfo {
  path: string;
  name: string;
}

/**
 * Fetch initial configuration from the backend.
 */
export async function fetchConfig(): Promise<Config> {
  const response = await fetch("/api/config");
  if (!response.ok) {
    throw new Error(`Failed to fetch config: ${response.statusText}`);
  }
  return response.json();
}

/**
 * Fetch list of runs from the backend.
 */
export async function fetchRuns(rootDir: string): Promise<RunInfo[]> {
  const response = await fetch(`/api/runs?root=${encodeURIComponent(rootDir)}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch runs: ${response.statusText}`);
  }
  const paths: string[] = await response.json();
  return paths.map((path) => ({
    path,
    name: path.split("/").pop() || path,
  }));
}

/**
 * Fetch raw protobuf events for a run.
 */
export async function fetchEvents(runPath: string): Promise<ArrayBuffer> {
  const response = await fetch(`/api/events/${encodeURIComponent(runPath)}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch events: ${response.statusText}`);
  }
  return response.arrayBuffer();
}

/**
 * Create a WebSocket connection for streaming events.
 */
export function createEventStream(
  runPath: string,
  onData: (buffer: ArrayBuffer) => void,
  onError?: (error: Event) => void
): WebSocket {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(
    `${protocol}//${window.location.host}/api/events/${encodeURIComponent(runPath)}/stream`
  );

  ws.binaryType = "arraybuffer";

  ws.onmessage = (event) => {
    if (event.data instanceof ArrayBuffer) {
      onData(event.data);
    }
  };

  ws.onerror = (error) => {
    if (onError) {
      onError(error);
    }
  };

  return ws;
}
