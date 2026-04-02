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

export async function fetchConfig(): Promise<Config> {
  const response = await fetch("/api/config");
  if (!response.ok) {
    throw new Error(`Failed to fetch config: ${response.statusText}`);
  }
  return response.json();
}

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

/** Scalar data: {tag: [{it, value, ts}, ...]} */
export type ScalarData = Record<string, { it: number; value: number; ts: number }[]>;

export async function fetchScalars(runPath: string): Promise<ScalarData> {
  const response = await fetch(`/api/scalars/${encodeURIComponent(runPath)}`);
  if (!response.ok) throw new Error(`Failed to fetch scalars: ${response.statusText}`);
  return response.json();
}

/** Text data: {tag: [{it, value}, ...]} */
export type TextData = Record<string, { it: number; value: string }[]>;

export async function fetchText(runPath: string): Promise<TextData> {
  const response = await fetch(`/api/text/${encodeURIComponent(runPath)}`);
  if (!response.ok) throw new Error(`Failed to fetch text: ${response.statusText}`);
  return response.json();
}

/** Figure metadata: {tag: [{it}, ...]} — no image bytes */
export type FigureMetaData = Record<string, { it: number }[]>;

export async function fetchFiguresMeta(runPath: string): Promise<FigureMetaData> {
  const response = await fetch(`/api/figures/${encodeURIComponent(runPath)}`);
  if (!response.ok) throw new Error(`Failed to fetch figures: ${response.statusText}`);
  return response.json();
}

/** Get a single figure image URL (loaded on demand) */
export function figureImageUrl(runPath: string, tag: string, it: number): string {
  return `/api/figure/${encodeURIComponent(runPath)}?tag=${encodeURIComponent(tag)}&it=${it}`;
}

/** Card data: {task: [{it, kind, title, content, image?}, ...]} */
export type CardData = Record<string, { it: number; kind: string; title: string; content: string; image?: string }[]>;

export async function fetchCards(runPath: string): Promise<CardData> {
  const response = await fetch(`/api/cards/${encodeURIComponent(runPath)}`);
  if (!response.ok) throw new Error(`Failed to fetch cards: ${response.statusText}`);
  return response.json();
}
