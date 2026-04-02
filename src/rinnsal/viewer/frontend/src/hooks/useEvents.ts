import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import {
  fetchScalars,
  fetchText,
  fetchFiguresMeta,
  fetchCards,
  ScalarData,
  TextData,
  FigureMetaData,
  CardData,
} from "../lib/api";

export type Tab = "scalars" | "text" | "figures" | "cards";

/** Per-run data for the active tab type */
export interface RunScalars {
  /** tag → [{it, value, ts}] */
  [tag: string]: { it: number; value: number; ts: number }[];
}

export interface RunText {
  [tag: string]: { it: number; value: string }[];
}

export interface RunFigures {
  [tag: string]: { it: number }[];
}

export interface RunCards {
  [task: string]: { it: number; kind: string; title: string; content: string; image?: string }[];
}

interface UseEventsResult {
  scalars: Map<string, ScalarData>;
  text: Map<string, TextData>;
  figures: Map<string, FigureMetaData>;
  cards: Map<string, CardData>;
  isLoading: boolean;
  error: string | null;
  refresh: () => void;
}

/**
 * Fetch data for selected runs, only for the active tab type.
 * Scalars/text/figures metadata are tiny (KB). Images loaded on demand.
 */
export function useEvents(
  selectedRuns: string[],
  activeTab: Tab,
): UseEventsResult {
  const [scalars, setScalars] = useState<Map<string, ScalarData>>(new Map());
  const [text, setText] = useState<Map<string, TextData>>(new Map());
  const [figures, setFigures] = useState<Map<string, FigureMetaData>>(new Map());
  const [cards, setCards] = useState<Map<string, CardData>>(new Map());
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Ref to current cache for the fetch callback
  const cacheRef = useRef({ scalars, text, figures, cards });
  cacheRef.current = { scalars, text, figures, cards };

  // Stabilize selectedRuns by content
  const selectedRunsKey = selectedRuns.join("\0");
  const stableRuns = useMemo(() => selectedRuns, [selectedRunsKey]);

  const fetchTab = useCallback(async (tab: Tab, runs: string[]) => {
    const cache = cacheRef.current;
    const getMap = () => {
      switch (tab) {
        case "scalars": return cache.scalars;
        case "text": return cache.text;
        case "figures": return cache.figures;
        case "cards": return cache.cards;
      }
    };

    const existing = getMap();
    const toFetch = runs.filter((r) => !existing.has(r));
    if (toFetch.length === 0) return;

    setIsLoading(true);
    setError(null);

    try {
      const BATCH = 6;
      for (let i = 0; i < toFetch.length; i += BATCH) {
        const batch = toFetch.slice(i, i + BATCH);
        const results = await Promise.all(
          batch.map(async (run) => {
            switch (tab) {
              case "scalars": return [run, await fetchScalars(run)] as const;
              case "text": return [run, await fetchText(run)] as const;
              case "figures": return [run, await fetchFiguresMeta(run)] as const;
              case "cards": return [run, await fetchCards(run)] as const;
            }
          })
        );

        // Update the appropriate state
        const setter = tab === "scalars" ? setScalars
          : tab === "text" ? setText
          : tab === "figures" ? setFigures
          : setCards;

        setter((prev: Map<string, any>) => {
          const next = new Map(prev);
          for (const [run, data] of results) {
            next.set(run, data);
          }
          return next;
        });
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load data");
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Fetch data when tab or selection changes
  useEffect(() => {
    if (stableRuns.length === 0) {
      setScalars(new Map());
      setText(new Map());
      setFigures(new Map());
      setCards(new Map());
      return;
    }

    // Prune deselected runs from active tab's cache
    const selectedSet = new Set(stableRuns);
    const prune = <T,>(prev: Map<string, T>): Map<string, T> => {
      let needsPrune = false;
      for (const key of prev.keys()) {
        if (!selectedSet.has(key)) { needsPrune = true; break; }
      }
      if (!needsPrune) return prev;
      const next = new Map<string, T>();
      for (const [k, v] of prev) {
        if (selectedSet.has(k)) next.set(k, v);
      }
      return next;
    };

    if (activeTab === "scalars") setScalars(prune);
    else if (activeTab === "text") setText(prune);
    else if (activeTab === "figures") setFigures(prune);
    else if (activeTab === "cards") setCards(prune);

    fetchTab(activeTab, stableRuns);
  }, [stableRuns, activeTab, fetchTab]);

  const refresh = useCallback(() => {
    if (activeTab === "scalars") setScalars(new Map());
    else if (activeTab === "text") setText(new Map());
    else if (activeTab === "figures") setFigures(new Map());
    else if (activeTab === "cards") setCards(new Map());
    fetchTab(activeTab, stableRuns);
  }, [stableRuns, activeTab, fetchTab]);

  return { scalars, text, figures, cards, isLoading, error, refresh };
}
