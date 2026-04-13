import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import {
  fetchScalars,
  fetchText,
  fetchFiguresMeta,
  fetchImagesMeta,
  fetchCardsIndex,
  ScalarData,
  TextData,
  FigureMetaData,
  ImageMetaData,
  CardIndexEntry,
} from "../lib/api";

export type Tab = "scalars" | "text" | "figures" | "images" | "cards" | "compare" | "graph";

interface UseEventsResult {
  scalars: Map<string, ScalarData>;
  text: Map<string, TextData>;
  figures: Map<string, FigureMetaData>;
  images: Map<string, ImageMetaData>;
  cards: Map<string, CardIndexEntry[]>;
  isLoading: boolean;
  error: string | null;
  refresh: () => void;
}

export function useEvents(
  selectedRuns: string[],
  activeTab: Tab,
): UseEventsResult {
  const [scalars, setScalars] = useState<Map<string, ScalarData>>(new Map());
  const [text, setText] = useState<Map<string, TextData>>(new Map());
  const [figures, setFigures] = useState<Map<string, FigureMetaData>>(new Map());
  const [images, setImages] = useState<Map<string, ImageMetaData>>(new Map());
  const [cards, setCards] = useState<Map<string, CardIndexEntry[]>>(new Map());
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const cacheRef = useRef({ scalars, text, figures, images, cards });
  cacheRef.current = { scalars, text, figures, images, cards };

  const selectedRunsKey = selectedRuns.join("\0");
  const stableRuns = useMemo(() => selectedRuns, [selectedRunsKey]);

  const fetchTab = useCallback(async (tab: Tab, runs: string[]) => {
    // "compare" tab doesn't fetch — slots fetch their own data.
    // "graph" tab fetches its own flow data, independent of selected runs.
    if (tab === "compare" || tab === "graph") return;

    const cache = cacheRef.current;
    const getMap = () => {
      switch (tab) {
        case "scalars": return cache.scalars;
        case "text": return cache.text;
        case "figures": return cache.figures;
        case "images": return cache.images;
        case "cards": return cache.cards;
      }
    };

    const existing = getMap();
    if (!existing) return;
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
              case "images": return [run, await fetchImagesMeta(run)] as const;
              case "cards": return [run, await fetchCardsIndex(run)] as const;
            }
          })
        );

        const setter = tab === "scalars" ? setScalars
          : tab === "text" ? setText
          : tab === "figures" ? setFigures
          : tab === "images" ? setImages
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

  // Force initial fetch on mount for persisted selections
  const didInitRef = useRef(false);
  useEffect(() => {
    if (!didInitRef.current && stableRuns.length > 0) {
      didInitRef.current = true;
      fetchTab(activeTab, stableRuns);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (stableRuns.length === 0) {
      setScalars(new Map());
      setText(new Map());
      setFigures(new Map());
      setImages(new Map());
      setCards(new Map());
      return;
    }

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
    else if (activeTab === "images") setImages(prune);
    else if (activeTab === "cards") setCards(prune);

    fetchTab(activeTab, stableRuns);
  }, [stableRuns, activeTab, fetchTab]);

  const refresh = useCallback(() => {
    cacheRef.current = {
      scalars: new Map(),
      text: new Map(),
      figures: new Map(),
      images: new Map(),
      cards: new Map(),
    };
    fetchTab(activeTab, stableRuns);
  }, [stableRuns, activeTab, fetchTab]);

  return { scalars, text, figures, images, cards, isLoading, error, refresh };
}
