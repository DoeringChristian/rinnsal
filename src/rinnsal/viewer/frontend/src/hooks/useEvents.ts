import { useState, useEffect, useCallback } from "react";
import { fetchEvents, createEventStream } from "../lib/api";
import { parseEvents, appendEvents, groupEvents, GroupedEvents, Event } from "../lib/events";

interface UseEventsResult {
  events: Map<string, GroupedEvents>;
  isLoading: boolean;
  error: string | null;
  refresh: () => void;
}

/**
 * Hook to fetch and stream events for selected runs.
 */
export function useEvents(selectedRuns: string[]): UseEventsResult {
  const [rawEvents, setRawEvents] = useState<Map<string, Event[]>>(new Map());
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [websockets, setWebsockets] = useState<Map<string, WebSocket>>(new Map());

  // Fetch initial events for all selected runs
  const loadEvents = useCallback(async () => {
    if (selectedRuns.length === 0) {
      setRawEvents(new Map());
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const results = await Promise.all(
        selectedRuns.map(async (run) => {
          const buffer = await fetchEvents(run);
          const events = parseEvents(buffer);
          return [run, events] as const;
        })
      );

      setRawEvents(new Map(results));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load events");
    } finally {
      setIsLoading(false);
    }
  }, [selectedRuns]);

  // Load events when selection changes
  useEffect(() => {
    loadEvents();
  }, [loadEvents]);

  // Set up WebSocket streams for live updates
  useEffect(() => {
    // Close old websockets
    for (const ws of websockets.values()) {
      ws.close();
    }

    if (selectedRuns.length === 0) {
      setWebsockets(new Map());
      return;
    }

    const newWebsockets = new Map<string, WebSocket>();

    for (const run of selectedRuns) {
      const ws = createEventStream(
        run,
        (buffer) => {
          setRawEvents((prev) => {
            const existing = prev.get(run) || [];
            const updated = appendEvents(existing, buffer);
            return new Map(prev).set(run, updated);
          });
        },
        (error) => {
          console.error(`WebSocket error for ${run}:`, error);
        }
      );
      newWebsockets.set(run, ws);
    }

    setWebsockets(newWebsockets);

    return () => {
      for (const ws of newWebsockets.values()) {
        ws.close();
      }
    };
  }, [selectedRuns]);

  // Group events for each run
  const events = new Map<string, GroupedEvents>();
  for (const [run, runEvents] of rawEvents) {
    events.set(run, groupEvents(runEvents));
  }

  return {
    events,
    isLoading,
    error,
    refresh: loadEvents,
  };
}
