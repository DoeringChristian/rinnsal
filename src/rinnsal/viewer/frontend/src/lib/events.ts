import { fromBinary } from "@bufbuild/protobuf";
import { type Event, EventSchema } from "../proto/events_pb";

export type { Event } from "../proto/events_pb";

/**
 * Parse length-prefixed protobuf events from a binary buffer.
 *
 * File format:
 * - 4 bytes: message length (little-endian uint32)
 * - N bytes: serialized protobuf Event message
 * - Repeat until end of buffer
 */
export function parseEvents(buffer: ArrayBuffer): Event[] {
  const view = new DataView(buffer);
  const events: Event[] = [];
  let offset = 0;

  while (offset + 4 <= buffer.byteLength) {
    const length = view.getUint32(offset, true); // little-endian
    offset += 4;

    if (offset + length > buffer.byteLength) {
      // Incomplete message, stop parsing
      break;
    }

    const msgBytes = new Uint8Array(buffer, offset, length);
    try {
      events.push(fromBinary(EventSchema, msgBytes));
    } catch (e) {
      console.error("Failed to parse event:", e);
    }
    offset += length;
  }

  return events;
}

/**
 * Append new events from incremental buffer (for WebSocket streaming).
 */
export function appendEvents(
  existing: Event[],
  newBuffer: ArrayBuffer
): Event[] {
  const newEvents = parseEvents(newBuffer);
  return [...existing, ...newEvents];
}

/**
 * Group events by type and tag.
 */
export interface GroupedEvents {
  scalars: Map<string, { iteration: bigint; value: number; timestamp: number }[]>;
  text: Map<string, { iteration: bigint; value: string }[]>;
  figures: Map<string, { iteration: bigint; image: Uint8Array; data: Uint8Array; interactive: boolean }[]>;
}

export function groupEvents(events: Event[]): GroupedEvents {
  const scalars = new Map<string, { iteration: bigint; value: number; timestamp: number }[]>();
  const text = new Map<string, { iteration: bigint; value: string }[]>();
  const figures = new Map<string, { iteration: bigint; image: Uint8Array; data: Uint8Array; interactive: boolean }[]>();

  for (const event of events) {
    const data = event.data;
    if (data.case === "scalar") {
      const tag = data.value.tag;
      if (!scalars.has(tag)) {
        scalars.set(tag, []);
      }
      scalars.get(tag)!.push({
        iteration: event.iteration,
        value: data.value.value,
        timestamp: event.timestamp,
      });
    } else if (data.case === "text") {
      const tag = data.value.tag;
      if (!text.has(tag)) {
        text.set(tag, []);
      }
      text.get(tag)!.push({
        iteration: event.iteration,
        value: data.value.value,
      });
    } else if (data.case === "figure") {
      const tag = data.value.tag;
      if (!figures.has(tag)) {
        figures.set(tag, []);
      }
      figures.get(tag)!.push({
        iteration: event.iteration,
        image: data.value.image,
        data: data.value.data,
        interactive: data.value.interactive,
      });
    }
  }

  // Sort each by iteration
  for (const arr of scalars.values()) {
    arr.sort((a, b) => Number(a.iteration - b.iteration));
  }
  for (const arr of text.values()) {
    arr.sort((a, b) => Number(a.iteration - b.iteration));
  }
  for (const arr of figures.values()) {
    arr.sort((a, b) => Number(a.iteration - b.iteration));
  }

  return { scalars, text, figures };
}
