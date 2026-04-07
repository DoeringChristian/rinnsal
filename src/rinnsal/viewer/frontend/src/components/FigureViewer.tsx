import { useMemo, useState, useCallback, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { FigureMetaData, figureImageUrl } from "../lib/api";
import { getRunColor } from "./RunSelector";
import { CollapsibleSection } from "./CollapsibleSection";

// ─── Types ───────────────────────────────────────────────────────

interface CompareSlot {
  run: string;
  tag: string;
  iterations: number[];
  linked: boolean;
  localIt: number;
  width?: number; // persisted width in px
}

interface CompareGroup {
  id: number;
  name: string;
  slots: CompareSlot[];
  collapsed: boolean;
}

function closestIt(iterations: number[], target: number): number {
  let best = iterations[0];
  let bestDist = Math.abs(best - target);
  for (let i = 1; i < iterations.length; i++) {
    const dist = Math.abs(iterations[i] - target);
    if (dist < bestDist) {
      best = iterations[i];
      bestDist = dist;
    }
  }
  return best;
}

let nextGroupId = 1;

// Shared drag state for cross-window drag-and-drop.
// dataTransfer.getData() can be empty across windows in some browsers,
// so we store the dragged slot on the main window object.
const DRAG_KEY = "__rinnsal_drag_slot__";
function setDragSlot(slot: CompareSlot) {
  (window as any)[DRAG_KEY] = slot;
}
function getDragSlot(): CompareSlot | null {
  const slot = (window as any)[DRAG_KEY];
  return slot ?? null;
}
function clearDragSlot() {
  delete (window as any)[DRAG_KEY];
}

/** Check if a group already contains a slot with the same run+tag. */
function groupHasSlot(group: CompareGroup, slot: CompareSlot): boolean {
  return group.slots.some((s) => s.run === slot.run && s.tag === slot.tag);
}

// ─── Persistence ─────────────────────────────────────────────────

const COMPARE_STORAGE_KEY = "rinnsal-compare-groups";

function loadGroups(): CompareGroup[] {
  try {
    const raw = sessionStorage.getItem(COMPARE_STORAGE_KEY);
    if (raw) {
      const groups: CompareGroup[] = JSON.parse(raw);
      // Restore nextGroupId
      for (const g of groups) {
        if (g.id >= nextGroupId) nextGroupId = g.id + 1;
      }
      return groups;
    }
  } catch { /* ignore */ }
  return [];
}

function saveGroups(groups: CompareGroup[]) {
  try {
    sessionStorage.setItem(COMPARE_STORAGE_KEY, JSON.stringify(groups));
  } catch { /* ignore */ }
}

// ─── Compare Group Panel ─────────────────────────────────────────

interface CompareGroupPanelProps {
  group: CompareGroup;
  onUpdate: (group: CompareGroup) => void;
  onDelete: () => void;
  onPopout: () => void;
  onDropSlot: (slot: CompareSlot) => void;
}

function CompareGroupPanel({ group, onUpdate, onDelete, onPopout, onDropSlot }: CompareGroupPanelProps) {
  const [overlay, setOverlay] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState(group.name);
  const { slots } = group;

  const commitName = () => {
    const trimmed = editName.trim();
    if (trimmed && trimmed !== group.name) {
      onUpdate({ ...group, name: trimmed });
    }
    setEditing(false);
  };

  const linkedIterations = useMemo(() => {
    const set = new Set<number>();
    for (const s of slots) {
      if (s.linked) for (const it of s.iterations) set.add(it);
    }
    return Array.from(set).sort((a, b) => a - b);
  }, [slots]);

  const [globalIdx, setGlobalIdx] = useState(
    linkedIterations.length > 0 ? linkedIterations.length - 1 : 0,
  );
  const safeGlobalIdx = Math.min(globalIdx, Math.max(0, linkedIterations.length - 1));
  const globalIt = linkedIterations[safeGlobalIdx] ?? 0;

  const removeSlot = (idx: number) => {
    onUpdate({ ...group, slots: slots.filter((_, i) => i !== idx) });
  };

  const toggleLink = (idx: number) => {
    onUpdate({
      ...group,
      slots: slots.map((s, i) => (i === idx ? { ...s, linked: !s.linked } : s)),
    });
  };

  const setLocalIt = (idx: number, it: number) => {
    onUpdate({
      ...group,
      slots: slots.map((s, i) => (i === idx ? { ...s, localIt: it } : s)),
    });
  };

  const toggleCollapsed = () => {
    onUpdate({ ...group, collapsed: !group.collapsed });
  };

  // Drag-and-drop: accept slots dragged onto this group
  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  };
  const handleDragLeave = () => setDragOver(false);
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    // Try dataTransfer first, fall back to shared drag state (cross-window)
    let slotData: CompareSlot | null = null;
    try {
      const raw = e.dataTransfer.getData("application/json");
      if (raw) slotData = JSON.parse(raw);
    } catch { /* ignore */ }
    if (!slotData) slotData = getDragSlot();
    if (slotData) {
      onDropSlot(slotData);
      clearDragSlot();
    }
  };

  // Drag start for individual slots (to move between groups/windows)
  const [dragFromIdx, setDragFromIdx] = useState<number | null>(null);
  const [dragOverIdx, setDragOverIdx] = useState<number | null>(null);

  const handleSlotDragStart = (e: React.DragEvent, slot: CompareSlot, idx: number) => {
    e.dataTransfer.setData("application/json", JSON.stringify(slot));
    e.dataTransfer.setData("text/x-group-id", String(group.id));
    e.dataTransfer.effectAllowed = "copy";
    setDragSlot(slot);
    setDragFromIdx(idx);
  };

  const handleSlotDrop = (e: React.DragEvent, targetIdx: number) => {
    e.preventDefault();
    e.stopPropagation();
    setDragOverIdx(null);

    // Check if reordering within same group
    const sourceGroupId = e.dataTransfer.getData("text/x-group-id");
    if (sourceGroupId === String(group.id) && dragFromIdx !== null && dragFromIdx !== targetIdx) {
      // Reorder
      const newSlots = [...slots];
      const [moved] = newSlots.splice(dragFromIdx, 1);
      newSlots.splice(targetIdx > dragFromIdx ? targetIdx - 1 : targetIdx, 0, moved);
      onUpdate({ ...group, slots: newSlots });
    } else {
      // Adding from outside — use normal drop logic
      let slotData: CompareSlot | null = null;
      try {
        const raw = e.dataTransfer.getData("application/json");
        if (raw) slotData = JSON.parse(raw);
      } catch { /* ignore */ }
      if (!slotData) slotData = getDragSlot();
      if (slotData) {
        onDropSlot(slotData);
        clearDragSlot();
      }
    }
    setDragFromIdx(null);
  };

  const handleSlotDragEnd = () => {
    setDragFromIdx(null);
    setDragOverIdx(null);
  };

  // Track shift key for synchronized resize
  const shiftHeldRef = useRef(false);
  const slotRefsRef = useRef<Map<number, HTMLDivElement>>(new Map());

  // Setup keyboard listener + resize persistence on the correct window.
  // Uses a callback ref so it works when portaled into popup windows.
  const cleanupRef = useRef<(() => void) | null>(null);

  const containerRefCallback = useCallback((el: HTMLDivElement | null) => {
    if (cleanupRef.current) {
      cleanupRef.current();
      cleanupRef.current = null;
    }
    if (!el) return;

    const ownerWindow = el.ownerDocument?.defaultView || window;

    // Shift key tracking
    const down = (e: KeyboardEvent) => { if (e.key === "Shift") shiftHeldRef.current = true; };
    const up = (e: KeyboardEvent) => { if (e.key === "Shift") shiftHeldRef.current = false; };
    ownerWindow.addEventListener("keydown", down);
    ownerWindow.addEventListener("keyup", up);

    // ResizeObserver for live shift-sync only (DOM-only, no state updates).
    // Use a flag to prevent cascading: when we set a sibling's width,
    // ignore the resulting observer callback for that sibling.
    let syncing = false;
    const RO = (ownerWindow as any).ResizeObserver || ResizeObserver;
    const observer = new RO((entries: ResizeObserverEntry[]) => {
      if (!shiftHeldRef.current || syncing) return;
      for (const entry of entries) {
        const targetWidth = entry.target.getBoundingClientRect().width;
        syncing = true;
        slotRefsRef.current.forEach((slotEl) => {
          if (slotEl !== entry.target) {
            slotEl.style.width = `${Math.round(targetWidth)}px`;
          }
        });
        // Reset flag after current microtask so observer callbacks from
        // the sibling resizes are ignored
        requestAnimationFrame(() => { syncing = false; });
        break;
      }
    });
    slotRefsRef.current.forEach((slotEl) => observer.observe(slotEl));

    // On mouseup, persist widths to state (triggers save to sessionStorage)
    const onMouseUp = () => {
      let changed = false;
      const updatedSlots = [...slots];
      slotRefsRef.current.forEach((slotEl, idx) => {
        const w = Math.round(slotEl.getBoundingClientRect().width);
        if (idx < updatedSlots.length && updatedSlots[idx].width !== w) {
          updatedSlots[idx] = { ...updatedSlots[idx], width: w };
          changed = true;

          // If shift held, sync all to the same width
          if (shiftHeldRef.current) {
            for (let i = 0; i < updatedSlots.length; i++) {
              updatedSlots[i] = { ...updatedSlots[i], width: w };
            }
            slotRefsRef.current.forEach((otherEl) => {
              if (otherEl !== slotEl) otherEl.style.width = `${w}px`;
            });
          }
        }
      });
      if (changed) onUpdate({ ...group, slots: updatedSlots });
    };
    ownerWindow.addEventListener("mouseup", onMouseUp);

    cleanupRef.current = () => {
      ownerWindow.removeEventListener("keydown", down);
      ownerWindow.removeEventListener("keyup", up);
      ownerWindow.removeEventListener("mouseup", onMouseUp);
      observer.disconnect();
    };
  }, [slots, group, onUpdate]);

  const setSlotRef = (idx: number, el: HTMLDivElement | null) => {
    if (el) slotRefsRef.current.set(idx, el);
    else slotRefsRef.current.delete(idx);
  };

  const content = (
    <div ref={containerRefCallback}>
      {!group.collapsed && linkedIterations.length > 1 && (
        <div className="flex items-center gap-2 mb-3">
          <span className="text-xs text-gray-500 shrink-0">it: {globalIt}</span>
          <input type="range" min={0} max={linkedIterations.length - 1} value={safeGlobalIdx} onChange={(e) => setGlobalIdx(parseInt(e.target.value))} className="flex-1" />
        </div>
      )}
      {!group.collapsed && (
        <div className="flex flex-wrap items-start gap-2">
          {slots.map((slot, idx) => {
            const it = slot.linked ? closestIt(slot.iterations, globalIt) : slot.localIt;
            const runName = slot.run.split("/").pop() || slot.run;
            const color = getRunColor(slot.run);
            const url = figureImageUrl(slot.run, slot.tag, it);

            // Show indicator line on left or right edge depending on dragOverIdx
            const showLeftIndicator = dragOverIdx === idx;
            const showRightIndicator = dragOverIdx === idx + 1 && idx === slots.length - 1;

            return (
              <div
                key={`${slot.run}-${slot.tag}-${idx}`}
                ref={(el) => setSlotRef(idx, el)}
                className={`bg-white rounded-lg border border-gray-200 p-3 cursor-grab active:cursor-grabbing relative ${dragFromIdx === idx ? "opacity-40" : ""}`}
                style={{
                  resize: "horizontal",
                  overflow: "auto",
                  minWidth: 250,
                  width: slot.width ? `${slot.width}px` : undefined,
                  borderLeft: showLeftIndicator ? "3px solid #3b82f6" : undefined,
                  borderRight: showRightIndicator ? "3px solid #3b82f6" : undefined,
                }}
                draggable
                onDragStart={(e) => handleSlotDragStart(e, slot, idx)}
                onDragEnd={handleSlotDragEnd}
                onDragOver={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  // Determine if cursor is on left or right half
                  const rect = e.currentTarget.getBoundingClientRect();
                  const midX = rect.left + rect.width / 2;
                  setDragOverIdx(e.clientX < midX ? idx : idx + 1);
                }}
                onDragLeave={() => setDragOverIdx(null)}
                onDrop={(e) => {
                  const rect = e.currentTarget.getBoundingClientRect();
                  const midX = rect.left + rect.width / 2;
                  const insertIdx = e.clientX < midX ? idx : idx + 1;
                  handleSlotDrop(e, insertIdx);
                }}
              >
                <div className="flex items-center justify-between mb-2 gap-1">
                  <div className="flex-1 min-w-0">
                    <div className="text-xs font-medium truncate" style={{ color }}>{runName}</div>
                    <div className="text-xs text-gray-500 truncate">{slot.tag}</div>
                  </div>
                  <div className="flex items-center gap-1 shrink-0">
                    <span className="text-xs text-gray-400">it:{it}</span>
                    <button
                      onClick={() => toggleLink(idx)}
                      title={slot.linked ? "Unlink" : "Link"}
                      className={`px-1.5 py-0.5 text-xs rounded transition-colors ${slot.linked ? "bg-blue-100 text-blue-700" : "bg-gray-100 text-gray-500"}`}
                    >
                      {slot.linked ? "\uD83D\uDD17" : "\u26D3\uFE0F"}
                    </button>
                    <button onClick={() => removeSlot(idx)} className="text-gray-400 hover:text-red-500 text-sm px-1" title="Remove">{"\u00D7"}</button>
                  </div>
                </div>
                {!slot.linked && slot.iterations.length > 1 && (
                  <div className="mb-2">
                    <input type="range" min={0} max={slot.iterations.length - 1} value={slot.iterations.indexOf(slot.localIt)} onChange={(e) => setLocalIt(idx, slot.iterations[parseInt(e.target.value)])} className="w-full" />
                  </div>
                )}
                <img src={url} alt={`${runName} / ${slot.tag} @ ${it}`} className="w-full h-auto rounded object-contain" loading="lazy" />
              </div>
            );
          })}
          {slots.length === 0 && (
            <p className="text-xs text-gray-400 text-center py-4 w-full">Drag figures here or click + on figures below</p>
          )}
        </div>
      )}
    </div>
  );

  if (overlay) {
    return (
      <div className="fixed inset-0 z-50 bg-black/80 overflow-auto p-8" onClick={() => setOverlay(false)}>
        <div className="max-w-6xl mx-auto" onClick={(e) => e.stopPropagation()}>
          <div className="bg-gray-100 rounded-lg border border-gray-300 p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-white">{group.name}</h3>
              <button onClick={() => setOverlay(false)} className="px-2 py-1 text-xs bg-gray-700 text-white rounded hover:bg-gray-600">Exit fullscreen</button>
            </div>
            {content}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className={`rounded-lg border p-4 mb-4 transition-colors ${dragOver ? "bg-blue-50 border-blue-300" : "bg-gray-100 border-gray-300"}`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-1">
          <button onClick={toggleCollapsed} className="text-xs text-gray-400 hover:text-gray-600">
            {group.collapsed ? "\u25B6" : "\u25BC"}
          </button>
          {editing ? (
            <input
              autoFocus
              value={editName}
              onChange={(e) => setEditName(e.target.value)}
              onBlur={commitName}
              onKeyDown={(e) => { if (e.key === "Enter") commitName(); if (e.key === "Escape") setEditing(false); }}
              className="text-sm font-semibold text-gray-700 border-b border-blue-400 outline-none bg-transparent w-40"
            />
          ) : (
            <span
              className="text-sm font-semibold text-gray-700 cursor-text hover:text-gray-900"
              onDoubleClick={() => { setEditName(group.name); setEditing(true); }}
              title="Double-click to rename"
            >
              {group.name}
            </span>
          )}
          <span className="text-xs text-gray-400 font-normal">({slots.length})</span>
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => setOverlay(true)} title="Fullscreen" className="px-1.5 py-0.5 text-xs text-gray-500 hover:text-gray-700 rounded hover:bg-gray-200 transition-colors">{"\u26F6"}</button>
          <button onClick={onPopout} title="Open in new window" className="px-1.5 py-0.5 text-xs text-gray-500 hover:text-gray-700 rounded hover:bg-gray-200 transition-colors">{"\u2197"}</button>
          <button onClick={onDelete} title="Delete comparison" className="px-1.5 py-0.5 text-xs text-gray-400 hover:text-red-500 rounded hover:bg-gray-200 transition-colors">{"\u00D7"}</button>
        </div>
      </div>
      {content}
    </div>
  );
}

// ─── Pop-out Window (React Portal) ───────────────────────────────

interface PopoutWindowProps {
  group: CompareGroup;
  onUpdate: (group: CompareGroup) => void;
  onClose: () => void;
}

function PopoutWindow({ group, onUpdate, onClose }: PopoutWindowProps) {
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const windowRef = useRef<Window | null>(null);
  const [popoutCollapsed, setPopoutCollapsed] = useState(false);

  useEffect(() => {
    const w = window.open("", `compare-${group.id}`, "width=1200,height=900");
    if (!w) { onClose(); return; }

    windowRef.current = w;

    w.document.write("<!DOCTYPE html><html><head></head><body><div id='root'></div></body></html>");
    w.document.close();

    // Copy stylesheets from parent
    for (const sheet of document.styleSheets) {
      try {
        if (sheet.href) {
          const link = w.document.createElement("link");
          link.rel = "stylesheet";
          link.href = sheet.href;
          w.document.head.appendChild(link);
        }
      } catch { /* cross-origin, skip */ }
    }

    const style = w.document.createElement("style");
    style.textContent = "body { font-family: system-ui, sans-serif; background: #f9fafb; margin: 0; padding: 16px; }";
    w.document.head.appendChild(style);

    setContainer(w.document.getElementById("root") as HTMLDivElement);

    // Allow drag-over in the popout window (needed for drop to work)
    const onDragOver = (e: DragEvent) => {
      e.preventDefault();
      if (e.dataTransfer) e.dataTransfer.dropEffect = "copy";
    };
    w.document.addEventListener("dragover", onDragOver);

    w.addEventListener("beforeunload", onClose);
    return () => {
      w.document.removeEventListener("dragover", onDragOver);
      w.removeEventListener("beforeunload", onClose);
      w.close();
    };
  }, []);

  if (!container) return null;

  const mergedGroup = { ...group, collapsed: popoutCollapsed };

  const handlePopoutUpdate = (updated: CompareGroup) => {
    setPopoutCollapsed(updated.collapsed);
    onUpdate({ ...updated, collapsed: group.collapsed }); // don't sync collapse to main
  };

  const handlePopoutDrop = (slot: CompareSlot) => {
    if (groupHasSlot(group, slot)) return;
    onUpdate({ ...group, slots: [...group.slots, slot] });
  };

  return createPortal(
    <CompareGroupPanel
      group={mergedGroup}
      onUpdate={handlePopoutUpdate}
      onDelete={onClose}
      onPopout={() => {}}
      onDropSlot={handlePopoutDrop}
    />,
    container,
  );
}

// ─── Add-to-Compare Dropdown ─────────────────────────────────────

interface AddToCompareButtonProps {
  groups: CompareGroup[];
  onAdd: (groupId: number | null) => void; // null = new group
}

function AddToCompareButton({ groups, onAdd }: AddToCompareButtonProps) {
  const [open, setOpen] = useState(false);

  // 0 groups: create new
  // 1 group: add directly to it
  if (groups.length <= 1) {
    return (
      <button
        onClick={() => onAdd(groups.length === 1 ? groups[0].id : null)}
        title="Add to comparison"
        className="w-6 h-6 flex items-center justify-center rounded-full border border-gray-300 text-gray-400 hover:border-blue-400 hover:text-blue-600 hover:bg-blue-50 transition-colors text-sm"
      >
        +
      </button>
    );
  }

  // 2+ groups: show dropdown
  return (
    <div className="relative">
      <button
        onClick={() => setOpen(!open)}
        title="Add to comparison"
        className="w-6 h-6 flex items-center justify-center rounded-full border border-gray-300 text-gray-400 hover:border-blue-400 hover:text-blue-600 hover:bg-blue-50 transition-colors text-sm"
      >
        +
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className="absolute right-0 top-8 z-50 bg-white border border-gray-200 rounded-lg shadow-lg py-1 min-w-[160px]">
            {groups.map((g) => (
              <button
                key={g.id}
                onClick={() => { onAdd(g.id); setOpen(false); }}
                className="block w-full text-left px-3 py-1.5 text-xs text-gray-700 hover:bg-blue-50 hover:text-blue-700"
              >
                {g.name} ({g.slots.length})
              </button>
            ))}
            <hr className="my-1 border-gray-100" />
            <button
              onClick={() => { onAdd(null); setOpen(false); }}
              className="block w-full text-left px-3 py-1.5 text-xs text-gray-500 hover:bg-gray-50"
            >
              + New comparison
            </button>
          </div>
        </>
      )}
    </div>
  );
}

// ─── Main FigureViewer ───────────────────────────────────────────

interface FigureViewerProps {
  data: Map<string, FigureMetaData>;
  selectedRuns: string[];
}

export default function FigureViewer({ data, selectedRuns }: FigureViewerProps) {
  const [groups, setGroups] = useState<CompareGroup[]>(loadGroups);

  // Persist groups on change
  useEffect(() => {
    saveGroups(groups);
  }, [groups]);

  const allTags = useMemo(() => {
    const tags = new Set<string>();
    for (const runData of data.values()) {
      for (const tag of Object.keys(runData)) tags.add(tag);
    }
    return Array.from(tags).sort();
  }, [data]);

  const addToGroup = useCallback(
    (run: string, tag: string, groupId: number | null) => {
      const runData = data.get(run);
      if (!runData || !runData[tag]) return;
      const iterations = runData[tag].map((f) => f.it);
      if (iterations.length === 0) return;

      const newSlot: CompareSlot = {
        run, tag, iterations, linked: true, localIt: iterations[iterations.length - 1],
      };

      setGroups((prev) => {
        if (groupId !== null) {
          return prev.map((g) => {
            if (g.id !== groupId) return g;
            if (groupHasSlot(g, newSlot)) return g; // no duplicates
            return { ...g, slots: [...g.slots, newSlot] };
          });
        }
        const id = nextGroupId++;
        return [...prev, { id, name: `Comparison ${id}`, slots: [newSlot], collapsed: false }];
      });
    },
    [data],
  );

  const newGroup = useCallback(() => {
    const id = nextGroupId++;
    setGroups((prev) => [...prev, { id, name: `Comparison ${id}`, slots: [], collapsed: false }]);
  }, []);

  const updateGroup = useCallback((updated: CompareGroup) => {
    setGroups((prev) => prev.map((g) => (g.id === updated.id ? updated : g)));
  }, []);

  const deleteGroup = useCallback((id: number) => {
    setGroups((prev) => prev.filter((g) => g.id !== id));
  }, []);

  // Handle dropping a slot onto a group — add without removing from others
  const handleDropSlot = useCallback((targetGroupId: number, slot: CompareSlot) => {
    setGroups((prev) =>
      prev.map((g) => {
        if (g.id !== targetGroupId) return g;
        if (groupHasSlot(g, slot)) return g;
        return { ...g, slots: [...g.slots, slot] };
      }),
    );
  }, []);

  // Track which groups are popped out
  const [poppedOut, setPoppedOut] = useState<Set<number>>(new Set());

  const popout = useCallback((id: number) => {
    setPoppedOut((prev) => new Set(prev).add(id));
  }, []);

  const closePopout = useCallback((id: number) => {
    setPoppedOut((prev) => { const next = new Set(prev); next.delete(id); return next; });
  }, []);

  if (allTags.length === 0) {
    return <p className="text-gray-500 text-center mt-8">No figures logged in selected runs.</p>;
  }

  return (
    <div>
      {/* Comparison groups */}
      {groups.length > 0 && (
        <div className="mb-6">
          <div className="flex items-center gap-2 mb-3 flex-wrap">
            <span className="text-xs text-gray-500 font-medium">Comparisons:</span>
            <button onClick={newGroup} className="px-2 py-1 text-xs rounded border border-dashed border-gray-300 text-gray-400 hover:text-gray-600 hover:border-gray-400 transition-colors">
              + New
            </button>
          </div>
          {groups.map((g) => (
            <CompareGroupPanel
              key={g.id}
              group={g}
              onUpdate={updateGroup}
              onDelete={() => deleteGroup(g.id)}
              onPopout={() => popout(g.id)}
              onDropSlot={(slot) => handleDropSlot(g.id, slot)}
            />
          ))}
          {/* Render React portals for popped-out groups */}
          {groups.filter((g) => poppedOut.has(g.id)).map((g) => (
            <PopoutWindow
              key={`popout-${g.id}`}
              group={g}
              onUpdate={updateGroup}
              onClose={() => closePopout(g.id)}
            />
          ))}
        </div>
      )}

      {groups.length === 0 && (
        <p className="text-xs text-gray-400 mb-4">Click + on figures below to start a comparison</p>
      )}

      {/* Normal figure grid */}
      <div className="space-y-6">
        {allTags.map((tag) => (
          <CollapsibleSection key={tag} title={tag}>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {Array.from(data).map(([run, runData]) => {
                const figs = runData[tag];
                if (!figs || figs.length === 0) return null;
                return (
                  <FigureRunCard
                    key={run}
                    run={run}
                    runPath={selectedRuns.find((r) => r === run) || run}
                    tag={tag}
                    figures={figs}
                    color={getRunColor(run)}
                    groups={groups}
                    onAddToCompare={(groupId) => addToGroup(run, tag, groupId)}
                  />
                );
              })}
            </div>
          </CollapsibleSection>
        ))}
      </div>
    </div>
  );
}

// ─── Figure Card ─────────────────────────────────────────────────

interface FigureRunCardProps {
  run: string;
  runPath: string;
  tag: string;
  figures: { it: number }[];
  color: string;
  groups: CompareGroup[];
  onAddToCompare: (groupId: number | null) => void;
}

function FigureRunCard({ run, runPath, tag, figures, color, groups, onAddToCompare }: FigureRunCardProps) {
  const [selectedIdx, setSelectedIdx] = useState(figures.length - 1);
  const runName = run.split("/").pop() || run;
  const currentFigure = figures[selectedIdx];
  const imageUrl = figureImageUrl(runPath, tag, currentFigure.it);

  // Make card draggable into comparison groups (including cross-window)
  const handleDragStart = (e: React.DragEvent) => {
    const slot: CompareSlot = {
      run, tag, iterations: figures.map((f) => f.it), linked: true,
      localIt: figures[figures.length - 1].it,
    };
    e.dataTransfer.setData("application/json", JSON.stringify(slot));
    e.dataTransfer.effectAllowed = "copy";
    setDragSlot(slot);
  };

  return (
    <div
      className="bg-white rounded-lg border border-gray-200 p-4 cursor-grab active:cursor-grabbing"
      draggable
      onDragStart={handleDragStart}
    >
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium" style={{ color }}>{runName}</span>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">it:{currentFigure.it}</span>
          <AddToCompareButton groups={groups} onAdd={onAddToCompare} />
        </div>
      </div>
      {figures.length > 1 && (
        <div className="mb-3">
          <input type="range" min={0} max={figures.length - 1} value={selectedIdx} onChange={(e) => setSelectedIdx(parseInt(e.target.value))} className="w-full" />
        </div>
      )}
      <img src={imageUrl} alt={`${runName} - iteration ${currentFigure.it}`} className="max-w-full rounded" loading="lazy" />
    </div>
  );
}
