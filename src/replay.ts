// ---------------------------------------------------------------------------
// Client-side action log replay — reconstruct state at any step without
// backend round-trips. Fetch the full log once, step through it locally.
// ---------------------------------------------------------------------------

/** Matches the JSON shape of Go's actionlog.Entry. */
export interface ActionLogEntry {
  runId: string;
  rerunIndex: number;
  seq: number;
  kind: string;
  pipelineId?: string;
  stageId?: string;
  stepId?: string;
  handle?: string;
  type?: string;
  payload?: Record<string, unknown>;
  output?: Record<string, unknown>;
  error?: string;
  duration?: number;
  attempt?: number;
  timestamp: string;
}

/** A single point on the timeline with its accumulated state. */
export interface TimelineSnapshot {
  seq: number;
  kind: string;
  type?: string;
  stageId?: string;
  stepId?: string;
  delta?: Record<string, unknown>;
  state: Record<string, unknown>;
  timestamp: string;
  duration?: number;
  error?: string;
}

function deepMerge(
  target: Record<string, unknown>,
  delta: Record<string, unknown>,
): Record<string, unknown> {
  const out = { ...target };
  for (const [k, v] of Object.entries(delta)) {
    out[k] = v;
  }
  return out;
}

/**
 * Client-side replayer. Loads a full action log and reconstructs state at
 * any sequence number by merging recorded state deltas.
 *
 * Usage:
 * ```ts
 * const entries = await fetch(`/api/runs/${runId}/log`).then(r => r.json())
 * const replayer = new Replayer()
 * replayer.load(entries)
 *
 * // State at a specific event
 * const state = replayer.stateAt(5)
 *
 * // Full timeline for scrubber
 * const timeline = replayer.timeline()
 * ```
 */
export class Replayer {
  private entries: ActionLogEntry[] = [];
  private snapshots = new Map<number, Record<string, unknown>>();
  private empty: Record<string, unknown> = {};

  /**
   * Load entries from the action log. Entries are sorted by `seq`
   * automatically — no pre-sorting required.
   */
  load(entries: ActionLogEntry[]): void {
    this.entries = [...entries].sort((a, b) => a.seq - b.seq);
    this.snapshots.clear();
    this.buildSnapshots();
  }

  /** State after the entry at the given seq. */
  stateAt(seq: number): Record<string, unknown> {
    return this.snapshots.get(seq) ?? this.empty;
  }

  /** State before the entry at the given seq (i.e. after the previous entry). */
  stateBefore(seq: number): Record<string, unknown> {
    const idx = this.entries.findIndex((e) => e.seq === seq);
    if (idx <= 0) return this.empty;
    return this.snapshots.get(this.entries[idx - 1].seq) ?? this.empty;
  }

  /** Full timeline with accumulated state at each entry. */
  timeline(): TimelineSnapshot[] {
    return this.entries.map((e) => ({
      seq: e.seq,
      kind: e.kind,
      type: e.type,
      stageId: e.stageId,
      stepId: e.stepId,
      delta: e.output,
      state: this.snapshots.get(e.seq) ?? this.empty,
      timestamp: e.timestamp,
      duration: e.duration,
      error: e.error,
    }));
  }

  /** State after the last entry. */
  finalState(): Record<string, unknown> {
    if (this.entries.length === 0) return this.empty;
    return this.snapshots.get(this.entries[this.entries.length - 1].seq) ?? this.empty;
  }

  /** Raw entries in seq order. */
  getEntries(): readonly ActionLogEntry[] {
    return this.entries;
  }

  /** Number of loaded entries. */
  get size(): number {
    return this.entries.length;
  }

  private buildSnapshots(): void {
    let state: Record<string, unknown> = {};
    for (const entry of this.entries) {
      if (entry.output && Object.keys(entry.output).length > 0) {
        state = deepMerge(state, entry.output);
      }
      this.snapshots.set(entry.seq, state);
    }
  }
}
