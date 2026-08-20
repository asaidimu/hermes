import { NODE_DEFS } from "./generated";
import { buildRegistryJSON, collectDefaults, handlesToFunction } from "./serialize";
import type { NodeCatalogEntry } from "./types";

export * from "./types";
export * from "./serialize";

/** All node definitions keyed by kind (generated from the per-kind node packages). */
export { NODE_DEFS } from "./generated";

/** Handles functions keyed by kind — the runtime `handles.js` source of truth. */
export const HANDLES: Record<string, (config: Record<string, any>) => any[]> =
  Object.fromEntries(
    Object.entries(NODE_DEFS).map(([kind, def]) => [kind, handlesToFunction(def)]),
  );

/** Node catalog metadata (includes defaults) keyed by kind. */
export const CATALOG: NodeCatalogEntry[] = buildRegistryJSON(NODE_DEFS);