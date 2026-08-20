import type { HandleSpec } from "./types";
import type { ConfigSchema, NodeCatalogEntry, NodeDef } from "./types";

/**
 * Collects `default` values from a configSchema's fields, mirroring the utils
 * `defineNode`/`defineResource` defaults computation.
 */
export function collectDefaults(schema: ConfigSchema): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, field] of Object.entries(schema.fields ?? {})) {
    if (field.default !== undefined) out[key] = field.default;
  }
  return out;
}

/**
 * Normalizes a NodeDef's `handles` into a plain function, so the emitted
 * handles.js always contains one function per kind (the client evaluates it with
 * `new Function("return (" + code + ")")`).
 */
export function handlesToFunction(
  def: NodeDef,
): (config: Record<string, any>) => HandleSpec[] {
  if (typeof def.handles === "function") return def.handles;
  const staticHandles = def.handles as HandleSpec[];
  return () => staticHandles;
}

/**
 * Builds the JS source for a `{ [kind]: function(config){...}, ... }` object
 * literal. Matches the contract the client evaluates:
 * `new Function("return (" + code + ")")`.
 */
export function buildHandlesJS(nodes: Record<string, NodeDef>): string {
  const entries: string[] = [];
  for (const [kind, def] of Object.entries(nodes)) {
    const fn = handlesToFunction(def);
    entries.push(`${JSON.stringify(kind)}: (${fn.toString()})`);
  }
  return `{\n${entries.join(",\n")}\n}`;
}

/**
 * Builds the catalog array for a set of node definitions: metadata + defaults.
 */
export function buildRegistryJSON(
  nodes: Record<string, NodeDef>,
): NodeCatalogEntry[] {
  return Object.values(nodes).map((def) => ({
    kind: def.kind,
    label: def.label,
    description: def.description,
    icon: def.icon,
    configSchema: def.configSchema,
    scope: def.scope,
    type: def.type,
    bodyHandle: def.bodyHandle,
    defaults: collectDefaults(def.configSchema),
  }));
}

export type { ConfigSchema, NodeCatalogEntry, NodeDef };