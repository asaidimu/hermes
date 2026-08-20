import type { NodeDef } from "../../../src/types";

export const queryNode: NodeDef = {
  kind: "query",
  label: "Database Query",
  description: "Execute a query against a database service collection.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "query",
    fields: {
      collection: { name: "collection", type: "string", default: "", required: true },
      operation: { name: "operation", type: "string", default: "find", required: true },
      query: { name: "query", type: "record", default: {}, required: false },
      data: { name: "data", type: "record", default: {}, required: false },
      key: { name: "key", type: "string", default: "", required: false },
    },
  },
  handles: () => [
    { type: "target", id: "", kind: "executable" },
    { type: "source", id: "", kind: "executable" },
    { type: "target", id: "service", kind: "resource", label: "Database Service" },
  ],
};