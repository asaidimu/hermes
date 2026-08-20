import type { NodeDef } from "../../../src/types";

export const tryCatchNode: NodeDef = {
  kind: "try-catch",
  label: "Try / Catch",
  description: "Execute a sub-flow and catch any errors it raises.",
  type: "executable",
  bodyHandle: "try",
  configSchema: {
    version: "1.0.0",
    name: "try-catch",
    fields: {
      errorKey: { name: "errorKey", type: "string", default: "error", required: true },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "done", label: "done" },
    { type: "source", id: "catch", label: "catch" },
    { type: "source", id: "try", label: "try" },
  ],
};