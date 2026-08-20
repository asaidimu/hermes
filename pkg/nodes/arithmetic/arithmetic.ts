import type { NodeDef } from "../../../src/types";

export const arithmeticNode: NodeDef = {
  kind: "arithmetic",
  label: "Arithmetic",
  description: "Performs a mathematical operation on two values.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "arithmetic",
    fields: {
      operation: { name: "operation", type: "string", default: "add", required: true },
      left: { name: "left", type: "string", default: "", required: true },
      right: { name: "right", type: "string", default: "", required: true },
      key: { name: "key", type: "string", default: "", required: true },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "" },
  ],
};