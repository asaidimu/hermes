import type { NodeDef } from "../../../src/types";

export const delayNode: NodeDef = {
  kind: "delay",
  label: "Delay",
  description: "Wait for a given number of milliseconds.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "delay",
    fields: {
      ms: { name: "ms", type: "number", default: 1000, required: true },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "" },
  ],
};