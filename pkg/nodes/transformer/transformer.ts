import type { NodeDef } from "../../../src/types";

export const transformerNode: NodeDef = {
  kind: "transformer",
  label: "Transformer",
  description: "Manipulate workflow state by applying transformation rules.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "transformer",
    fields: {
      rules: { name: "rules", type: "array", schema: { type: "record" }, required: true, default: [] },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "" },
  ],
};