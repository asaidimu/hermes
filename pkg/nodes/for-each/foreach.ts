import type { NodeDef } from "../../../src/types";

export const forEachNode: NodeDef = {
  kind: "for-each",
  label: "For Each / Iterator",
  description: "Iterate over an array or object collection step-by-step.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "for-each",
    fields: {
      itemsKey: { name: "itemsKey", type: "string", default: "items", required: true },
      itemKey: { name: "itemKey", type: "string", default: "item", required: true },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "done", label: "done" },
    { type: "source", id: "do", label: "do" },
  ],
};