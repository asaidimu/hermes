import type { NodeDef } from "../../../src/types";

export const ifNode: NodeDef = {
  kind: "if",
  label: "If / Condition",
  description: "Branch to 'true' or 'false' output based on conditions with AND/OR combinators.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "if",
    fields: {
      mode: { name: "mode", type: "string", default: "simple", required: true },
      key: {
        name: "key",
        type: "string",
        default: "state.value",
        required: true,
      },
      predicate: { name: "predicate", type: "string", default: "===", required: true },
      value: { name: "value", type: "string", default: "10", required: true },
      condition: {
        name: "condition",
        type: "union",
        schema: [{ id: "simpleCondition" }, { id: "complexCondition" }],
        required: true,
      },
    },
    schemas: {
      simpleCondition: {
        name: "simpleCondition",
        fields: {
          key: { name: "key", type: "string", required: true },
          predicate: { name: "predicate", type: "string", required: true },
          value: { name: "value", type: "string", required: true },
        },
      },
      complexCondition: { name: "complexCondition", type: "string" },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "if", label: "true" },
    { type: "source", id: "else", label: "false" },
  ],
};