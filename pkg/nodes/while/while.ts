import type { NodeDef } from "../../../src/types";

export const whileNode: NodeDef = {
  kind: "while",
  label: "While Loop",
  description: "Repeatedly execute the 'do' branch as long as the condition remains true.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "while",
    fields: {
      mode: { name: "mode", type: "string", default: "simple", required: true },
      key: { name: "key", type: "string", default: "state.index", required: true },
      predicate: { name: "predicate", type: "string", default: "<", required: true },
      value: { name: "value", type: "string", default: "5", required: true },
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
    { type: "source", id: "done", label: "done" },
    { type: "source", id: "do", label: "do" },
  ],
};
