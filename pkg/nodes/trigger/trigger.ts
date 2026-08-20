import type { NodeDef } from "../../../src/types";

export const triggerNode: NodeDef = {
  kind: "trigger",
  label: "Trigger",
  description: "Starts the state machine workflow with injectables/initial state context.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "trigger",
    fields: {
      initialState: { name: "initialState", type: "record", required: true },
    },
  },
  handles: () => [{ type: "source", id: "" }],
};