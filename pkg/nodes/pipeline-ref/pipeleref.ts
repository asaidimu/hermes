import type { NodeDef } from "../../../src/types";

export const pipelineRefNode: NodeDef = {
  kind: "pipeline-ref",
  label: "Pipeline Reference",
  description: "Invoke a registered sub-pipeline with fresh state and optional result merging.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "pipeline-ref",
    fields: {
      pipelineId: { name: "pipelineId", type: "string", required: true },
      initialState: { name: "initialState", type: "record" },
      resultKey: { name: "resultKey", type: "string" },
    },
  },
  handles: (config) => [
    { type: "target", id: "" },
    { type: "source", id: "success", label: "Success" },
    { type: "source", id: "failure", label: "Failure" },
  ],
};
