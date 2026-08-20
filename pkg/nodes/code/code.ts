import type { NodeDef } from "../../../src/types";

const DEFAULT_CODE = `// Example: Transform text to uppercase
return {
  text: state.text?.toUpperCase()
};`;

export const codeNode: NodeDef = {
  kind: "code",
  label: "JavaScript Code",
  description: "Execute custom JS transformations on the workflow state.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "code",
    fields: {
      code: { name: "code", type: "string", default: DEFAULT_CODE, required: true },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "" },
  ],
};