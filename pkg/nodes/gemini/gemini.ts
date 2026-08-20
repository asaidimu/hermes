import type { NodeDef } from "../../../src/types";

export const geminiNode: NodeDef = {
  kind: "gemini",
  label: "Gemini AI",
  description: "Execute structured prompt queries against Google Gemini model configurations.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "gemini",
    fields: {
      apiKey: { name: "apiKey", type: "string", default: "", required: true },
      model: { name: "model", type: "string", default: "gemini-2.5-flash", required: true },
      prompt: { name: "prompt", type: "string", default: "", required: true },
      systemInstruction: { name: "systemInstruction", type: "string", default: "", required: false },
      key: { name: "key", type: "string", default: "gemini_response", required: true },
      temperature: { name: "temperature", type: "number", default: 0.7, required: false },
      jsonMode: { name: "jsonMode", type: "boolean", default: false, required: false },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "" },
  ],
};