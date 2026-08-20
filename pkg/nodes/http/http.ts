import type { NodeDef } from "../../../src/types";

export const httpNode: NodeDef = {
  kind: "http",
  label: "HTTP Request",
  description: "Execute a standard HTTP request to an external service or API.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "http",
    fields: {
      method: { name: "method", type: "string", default: "GET", required: true },
      url: { name: "url", type: "string", default: "", required: true },
      key: { name: "key", type: "string", default: "", required: false },
      headers: { name: "headers", type: "array", default: [], required: false },
      params: { name: "params", type: "array", default: [], required: false },
      body: { name: "body", type: "string", default: "", required: false },
      responseType: { name: "responseType", type: "string", default: "json", required: false },
      throwOnError: { name: "throwOnError", type: "boolean", default: true, required: false },
      timeoutMs: { name: "timeoutMs", type: "number", default: 30000, required: false },
    },
  },
  handles: () => [
    { type: "target", id: "" },
    { type: "source", id: "" },
  ],
};