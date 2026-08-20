import type { HandleSpec, NodeDef } from "../../../src/types";

export const switchNode: NodeDef = {
  kind: "switch",
  label: "Switch",
  description: "Match a workflow state value against several static cases to branch paths.",
  type: "executable",
  configSchema: {
    version: "1.0.0",
    name: "switch",
    fields: {
      value: { name: "value", type: "string", default: "state.value", required: true },
      cases: { name: "cases", type: "string", default: "[]", required: true },
      defaultHandle: { name: "defaultHandle", type: "string", default: "default", required: true },
    },
  },
  handles: (config) => {
    const specs: HandleSpec[] = [{ type: "target", id: "", label: "in" }];
    try {
      const parsed = JSON.parse(config.cases || "[]");
      if (Array.isArray(parsed)) {
        parsed.forEach((item: any) => {
          if (item.id) {
            specs.push({
              type: "source",
              id: String(item.id),
              label: item.label === "" ? `""` : String(item.label),
            });
          }
        });
      } else {
        for (const [match, label] of Object.entries(parsed)) {
          specs.push({ type: "source", id: String(label), label: String(match) });
        }
      }
    } catch {}
    if (config.defaultHandle) {
      specs.push({ type: "source", id: String(config.defaultHandle), label: "default" });
    }
    return specs;
  },
};