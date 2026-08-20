import type { NodeDef } from "../../../src/types";

export const databaseServiceNode: NodeDef = {
  kind: "database",
  label: "Database Service",
  description: "Provides a database instance to workflow nodes via the artifact container.",
  type: "resource",
  configSchema: {
    version: "1.0.0",
    name: "database",
    fields: {
      databaseName: { name: "databaseName", type: "string", default: "workflow", required: true },
    },
  },
  handles: () => [{ type: "source", id: "db", kind: "resource" }],
};