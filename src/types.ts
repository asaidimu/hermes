// ---------------------------------------------------------------------------
// Node handle & definition types
// ---------------------------------------------------------------------------

export type HandleType = "source" | "target";

export type HandleKind = "executable" | "resource";

export interface HandleSpec {
  type: HandleType;
  id: string;
  label?: string;
  /** Constrains which peer node kinds can connect to this handle. Defaults to "executable". */
  kind?: HandleKind;
}

export type NodeType = "executable" | "resource";

export interface ConfigField {
  name: string;
  type: string;
  default?: unknown;
  required?: boolean;
  schema?: unknown;
}

export interface ConfigSchema {
  version: string;
  name: string;
  fields: Record<string, ConfigField>;
  schemas?: Record<string, unknown>;
}

/** A per-kind node definition as declared in pkg/nodes/<kind>/<kind>.ts. */
export interface NodeDef {
  kind: string;
  label: string;
  description?: string;
  icon?: string;
  configSchema: ConfigSchema;
  scope?: string;
  type: NodeType;
  bodyHandle?: string;
  handles: ((config: Record<string, any>) => HandleSpec[]) | HandleSpec[];
}

/** Catalog metadata for a node kind, as shipped in the package. */
export interface NodeCatalogEntry {
  kind: string;
  label: string;
  description?: string;
  icon?: string;
  configSchema: ConfigSchema;
  scope?: string;
  type: NodeType;
  bodyHandle?: string;
  defaults: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Wire types — workflow graph
// ---------------------------------------------------------------------------

export interface WorkflowNode {
  id: string;
  type?: string;
  data: Record<string, any>;
  parentId?: string;
  position: { x: number; y: number };
}

export type EdgeRole = "flow" | "dependency" | "placeholder";

export interface WorkflowEdgeData {
  role: EdgeRole;
}

export interface WorkflowEdge {
  id: string;
  source: string;
  sourceHandle?: string;
  target: string;
  targetHandle?: string;
  data?: WorkflowEdgeData;
}

export interface WorkflowState {
  [key: string]: any;
}

// ---------------------------------------------------------------------------
// Wire types — events, triggers, outcomes
// ---------------------------------------------------------------------------

export interface WorkflowEvent<Payload = unknown> {
  type: string;
  payload: Payload;
  timestamp: number;
  state?: WorkflowState;
}

export type WorkflowTrigger = {
  id: string;
  event: string;
  predicate: (event: WorkflowEvent, state?: WorkflowState) => boolean;
};

export type Severity = "INFO" | "OK" | "WARN" | "ERROR";

export interface EventPathItem {
  kind: "pipeline" | "stage" | "step";
  id: string;
  label: string;
}

export type EventPath = ReadonlyArray<EventPathItem>;

interface BaseEvent {
  at: number;
  elapsed: number;
  path: EventPath;
  runId: string;
}

export type PipelineEvent =
  | (BaseEvent & {
      type: "pipeline:start";
      pipelineId: string;
      pipelineLabel: string;
    })
  | (BaseEvent & {
      type: "pipeline:success";
      pipelineId: string;
      pipelineLabel: string;
      finalState: WorkflowState;
    })
  | (BaseEvent & {
      type: "pipeline:failure";
      pipelineId: string;
      pipelineLabel: string;
      errorMessage: string;
    })
  | (BaseEvent & {
      type: "stage:start";
      stageId: string;
      stageLabel: string;
      mode: "steps" | "pipelines";
    })
  | (BaseEvent & {
      type: "stage:success";
      stageId: string;
      stageLabel: string;
      nextInstruction: unknown;
    })
  | (BaseEvent & {
      type: "stage:failure";
      stageId: string;
      stageLabel: string;
      errorMessage: string;
    })
  | (BaseEvent & { type: "step:start"; stepId: string; stepLabel: string })
  | (BaseEvent & {
      type: "step:success";
      stepId: string;
      stepLabel: string;
      durationMs: number;
    })
  | (BaseEvent & {
      type: "step:failure";
      stepId: string;
      stepLabel: string;
      errorMessage: string;
    })
  | (BaseEvent & {
      type: "router:evaluated";
      stageId: string;
      stageLabel: string;
      instruction: unknown;
      interpretation: string;
    })
  | (BaseEvent & {
      type: "subpipeline:fork";
      stageId: string;
      stageLabel: string;
      subPipelineIds: string[];
    })
  | (BaseEvent & {
      type: "subpipeline:join";
      stageId: string;
      stageLabel: string;
      results: Record<string, unknown>;
    })
  | (BaseEvent & {
      type: "resource:init";
      resourceId: string;
      resourceKind: string;
      resourceLabel: string;
    })
  | (BaseEvent & {
      type: "resource:ready";
      resourceId: string;
      resourceKind: string;
      resourceLabel: string;
    })
  | (BaseEvent & {
      type: "resource:init:failure";
      resourceId: string;
      resourceKind: string;
      resourceLabel: string;
      errorMessage: string;
    })
  | (BaseEvent & {
      type: "resource:cleanup";
      resourceId: string;
      resourceKind: string;
      resourceLabel: string;
    })
  | (BaseEvent & {
      type: "resource:cleanup:failure";
      resourceId: string;
      resourceKind: string;
      resourceLabel: string;
      errorMessage: string;
    });

export interface RunOutcome {
  ok: boolean;
  status: string;
  finalState?: WorkflowState;
  error?: string;
  cleanupErrors?: Array<{ resourceId: string; message: string }>;
  executedNodeIds?: string[];
}

/** Per-event metadata consumed by the timeline UI to reconstruct node statuses. */
export interface TimelineEventMeta {
  runId: string;
  nodeId?: string;
  label?: string;
  status: string;
  timestamp: number;
  [key: string]: any;
}

export type RunTimelineMeta = Record<string, TimelineEventMeta>;