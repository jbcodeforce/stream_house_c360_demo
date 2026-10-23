export interface DataProduct {
  id: string
  name: string
  description: string
  statement_count: number
}

export interface GraphNode {
  id: string
  type: 'source' | 'statement'
  label: string
  statement_name: string | null
  topic: string | null
  is_product: boolean
}

export interface GraphEdge {
  source: string
  target: string
}

export interface Graph {
  nodes: GraphNode[]
  edges: GraphEdge[]
}

export type Phase = 'RUNNING' | 'PENDING' | 'DEGRADED' | 'FAILED' | 'STOPPED' | 'UNKNOWN'

export interface NodeStatus {
  node_id: string
  statement_name: string | null
  phase: Phase
  lagging: boolean
}

export interface StatusRollup {
  running: number
  total: number
  lagging: number
}

export interface StatusResponse {
  statements: NodeStatus[]
  rollup: StatusRollup
}

export interface NodeMetrics {
  node_id: string
  phase: Phase
  uptime_seconds: number | null
  messages_in_per_sec: number | null
  messages_out_per_sec: number | null
  pending_records: number | null
  compute_pool_cfu: number | null
  sql: string | null
}
