import { Handle, Position } from '@xyflow/react'
import type { NodeProps } from '@xyflow/react'
import type { Phase } from '../types/dataProduct'

export interface StatementNodeData {
  label: string
  nodeType: 'source' | 'statement'
  phase: Phase
  lagging: boolean
  is_product: boolean
  selected: boolean
  [key: string]: unknown
}

/**
 * Maps a node to a status modifier class. Source (topic) nodes are always
 * neutral; statement nodes take their colour from live phase/lag.
 */
function statusClass(
  nodeType: 'source' | 'statement',
  phase: Phase,
  lagging: boolean,
): string {
  if (nodeType === 'source') return 'statement-node--neutral'
  if (phase === 'RUNNING') return lagging ? 'statement-node--lagging' : 'statement-node--running'
  if (phase === 'DEGRADED') return 'statement-node--lagging'
  if (phase === 'FAILED' || phase === 'STOPPED') return 'statement-node--failed'
  return 'statement-node--unknown'
}

export default function StatementNode({ data }: NodeProps) {
  const d = data as StatementNodeData
  const classes = [
    'statement-node',
    statusClass(d.nodeType, d.phase, d.lagging),
    d.is_product ? 'statement-node--product' : '',
    d.selected ? 'statement-node--selected' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div className={classes}>
      <Handle type="target" position={Position.Left} />
      <span className="statement-node__dot" aria-hidden="true" />
      <span className="statement-node__label">{d.label}</span>
      {d.is_product && <span className="statement-node__badge">product</span>}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
