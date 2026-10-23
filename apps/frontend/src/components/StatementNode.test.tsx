import { render } from '@testing-library/react'
import type { NodeProps } from '@xyflow/react'
import { describe, expect, it, vi } from 'vitest'
import type { Phase } from '../types/dataProduct'
import StatementNode, { type StatementNodeData } from './StatementNode'

// Handle requires a React Flow store/provider; stub it so the node renders bare.
vi.mock('@xyflow/react', () => ({
  Handle: () => null,
  Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
}))

function renderNode(overrides: Partial<StatementNodeData> = {}): HTMLElement {
  const data: StatementNodeData = {
    label: 'dim_customers',
    nodeType: 'statement',
    phase: 'RUNNING',
    lagging: false,
    is_product: false,
    selected: false,
    ...overrides,
  }
  const props = { data } as unknown as NodeProps
  const { container } = render(<StatementNode {...props} />)
  return container.querySelector('.statement-node') as HTMLElement
}

describe('StatementNode statusClass', () => {
  it('colors a RUNNING statement green', () => {
    expect(renderNode({ phase: 'RUNNING', lagging: false }).className).toContain(
      'statement-node--running',
    )
  })

  it('colors a lagging RUNNING statement amber', () => {
    const cls = renderNode({ phase: 'RUNNING', lagging: true }).className
    expect(cls).toContain('statement-node--lagging')
    expect(cls).not.toContain('statement-node--running')
  })

  it('colors a DEGRADED statement amber', () => {
    expect(renderNode({ phase: 'DEGRADED' }).className).toContain('statement-node--lagging')
  })

  it('colors FAILED and STOPPED statements red', () => {
    for (const phase of ['FAILED', 'STOPPED'] as Phase[]) {
      expect(renderNode({ phase }).className).toContain('statement-node--failed')
    }
  })

  it('colors UNKNOWN and PENDING statements grey', () => {
    for (const phase of ['UNKNOWN', 'PENDING'] as Phase[]) {
      expect(renderNode({ phase }).className).toContain('statement-node--unknown')
    }
  })

  it('renders source nodes neutral regardless of phase', () => {
    const cls = renderNode({ nodeType: 'source', phase: 'RUNNING' }).className
    expect(cls).toContain('statement-node--neutral')
    expect(cls).not.toContain('statement-node--running')
  })

  it('adds the product accent class when is_product is set', () => {
    expect(renderNode({ is_product: true }).className).toContain('statement-node--product')
  })
})
