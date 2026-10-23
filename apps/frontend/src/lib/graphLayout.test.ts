import { describe, expect, it } from 'vitest'
import type { GraphEdge, GraphNode } from '../types/dataProduct'
import { layoutGraph } from './graphLayout'

describe('layoutGraph', () => {
  it('places a source upstream (smaller x) of the statement it feeds', () => {
    const nodes = [
      { id: 'topic_customers', type: 'source', label: '' },
      { id: 'src_customers', type: 'statement', label: '' },
    ] as GraphNode[]
    const edges: GraphEdge[] = [{ source: 'topic_customers', target: 'src_customers' }]
    const pos = Object.fromEntries(layoutGraph(nodes, edges).map((n) => [n.id, n.position]))
    expect(pos.topic_customers.x).toBeLessThan(pos.src_customers.x)
  })

  it('returns a position for every node', () => {
    const nodes = [
      { id: 'topic_customers', type: 'source', label: '' },
      { id: 'topic_accounts', type: 'source', label: '' },
      { id: 'src_customers', type: 'statement', label: '' },
      { id: 'src_accounts', type: 'statement', label: '' },
      { id: 'int_c360', type: 'statement', label: '' },
    ] as GraphNode[]
    const edges: GraphEdge[] = [
      { source: 'topic_customers', target: 'src_customers' },
      { source: 'topic_accounts', target: 'src_accounts' },
      { source: 'src_customers', target: 'int_c360' },
      { source: 'src_accounts', target: 'int_c360' },
    ]
    const result = layoutGraph(nodes, edges)
    expect(result).toHaveLength(nodes.length)
    for (const node of nodes) {
      const entry = result.find((n) => n.id === node.id)
      expect(entry).toBeDefined()
      expect(typeof entry?.position.x).toBe('number')
      expect(typeof entry?.position.y).toBe('number')
    }
  })
})
