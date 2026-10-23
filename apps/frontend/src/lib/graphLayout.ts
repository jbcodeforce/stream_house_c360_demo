import dagre from '@dagrejs/dagre'
import type { GraphEdge, GraphNode } from '../types/dataProduct'

const NODE_WIDTH = 180
const NODE_HEIGHT = 60

export interface LayoutPosition {
  id: string
  position: { x: number; y: number }
}

/**
 * Assigns left-to-right coordinates to graph nodes using dagre's layered
 * layout algorithm. Pure function: no React/DOM dependency.
 */
export function layoutGraph(nodes: GraphNode[], edges: GraphEdge[]): LayoutPosition[] {
  const graph = new dagre.graphlib.Graph()
  graph.setDefaultEdgeLabel(() => ({}))
  graph.setGraph({ rankdir: 'LR', nodesep: 40, ranksep: 80 })

  for (const node of nodes) {
    graph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT })
  }

  for (const edge of edges) {
    graph.setEdge(edge.source, edge.target)
  }

  dagre.layout(graph)

  return nodes.map((node) => {
    const { x, y } = graph.node(node.id)
    return { id: node.id, position: { x, y } }
  })
}
