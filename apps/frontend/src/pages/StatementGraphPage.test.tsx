import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/dataProducts'
import type { NodeMetrics, StatusResponse } from '../types/dataProduct'
import StatementGraphPage from './StatementGraphPage'

// React Flow does not lay out or measure under jsdom. Replace it with a light
// stub that renders one button per node (showing its label) and forwards clicks
// through onNodeClick(event, node) so tests can select a node by its id.
vi.mock('@xyflow/react', () => ({
  ReactFlow: ({
    nodes,
    onNodeClick,
  }: {
    nodes: Array<{ id: string; data?: { label?: string } }>
    onNodeClick?: (e: unknown, n: { id: string }) => void
  }) => (
    <div data-testid="rf">
      {nodes.map((n) => (
        <button
          key={n.id}
          data-testid={`rf-node-${n.id}`}
          onClick={(e) => onNodeClick?.(e, n)}
        >
          {String(n.data?.label ?? n.id)}
        </button>
      ))}
    </div>
  ),
  Background: () => null,
  Controls: () => null,
  Handle: () => null,
  Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
}))

const sampleMetrics: NodeMetrics = {
  node_id: 'src_customers',
  phase: 'RUNNING',
  uptime_seconds: 3600,
  messages_in_per_sec: 12.5,
  messages_out_per_sec: 10,
  pending_records: 42,
  compute_pool_cfu: 5,
  sql: 'SELECT * FROM customers',
}

function makeStatus({
  running,
  total,
  lagging,
}: {
  running: number
  total: number
  lagging: number
}): StatusResponse {
  // Key the first status to the graph's real statement node (dim_customers) so
  // the integration test exercises a realistic status->node mapping, then pad to
  // reach `total` for the rollup counts.
  const ids = [
    'dim_customers',
    ...Array.from({ length: Math.max(0, total - 1) }, (_, i) => `stmt_${i}`),
  ]
  const statements = ids.map((node_id, i) => ({
    node_id,
    statement_name: node_id,
    phase: (i < running ? 'RUNNING' : 'FAILED') as StatusResponse['statements'][number]['phase'],
    lagging: i < lagging,
  }))
  return { statements, rollup: { running, total, lagging } }
}

const goodStatus = makeStatus({ running: 8, total: 8, lagging: 0 })

function mockGraph() {
  return vi.spyOn(api, 'getGraph').mockResolvedValue({
    nodes: [
      {
        id: 'src_customers',
        type: 'source',
        label: 'customers',
        statement_name: null,
        topic: 'customers',
        is_product: false,
      },
      {
        id: 'dim_customers',
        type: 'statement',
        label: 'dim_customers',
        statement_name: 'dim_customers',
        topic: 'dim_customers',
        is_product: true,
      },
    ],
    edges: [{ source: 'src_customers', target: 'dim_customers' }],
  })
}

function mockStatus(rollup = { running: 8, total: 8, lagging: 0 }) {
  return vi.spyOn(api, 'getStatus').mockResolvedValue(makeStatus(rollup))
}

function renderGraphPage() {
  return render(
    <MemoryRouter initialEntries={['/data-products/c360']}>
      <Routes>
        <Route path="/data-products/:id" element={<StatementGraphPage />} />
      </Routes>
    </MemoryRouter>,
  )
}

async function selectNode(nodeId: string) {
  const user = userEvent.setup()
  const btn = await screen.findByTestId(`rf-node-${nodeId}`)
  await user.click(btn)
}

// Jump the 5s poll interval forward and flush the resulting promises.
async function advancePollInterval() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5000)
  })
}

beforeEach(() => {
  // shouldAdvanceTime lets findByText / userEvent make real progress while we
  // still control the 5s poll interval with advanceTimersByTime.
  vi.useFakeTimers({ shouldAdvanceTime: true })
  mockGraph()
  mockStatus()
  vi.spyOn(api, 'getNodeMetrics').mockResolvedValue(sampleMetrics)
})

afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

describe('StatementGraphPage', () => {
  it('shows the health rollup from status', async () => {
    mockGraph()
    mockStatus({ running: 7, total: 8, lagging: 1 })
    renderGraphPage()
    expect(await screen.findByText(/7 \/ 8 RUNNING/)).toBeInTheDocument()
    expect(screen.getByText(/1 with lag/)).toBeInTheDocument()
  })

  it('opens the metrics panel when a node is selected', async () => {
    mockGraph()
    mockStatus()
    vi.spyOn(api, 'getNodeMetrics').mockResolvedValue(sampleMetrics)
    renderGraphPage()
    await selectNode('src_customers')
    expect(await screen.findByText(/Pending records/)).toBeInTheDocument()
  })

  it('renders a missing metric as an em dash', async () => {
    vi.spyOn(api, 'getNodeMetrics').mockResolvedValue({ ...sampleMetrics, messages_in_per_sec: null })
    renderGraphPage()
    await selectNode('src_customers')
    expect(await screen.findByText('—')).toBeInTheDocument()
  })

  it('keeps the last good status when a poll fails', async () => {
    vi.spyOn(api, 'getStatus')
      .mockResolvedValueOnce(goodStatus)
      .mockRejectedValueOnce(new Error('blip'))
    renderGraphPage()
    expect(await screen.findByText(/8 \/ 8 RUNNING/)).toBeInTheDocument()
    await advancePollInterval()
    expect(screen.getByText(/8 \/ 8 RUNNING/)).toBeInTheDocument() // unchanged, not blanked
  })
})
