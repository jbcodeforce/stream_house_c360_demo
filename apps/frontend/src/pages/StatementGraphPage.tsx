import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useParams } from 'react-router-dom'
import {
  Background,
  Controls,
  ReactFlow,
  type Edge,
  type Node,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { X } from 'lucide-react'
import { ApiError } from '../api/client'
import { getCost, getGraph, getNodeMetrics, getStatus, listDataProducts } from '../api/dataProducts'
import StatementNode from '../components/StatementNode'
import type { StatementNodeData } from '../components/StatementNode'
import { layoutGraph } from '../lib/graphLayout'
import { formatCurrency } from '../lib/format'
import type { Cost, Graph, NodeMetrics, NodeStatus, StatusResponse } from '../types/dataProduct'

const POLL_MS = 5000
// Cost data lags ~a day and barely moves, so poll it far less often than status.
const COST_POLL_MS = 300_000
const nodeTypes = { statement: StatementNode }

function fmt(value: number | string | null | undefined): string {
  return value === null || value === undefined ? '—' : String(value)
}

function fmtRate(value: number | null): string {
  return value === null ? '—' : `${value.toFixed(1)}/s`
}

function fmtUptime(seconds: number | null): string {
  if (seconds === null) return '—'
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  if (h > 0) return `${h}h ${m}m`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

export default function StatementGraphPage() {
  const { id } = useParams<{ id: string }>()
  const location = useLocation()

  // Name of the data product selected on the previous page. Passed via router
  // state for instant display; resolved from the list as a fallback on a direct
  // load / refresh (when navigation state is gone).
  const navName = (location.state as { name?: string } | null)?.name ?? null
  const [productName, setProductName] = useState<string | null>(navName)

  const [graph, setGraph] = useState<Graph | null>(null)
  const [positions, setPositions] = useState<Record<string, { x: number; y: number }>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Last good status is kept across failed polls so the graph never blanks.
  const [status, setStatus] = useState<StatusResponse | null>(null)
  const [stale, setStale] = useState(false)
  const [lastRefreshed, setLastRefreshed] = useState<number | null>(null)
  const [now, setNow] = useState(() => Date.now())

  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [metrics, setMetrics] = useState<NodeMetrics | null>(null)
  const [metricsLoading, setMetricsLoading] = useState(false)

  // Month-to-date pipeline cost. Null until loaded / when unavailable — the
  // banner renders only when available, never $0, and a failure never affects
  // the graph (cost is purely additive).
  const [cost, setCost] = useState<Cost | null>(null)

  const selectedIdRef = useRef<string | null>(null)
  useEffect(() => {
    selectedIdRef.current = selectedId
  }, [selectedId])

  // Guards against state updates after unmount (the page runs a 5s interval, so
  // a getNodeMetrics call can still be in flight when the user navigates away).
  const mountedRef = useRef(true)
  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
    }
  }, [])

  // Fallback: if we arrived without router state (direct load / refresh), look
  // the product name up by id so the title is still correct.
  useEffect(() => {
    if (!id || navName) return
    let active = true
    listDataProducts()
      .then((products) => {
        if (active) setProductName(products.find((p) => p.id === id)?.name ?? null)
      })
      .catch(() => {
        /* leave the title on its generic fallback */
      })
    return () => {
      active = false
    }
  }, [id, navName])

  // Load the graph once, then compute static left-to-right positions.
  useEffect(() => {
    if (!id) return
    let active = true
    setLoading(true)
    setError(null)
    getGraph(id)
      .then((g) => {
        if (!active) return
        setGraph(g)
        const laid = layoutGraph(g.nodes, g.edges)
        const map: Record<string, { x: number; y: number }> = {}
        for (const p of laid) map[p.id] = p.position
        setPositions(map)
      })
      .catch((e) => {
        if (!active) return
        setError(e instanceof ApiError ? e.message : 'Failed to load statement graph')
      })
      .finally(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
    }
  }, [id])

  const fetchMetrics = useCallback(
    (nodeId: string) => {
      if (!id) return
      setMetricsLoading(true)
      getNodeMetrics(id, nodeId)
        .then((m) => {
          if (mountedRef.current && selectedIdRef.current === nodeId) setMetrics(m)
        })
        .catch(() => {
          /* keep whatever metrics we last had; a transient failure is tolerated */
        })
        .finally(() => {
          if (mountedRef.current) setMetricsLoading(false)
        })
    },
    [id],
  )

  // Poll status (and the open node's metrics) every 5s. A rejected poll keeps
  // the last good status and raises a subtle "stale" flag instead of clearing.
  useEffect(() => {
    if (!id) return
    let active = true

    async function poll() {
      try {
        const next = await getStatus(id!)
        if (!active) return
        setStatus(next)
        setStale(false)
        setLastRefreshed(Date.now())
      } catch {
        if (!active) return
        setStale(true)
      }
      const open = selectedIdRef.current
      if (open) fetchMetrics(open)
    }

    void poll()
    const handle = setInterval(() => void poll(), POLL_MS)
    return () => {
      active = false
      clearInterval(handle)
    }
  }, [id, fetchMetrics])

  // Lightweight 1s ticker so "refreshed Ns ago" stays current between polls.
  useEffect(() => {
    const handle = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(handle)
  }, [])

  // Poll pipeline cost on a slow cadence (cost lags ~a day). A failure leaves the
  // banner hidden and never touches the graph.
  useEffect(() => {
    if (!id) return
    let active = true

    async function pollCost() {
      try {
        const next = await getCost(id!)
        if (active) setCost(next)
      } catch {
        if (active) setCost(null)
      }
    }

    void pollCost()
    const handle = setInterval(() => void pollCost(), COST_POLL_MS)
    return () => {
      active = false
      clearInterval(handle)
    }
  }, [id])

  const handleSelect = useCallback(
    (nodeId: string) => {
      setSelectedId(nodeId)
      setMetrics(null)
      fetchMetrics(nodeId)
    },
    [fetchMetrics],
  )

  const statusByNode = useMemo(() => {
    const map = new Map<string, NodeStatus>()
    for (const s of status?.statements ?? []) map.set(s.node_id, s)
    return map
  }, [status])

  const rfNodes = useMemo<Node[]>(() => {
    if (!graph) return []
    return graph.nodes.map((n) => {
      const s = statusByNode.get(n.id)
      const data: StatementNodeData = {
        label: n.label,
        nodeType: n.type,
        phase: s?.phase ?? 'UNKNOWN',
        lagging: s?.lagging ?? false,
        is_product: n.is_product,
        selected: n.id === selectedId,
      }
      return {
        id: n.id,
        type: 'statement',
        position: positions[n.id] ?? { x: 0, y: 0 },
        data,
      }
    })
  }, [graph, positions, statusByNode, selectedId])

  const rfEdges = useMemo<Edge[]>(() => {
    if (!graph) return []
    return graph.edges.map((e, i) => ({
      id: `e-${i}-${e.source}-${e.target}`,
      source: e.source,
      target: e.target,
      animated: true,
    }))
  }, [graph])

  if (loading) return <div className="status-msg">Loading…</div>
  if (error) return <div className="status-msg status-msg--error">{error}</div>
  if (!graph) return null

  const rollup = status?.rollup
  const refreshedAgo =
    lastRefreshed === null ? null : Math.max(0, Math.floor((now - lastRefreshed) / 1000))

  return (
    <div className="graph-page">
      <div className="page-header">
        <div>
          <h1>{productName ?? 'Statement Graph'}</h1>
          <p className="page-subtitle">Statement graph</p>
        </div>
        {cost?.available && (
          <div className="cost-banner" title={cost.pool_ids.join(', ')}>
            <span className="cost-banner__amount">{formatCurrency(cost.amount)}</span>
            <span className="cost-banner__label">this month</span>
            {cost.cfu != null && (
              <span className="cost-banner__cfu">· {Math.round(cost.cfu)} CFU</span>
            )}
          </div>
        )}
        <Link to="/data-products" className="btn btn--secondary">
          Back
        </Link>
      </div>

      <div className={`health-bar${stale ? ' health-bar--stale' : ''}`}>
        {rollup ? (
          <>
            <span className="health-bar__count">
              {`${rollup.running} / ${rollup.total} RUNNING`}
            </span>
            {rollup.lagging > 0 && (
              <span className="health-pill health-pill--lag">{rollup.lagging} with lag</span>
            )}
          </>
        ) : (
          <span className="health-bar__count">Waiting for status…</span>
        )}
        <span className="health-bar__refreshed">
          {refreshedAgo === null ? 'never refreshed' : `refreshed ${refreshedAgo}s ago`}
          {stale && ' · reconnecting'}
        </span>
      </div>

      <div className="graph-page__body">
        <div className="graph-canvas">
          <ReactFlow
            nodes={rfNodes}
            edges={rfEdges}
            nodeTypes={nodeTypes}
            onNodeClick={(_, node) => handleSelect(node.id)}
            fitView
            proOptions={{ hideAttribution: true }}
          >
            <Background />
            <Controls />
          </ReactFlow>
        </div>

        {selectedId && (
          <aside className="metrics-panel">
            <div className="metrics-panel__head">
              <h2 className="metrics-panel__title">{selectedId}</h2>
              <button
                type="button"
                className="icon-btn"
                aria-label="Close metrics"
                onClick={() => {
                  setSelectedId(null)
                  setMetrics(null)
                }}
              >
                <X size={16} />
              </button>
            </div>

            {metricsLoading && !metrics && <div className="status-msg">Loading metrics…</div>}

            {metrics && (
              <>
                <dl className="metric-rows">
                  <div className="metric-row">
                    <dt>Status</dt>
                    <dd>{fmt(metrics.phase)}</dd>
                  </div>
                  <div className="metric-row">
                    <dt>Uptime</dt>
                    <dd>{fmtUptime(metrics.uptime_seconds)}</dd>
                  </div>
                  <div className="metric-row">
                    <dt>Messages in</dt>
                    <dd>{fmtRate(metrics.messages_in_per_sec)}</dd>
                  </div>
                  <div className="metric-row">
                    <dt>Messages out</dt>
                    <dd>{fmtRate(metrics.messages_out_per_sec)}</dd>
                  </div>
                  <div className="metric-row">
                    <dt>Pending records</dt>
                    <dd>{fmt(metrics.pending_records)}</dd>
                  </div>
                  <div className="metric-row">
                    <dt>Compute pool</dt>
                    <dd>{metrics.compute_pool_cfu === null ? '—' : `${metrics.compute_pool_cfu} CFU`}</dd>
                  </div>
                </dl>
                <div className="metric-sql">
                  <div className="metric-sql__label">SQL</div>
                  <pre className="metric-sql__code">{metrics.sql ?? '—'}</pre>
                </div>
              </>
            )}
          </aside>
        )}
      </div>
    </div>
  )
}
