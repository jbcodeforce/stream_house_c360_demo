import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import { getGraph, getNodeMetrics, getStatus, listDataProducts } from './dataProducts'

afterEach(() => vi.restoreAllMocks())

describe('dataProducts api', () => {
  it('listDataProducts GETs /data-products', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue([])
    await listDataProducts()
    expect(spy).toHaveBeenCalledWith('/data-products')
  })

  it('getGraph GETs /data-products/:id/graph', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getGraph('c360')
    expect(spy).toHaveBeenCalledWith('/data-products/c360/graph')
  })

  it('getStatus GETs /data-products/:id/status', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getStatus('c360')
    expect(spy).toHaveBeenCalledWith('/data-products/c360/status')
  })

  it('getNodeMetrics GETs /data-products/:id/nodes/:nodeId/metrics', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getNodeMetrics('c360', 'src_customers')
    expect(spy).toHaveBeenCalledWith('/data-products/c360/nodes/src_customers/metrics')
  })
})
