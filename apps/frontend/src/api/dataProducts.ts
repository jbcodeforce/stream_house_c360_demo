import type { Cost, DataProduct, Graph, NodeMetrics, StatusResponse } from '../types/dataProduct'
import { request } from './client'

export function listDataProducts(): Promise<DataProduct[]> {
  return request<DataProduct[]>('/data-products')
}

export function getGraph(id: string): Promise<Graph> {
  return request<Graph>(`/data-products/${id}/graph`)
}

export function getStatus(id: string): Promise<StatusResponse> {
  return request<StatusResponse>(`/data-products/${id}/status`)
}

export function getNodeMetrics(id: string, nodeId: string): Promise<NodeMetrics> {
  return request<NodeMetrics>(`/data-products/${id}/nodes/${nodeId}/metrics`)
}

export function getCost(id: string): Promise<Cost> {
  return request<Cost>(`/data-products/${id}/cost`)
}
