import type { C360Analytics, CustomerProfile, CustomerSearchResponse } from '../types/analytics'
import { request } from './client'

export function getC360Analytics(): Promise<C360Analytics> {
  return request<C360Analytics>('/analytics/c360-profile')
}

export function searchCustomers(name: string): Promise<CustomerSearchResponse> {
  return request<CustomerSearchResponse>(
    `/analytics/c360-profile/search?name=${encodeURIComponent(name)}`,
  )
}

export function getCustomerProfile(customerId: string): Promise<CustomerProfile> {
  return request<CustomerProfile>(
    `/analytics/c360-profile/customer/${encodeURIComponent(customerId)}`,
  )
}
