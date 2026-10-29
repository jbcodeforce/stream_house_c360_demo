import type { DataAnalytics, CustomerProfile, CustomerSearchResponse } from '../types/analytics'
import { request } from './client'

export function getC360Analytics(): Promise<DataAnalytics> {
  return request<DataAnalytics>('/analytics/c360-profile')
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

// RTCE / Lightning Query API variants (current-state point lookups).
export function searchCustomersRtce(name: string): Promise<CustomerSearchResponse> {
  return request<CustomerSearchResponse>(
    `/analytics/c360-profile/rtce/search?name=${encodeURIComponent(name)}`,
  )
}

export function getCustomerProfileRtce(customerId: string): Promise<CustomerProfile> {
  return request<CustomerProfile>(
    `/analytics/c360-profile/rtce/customer/${encodeURIComponent(customerId)}`,
  )
}
