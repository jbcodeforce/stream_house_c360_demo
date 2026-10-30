import type { DataAnalytics, CustomerProfile } from '../types/analytics'
import { request } from './client'

export function getC360Analytics(): Promise<DataAnalytics> {
  return request<DataAnalytics>('/analytics/c360-profile')
}

export function getCustomerProfile(customerId: string): Promise<CustomerProfile> {
  return request<CustomerProfile>(
    `/analytics/c360-profile/customer/${encodeURIComponent(customerId)}`,
  )
}

// RTCE / Lightning Query API variant (current-state point lookup).
export function getCustomerProfileRtce(customerId: string): Promise<CustomerProfile> {
  return request<CustomerProfile>(
    `/analytics/c360-profile/rtce/customer/${encodeURIComponent(customerId)}`,
  )
}
