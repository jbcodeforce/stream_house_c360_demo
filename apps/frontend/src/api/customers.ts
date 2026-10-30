import type { Customer, CustomerCreate, CustomerUpdate } from '../types/customer'
import { request } from './client'

export function listCustomers(): Promise<Customer[]> {
  return request<Customer[]>('/customers')
}

// Resolve a name to customers via the operational store (system of record for
// identity). The C360 dashboard calls this first, then fetches the analytics
// profile by customer_id.
export function searchCustomersByName(name: string): Promise<Customer[]> {
  return request<Customer[]>(`/customers?name=${encodeURIComponent(name)}`)
}

export function getCustomer(id: string): Promise<Customer> {
  return request<Customer>(`/customers/${id}`)
}

export function createCustomer(data: CustomerCreate): Promise<Customer> {
  return request<Customer>('/customers', {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

export function updateCustomer(id: string, data: CustomerUpdate): Promise<Customer> {
  return request<Customer>(`/customers/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

export function deleteCustomer(id: string): Promise<void> {
  return request<void>(`/customers/${id}`, { method: 'DELETE' })
}
