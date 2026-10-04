import type { Account, AccountCreate, AccountUpdate } from '../types/account'
import { request } from './client'

export function listAccounts(): Promise<Account[]> {
  return request<Account[]>('/accounts')
}

export function getAccount(id: string): Promise<Account> {
  return request<Account>(`/accounts/${id}`)
}

export function createAccount(data: AccountCreate): Promise<Account> {
  return request<Account>('/accounts', { method: 'POST', body: JSON.stringify(data) })
}

export function updateAccount(id: string, data: AccountUpdate): Promise<Account> {
  return request<Account>(`/accounts/${id}`, { method: 'PUT', body: JSON.stringify(data) })
}

export function deleteAccount(id: string): Promise<void> {
  return request<void>(`/accounts/${id}`, { method: 'DELETE' })
}
