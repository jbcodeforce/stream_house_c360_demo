import type { Transaction, TransactionCreate } from '../types/transaction'
import { request } from './client'

export function listTransactions(): Promise<Transaction[]> {
  return request<Transaction[]>('/transactions')
}

export function getTransaction(id: string): Promise<Transaction> {
  return request<Transaction>(`/transactions/${id}`)
}

export function createTransaction(data: TransactionCreate): Promise<Transaction> {
  return request<Transaction>('/transactions', { method: 'POST', body: JSON.stringify(data) })
}
