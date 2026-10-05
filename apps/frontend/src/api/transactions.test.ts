import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import { createTransaction, getTransaction, listTransactions } from './transactions'

afterEach(() => vi.restoreAllMocks())

describe('transactions api', () => {
  it('listTransactions GETs /transactions', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue([])
    await listTransactions()
    expect(spy).toHaveBeenCalledWith('/transactions')
  })

  it('getTransaction GETs /transactions/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getTransaction('t1')
    expect(spy).toHaveBeenCalledWith('/transactions/t1')
  })

  it('createTransaction POSTs body to /transactions', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    const body = { account_id: 'a1', customer_id: 'c1', transaction_type: 'CREDIT', amount: 5 }
    await createTransaction(body)
    expect(spy).toHaveBeenCalledWith('/transactions', { method: 'POST', body: JSON.stringify(body) })
  })
})
