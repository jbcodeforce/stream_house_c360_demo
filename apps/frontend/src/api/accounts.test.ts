import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import {
  createAccount,
  deleteAccount,
  getAccount,
  listAccounts,
  updateAccount,
} from './accounts'

afterEach(() => vi.restoreAllMocks())

describe('accounts api', () => {
  it('listAccounts GETs /accounts', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue([])
    await listAccounts()
    expect(spy).toHaveBeenCalledWith('/accounts')
  })

  it('getAccount GETs /accounts/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getAccount('a1')
    expect(spy).toHaveBeenCalledWith('/accounts/a1')
  })

  it('createAccount POSTs body to /accounts', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    const body = { customer_id: 'c1', account_number: 'ACC-1', account_type: 'CHECKING' }
    await createAccount(body)
    expect(spy).toHaveBeenCalledWith('/accounts', { method: 'POST', body: JSON.stringify(body) })
  })

  it('updateAccount PUTs body to /accounts/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await updateAccount('a1', { status: 'CLOSED' })
    expect(spy).toHaveBeenCalledWith('/accounts/a1', {
      method: 'PUT', body: JSON.stringify({ status: 'CLOSED' }),
    })
  })

  it('deleteAccount DELETEs /accounts/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue(undefined)
    await deleteAccount('a1')
    expect(spy).toHaveBeenCalledWith('/accounts/a1', { method: 'DELETE' })
  })
})
