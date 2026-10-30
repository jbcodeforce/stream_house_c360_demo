import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import {
  createCustomer,
  deleteCustomer,
  getCustomer,
  listCustomers,
  searchCustomersByName,
  updateCustomer,
} from './customers'

afterEach(() => vi.restoreAllMocks())

describe('customers api', () => {
  it('listCustomers GETs /customers', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue([])
    await listCustomers()
    expect(spy).toHaveBeenCalledWith('/customers')
  })

  it('searchCustomersByName GETs /customers with an encoded name filter', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue([])
    await searchCustomersByName('José Díaz')
    expect(spy).toHaveBeenCalledWith('/customers?name=Jos%C3%A9%20D%C3%ADaz')
  })

  it('getCustomer GETs /customers/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getCustomer('abc')
    expect(spy).toHaveBeenCalledWith('/customers/abc')
  })

  it('createCustomer POSTs body to /customers', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    const body = { first_name: 'A', last_name: 'B', email: 'a@b.c' }
    await createCustomer(body)
    expect(spy).toHaveBeenCalledWith('/customers', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  })

  it('updateCustomer PUTs body to /customers/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await updateCustomer('abc', { city: 'NYC' })
    expect(spy).toHaveBeenCalledWith('/customers/abc', {
      method: 'PUT',
      body: JSON.stringify({ city: 'NYC' }),
    })
  })

  it('deleteCustomer DELETEs /customers/:id', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue(undefined)
    await deleteCustomer('abc')
    expect(spy).toHaveBeenCalledWith('/customers/abc', { method: 'DELETE' })
  })
})
