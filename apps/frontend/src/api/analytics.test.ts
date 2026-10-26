import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import { getC360Analytics, getCustomerProfile, searchCustomers } from './analytics'

afterEach(() => vi.restoreAllMocks())

describe('analytics api', () => {
  it('getC360Analytics GETs /analytics/c360-profile', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getC360Analytics()
    expect(spy).toHaveBeenCalledWith('/analytics/c360-profile')
  })

  it('searchCustomers GETs the search endpoint with an encoded name', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await searchCustomers('José Díaz')
    expect(spy).toHaveBeenCalledWith('/analytics/c360-profile/search?name=Jos%C3%A9%20D%C3%ADaz')
  })

  it('getCustomerProfile GETs the by-id endpoint', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getCustomerProfile('abc-123')
    expect(spy).toHaveBeenCalledWith('/analytics/c360-profile/customer/abc-123')
  })
})
