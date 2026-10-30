import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import { getC360Analytics, getCustomerProfile, getCustomerProfileRtce } from './analytics'

afterEach(() => vi.restoreAllMocks())

describe('analytics api', () => {
  it('getC360Analytics GETs /analytics/c360-profile', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getC360Analytics()
    expect(spy).toHaveBeenCalledWith('/analytics/c360-profile')
  })

  it('getCustomerProfile GETs the by-id endpoint', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getCustomerProfile('abc-123')
    expect(spy).toHaveBeenCalledWith('/analytics/c360-profile/customer/abc-123')
  })

  it('getCustomerProfileRtce GETs the RTCE by-id endpoint', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({})
    await getCustomerProfileRtce('abc-123')
    expect(spy).toHaveBeenCalledWith('/analytics/c360-profile/rtce/customer/abc-123')
  })
})
