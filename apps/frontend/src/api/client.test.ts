import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, request } from './client'

afterEach(() => vi.restoreAllMocks())

function mockFetch(resp: Partial<Response> & { json?: () => Promise<unknown> }) {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(resp as Response))
}

describe('request', () => {
  it('returns parsed JSON on 200', async () => {
    mockFetch({ ok: true, status: 200, json: async () => ({ a: 1 }) })
    const data = await request<{ a: number }>('/x')
    expect(data).toEqual({ a: 1 })
  })

  it('returns undefined on 204 without parsing body', async () => {
    const json = vi.fn()
    mockFetch({ ok: true, status: 204, json })
    const data = await request<void>('/x', { method: 'DELETE' })
    expect(data).toBeUndefined()
    expect(json).not.toHaveBeenCalled()
  })

  it('throws ApiError with status and server detail on non-2xx', async () => {
    mockFetch({ ok: false, status: 404, json: async () => ({ detail: 'Customer not found' }) })
    await expect(request('/x')).rejects.toMatchObject({
      name: 'ApiError',
      status: 404,
      message: 'Customer not found',
    })
    expect((await request('/x').catch((e) => e)) instanceof ApiError).toBe(true)
  })
})
