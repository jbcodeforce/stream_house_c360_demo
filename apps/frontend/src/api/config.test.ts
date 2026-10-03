import { afterEach, describe, expect, it, vi } from 'vitest'
import * as client from './client'
import { getConfig, updateConfig } from './config'

afterEach(() => vi.restoreAllMocks())

describe('config api', () => {
  it('getConfig GETs /config', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({ kafka_produce_enabled: false })
    await getConfig()
    expect(spy).toHaveBeenCalledWith('/config')
  })

  it('updateConfig PUTs the body to /config', async () => {
    const spy = vi.spyOn(client, 'request').mockResolvedValue({ kafka_produce_enabled: true })
    await updateConfig({ kafka_produce_enabled: true })
    expect(spy).toHaveBeenCalledWith('/config', {
      method: 'PUT',
      body: JSON.stringify({ kafka_produce_enabled: true }),
    })
  })
})
