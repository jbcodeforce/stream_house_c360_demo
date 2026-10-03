import type { AppConfig } from '../types/config'
import { request } from './client'

export function getConfig(): Promise<AppConfig> {
  return request<AppConfig>('/config')
}

export function updateConfig(data: AppConfig): Promise<AppConfig> {
  return request<AppConfig>('/config', {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}
