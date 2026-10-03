import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/config'
import { ApiError } from '../api/client'
import ConfigPage from './ConfigPage'

afterEach(() => vi.restoreAllMocks())

describe('ConfigPage', () => {
  it('renders the current config value', async () => {
    vi.spyOn(api, 'getConfig').mockResolvedValue({ kafka_produce_enabled: true })
    render(<ConfigPage />)
    const toggle = await screen.findByRole('checkbox')
    expect(toggle).toBeChecked()
  })

  it('auto-saves the new value when toggled', async () => {
    vi.spyOn(api, 'getConfig').mockResolvedValue({ kafka_produce_enabled: false })
    const update = vi.spyOn(api, 'updateConfig').mockResolvedValue({ kafka_produce_enabled: true })
    render(<ConfigPage />)
    const toggle = await screen.findByRole('checkbox')
    await userEvent.click(toggle)
    await waitFor(() =>
      expect(update).toHaveBeenCalledWith({ kafka_produce_enabled: true }),
    )
    expect(toggle).toBeChecked()
  })

  it('reverts the toggle and shows an error when save fails', async () => {
    vi.spyOn(api, 'getConfig').mockResolvedValue({ kafka_produce_enabled: false })
    vi.spyOn(api, 'updateConfig').mockRejectedValue(new ApiError(500, 'disk full'))
    render(<ConfigPage />)
    const toggle = await screen.findByRole('checkbox')
    await userEvent.click(toggle)
    expect(await screen.findByText(/disk full/)).toBeInTheDocument()
    await waitFor(() => expect(toggle).not.toBeChecked())
  })
})
