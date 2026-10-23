import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/dataProducts'
import { ApiError } from '../api/client'
import DataProductsPage from './DataProductsPage'

afterEach(() => vi.restoreAllMocks())

function renderPage() {
  return render(
    <MemoryRouter>
      <DataProductsPage />
    </MemoryRouter>,
  )
}

describe('DataProductsPage', () => {
  it('lists data products with their statement counts', async () => {
    vi.spyOn(api, 'listDataProducts').mockResolvedValue([
      { id: 'c360', name: 'Customer 360', description: 'x', statement_count: 8 },
    ])
    renderPage()
    expect(await screen.findByText('Customer 360')).toBeInTheDocument()
    expect(screen.getByText('8')).toBeInTheDocument()
  })

  it('links each row to the data product detail route', async () => {
    vi.spyOn(api, 'listDataProducts').mockResolvedValue([
      { id: 'c360', name: 'Customer 360', description: 'x', statement_count: 8 },
    ])
    renderPage()
    const link = await screen.findByRole('link', { name: /customer 360/i })
    expect(link).toHaveAttribute('href', '/data-products/c360')
  })

  it('shows an error when the list request fails', async () => {
    vi.spyOn(api, 'listDataProducts').mockRejectedValue(new ApiError(500, 'boom'))
    renderPage()
    expect(await screen.findByText(/boom/)).toBeInTheDocument()
  })
})
