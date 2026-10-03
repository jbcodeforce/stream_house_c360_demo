import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/customers'
import { ApiError } from '../api/client'
import CustomersPage from './CustomersPage'

const sample = [
  {
    customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
    phone: '555', segment: 'VIP', status: 'ACTIVE',
    country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '',
  },
]

afterEach(() => vi.restoreAllMocks())

function renderPage() {
  return render(
    <MemoryRouter>
      <CustomersPage />
    </MemoryRouter>,
  )
}

describe('CustomersPage', () => {
  it('renders a row per customer from the API', async () => {
    vi.spyOn(api, 'listCustomers').mockResolvedValue(sample as never)
    renderPage()
    expect(await screen.findByText('Ada Lovelace')).toBeInTheDocument()
    expect(screen.getByText('ada@x.io')).toBeInTheDocument()
  })

  it('shows an error message when the list request fails', async () => {
    vi.spyOn(api, 'listCustomers').mockRejectedValue(new ApiError(500, 'boom'))
    renderPage()
    expect(await screen.findByText(/boom/)).toBeInTheDocument()
  })

  it('deletes a customer after confirmation and refetches', async () => {
    const list = vi.spyOn(api, 'listCustomers')
    list.mockResolvedValueOnce(sample as never).mockResolvedValueOnce([] as never)
    const del = vi.spyOn(api, 'deleteCustomer').mockResolvedValue(undefined)
    renderPage()
    await screen.findByText('Ada Lovelace')

    const row = screen.getByText('Ada Lovelace').closest('tr')!
    await userEvent.click(within(row).getByRole('button', { name: /delete/i }))
    const dialog = screen.getByRole('dialog')
    await userEvent.click(within(dialog).getByRole('button', { name: /^delete$/i }))

    await waitFor(() => expect(del).toHaveBeenCalledWith('1'))
    await waitFor(() => expect(screen.queryByText('Ada Lovelace')).not.toBeInTheDocument())
  })
})
