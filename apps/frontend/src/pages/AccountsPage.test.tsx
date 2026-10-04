import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as accountsApi from '../api/accounts'
import * as customersApi from '../api/customers'
import { ApiError } from '../api/client'
import AccountsPage from './AccountsPage'

const account = {
  account_id: 'a1', customer_id: 'c1', account_number: 'ACC-1', account_type: 'CHECKING',
  currency: 'USD', balance: 100, credit_limit: null, opened_date: '2021-01-01',
  closed_date: null, status: 'ACTIVE', created_at: '', updated_at: '',
}
const customer = {
  customer_id: 'c1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
  country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '',
}

afterEach(() => vi.restoreAllMocks())

function renderPage() {
  return render(<MemoryRouter><AccountsPage /></MemoryRouter>)
}

describe('AccountsPage', () => {
  it('renders a row per account with the owner name resolved', async () => {
    vi.spyOn(accountsApi, 'listAccounts').mockResolvedValue([account] as never)
    vi.spyOn(customersApi, 'listCustomers').mockResolvedValue([customer] as never)
    renderPage()
    expect(await screen.findByText('ACC-1')).toBeInTheDocument()
    expect(screen.getByText('Ada Lovelace')).toBeInTheDocument()
  })

  it('shows an error when the list request fails', async () => {
    vi.spyOn(accountsApi, 'listAccounts').mockRejectedValue(new ApiError(500, 'boom'))
    vi.spyOn(customersApi, 'listCustomers').mockResolvedValue([] as never)
    renderPage()
    expect(await screen.findByText(/boom/)).toBeInTheDocument()
  })

  it('deletes an account after confirmation and refetches', async () => {
    const list = vi.spyOn(accountsApi, 'listAccounts')
    list.mockResolvedValueOnce([account] as never).mockResolvedValueOnce([] as never)
    vi.spyOn(customersApi, 'listCustomers').mockResolvedValue([customer] as never)
    const del = vi.spyOn(accountsApi, 'deleteAccount').mockResolvedValue(undefined)
    renderPage()
    await screen.findByText('ACC-1')
    const row = screen.getByText('ACC-1').closest('tr')!
    await userEvent.click(within(row).getByRole('button', { name: /delete/i }))
    const dialog = screen.getByRole('dialog')
    await userEvent.click(within(dialog).getByRole('button', { name: /^delete$/i }))
    await waitFor(() => expect(del).toHaveBeenCalledWith('a1'))
    await waitFor(() => expect(screen.queryByText('ACC-1')).not.toBeInTheDocument())
  })
})
