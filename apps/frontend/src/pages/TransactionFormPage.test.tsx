import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as txnApi from '../api/transactions'
import * as acctApi from '../api/accounts'
import TransactionFormPage from './TransactionFormPage'

const acct = { account_id: 'a1', customer_id: 'c1', account_number: 'CHK-1', account_type: 'CHECKING', currency: 'USD', balance: 0, credit_limit: null, opened_date: '2020-01-01', closed_date: null, status: 'ACTIVE', created_at: '', updated_at: '' }

afterEach(() => vi.restoreAllMocks())

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/transactions/new']}>
      <Routes>
        <Route path="/transactions/new" element={<TransactionFormPage />} />
        <Route path="/transactions" element={<div>list</div>} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('TransactionFormPage', () => {
  it('validates required fields before submitting', async () => {
    vi.spyOn(acctApi, 'listAccounts').mockResolvedValue([acct] as never)
    const create = vi.spyOn(txnApi, 'createTransaction').mockResolvedValue({} as never)
    renderPage()
    await screen.findByRole('button', { name: /save/i })
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    expect(await screen.findByText(/account is required/i)).toBeInTheDocument()
    expect(create).not.toHaveBeenCalled()
  })

  it('creates a transaction, deriving customer_id from the selected account', async () => {
    vi.spyOn(acctApi, 'listAccounts').mockResolvedValue([acct] as never)
    const create = vi.spyOn(txnApi, 'createTransaction').mockResolvedValue({} as never)
    renderPage()
    await screen.findByRole('button', { name: /save/i })
    await userEvent.selectOptions(screen.getByLabelText(/account/i), 'a1')
    await userEvent.type(screen.getByLabelText(/amount/i), '42.50')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    await waitFor(() => expect(create).toHaveBeenCalledTimes(1))
    const payload = create.mock.calls[0][0]
    expect(payload).toMatchObject({ account_id: 'a1', customer_id: 'c1', amount: 42.5, transaction_type: 'CREDIT' })
  })
})
