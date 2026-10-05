import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as txnApi from '../api/transactions'
import * as acctApi from '../api/accounts'
import * as custApi from '../api/customers'
import { ApiError } from '../api/client'
import TransactionsPage from './TransactionsPage'

const txn = {
  transaction_id: 't1', account_id: 'a1', customer_id: 'c1', transaction_type: 'CREDIT',
  amount: 99.9, currency: 'USD', status: 'COMPLETED', transacted_at: '2024-01-01T00:00:00+00:00',
  posted_at: null, created_at: '',
}
const acct = { account_id: 'a1', customer_id: 'c1', account_number: 'CHK-1', account_type: 'CHECKING', currency: 'USD', balance: 0, credit_limit: null, opened_date: '2020-01-01', closed_date: null, status: 'ACTIVE', created_at: '', updated_at: '' }
const cust = { customer_id: 'c1', first_name: 'Ada', last_name: 'Lovelace', email: 'a@x.io', country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '' }

afterEach(() => vi.restoreAllMocks())

describe('TransactionsPage', () => {
  it('renders rows with resolved account number and customer name', async () => {
    vi.spyOn(txnApi, 'listTransactions').mockResolvedValue([txn] as never)
    vi.spyOn(acctApi, 'listAccounts').mockResolvedValue([acct] as never)
    vi.spyOn(custApi, 'listCustomers').mockResolvedValue([cust] as never)
    render(<MemoryRouter><TransactionsPage /></MemoryRouter>)
    expect(await screen.findByText('CHK-1')).toBeInTheDocument()
    expect(screen.getByText('Ada Lovelace')).toBeInTheDocument()
    expect(screen.getByText('CREDIT')).toBeInTheDocument()
  })

  it('shows an error when the list request fails', async () => {
    vi.spyOn(txnApi, 'listTransactions').mockRejectedValue(new ApiError(500, 'boom'))
    vi.spyOn(acctApi, 'listAccounts').mockResolvedValue([] as never)
    vi.spyOn(custApi, 'listCustomers').mockResolvedValue([] as never)
    render(<MemoryRouter><TransactionsPage /></MemoryRouter>)
    expect(await screen.findByText(/boom/)).toBeInTheDocument()
  })
})
