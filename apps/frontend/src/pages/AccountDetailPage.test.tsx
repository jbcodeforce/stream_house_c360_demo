import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/accounts'
import { ApiError } from '../api/client'
import AccountDetailPage from './AccountDetailPage'

const account = {
  account_id: 'a1', customer_id: 'c1', account_number: 'ACC-1', account_type: 'CHECKING',
  currency: 'USD', balance: 100, credit_limit: null, opened_date: '2021-01-01',
  closed_date: null, status: 'ACTIVE', created_at: '', updated_at: '',
}

afterEach(() => vi.restoreAllMocks())

function renderAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/accounts/${id}`]}>
      <Routes><Route path="/accounts/:id" element={<AccountDetailPage />} /></Routes>
    </MemoryRouter>,
  )
}

describe('AccountDetailPage', () => {
  it('renders account fields', async () => {
    vi.spyOn(api, 'getAccount').mockResolvedValue(account as never)
    renderAt('a1')
    expect(await screen.findByRole('heading', { name: /ACC-1/ })).toBeInTheDocument()
    expect(screen.getByText('CHECKING')).toBeInTheDocument()
  })

  it('shows the error message on 404', async () => {
    vi.spyOn(api, 'getAccount').mockRejectedValue(new ApiError(404, 'Account not found'))
    renderAt('nope')
    expect(await screen.findByText(/Account not found/)).toBeInTheDocument()
  })
})
