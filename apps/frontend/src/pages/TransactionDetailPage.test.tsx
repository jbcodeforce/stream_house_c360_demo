import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/transactions'
import { ApiError } from '../api/client'
import TransactionDetailPage from './TransactionDetailPage'

const txn = {
  transaction_id: 't1', account_id: 'a1', customer_id: 'c1', transaction_type: 'CREDIT',
  amount: 99.9, currency: 'USD', status: 'COMPLETED', reference_id: 'REF-1',
  transacted_at: '2024-01-01T00:00:00+00:00', posted_at: null, created_at: '',
}

afterEach(() => vi.restoreAllMocks())

function renderAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/transactions/${id}`]}>
      <Routes><Route path="/transactions/:id" element={<TransactionDetailPage />} /></Routes>
    </MemoryRouter>,
  )
}

describe('TransactionDetailPage', () => {
  it('renders transaction fields', async () => {
    vi.spyOn(api, 'getTransaction').mockResolvedValue(txn as never)
    renderAt('t1')
    expect(await screen.findByRole('heading', { name: /REF-1/ })).toBeInTheDocument()
    expect(screen.getByText('CREDIT')).toBeInTheDocument()
  })

  it('shows the error message on 404', async () => {
    vi.spyOn(api, 'getTransaction').mockRejectedValue(new ApiError(404, 'Transaction not found'))
    renderAt('nope')
    expect(await screen.findByText(/Transaction not found/)).toBeInTheDocument()
  })
})
