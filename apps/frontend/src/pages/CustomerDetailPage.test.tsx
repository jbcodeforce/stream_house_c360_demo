import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/customers'
import { ApiError } from '../api/client'
import CustomerDetailPage from './CustomerDetailPage'

const customer = {
  customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
  phone: '555', segment: 'VIP', status: 'ACTIVE', country: 'US',
  customer_since: '2020-01-01', created_at: '', updated_at: '',
}

afterEach(() => vi.restoreAllMocks())

function renderAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/customers/${id}`]}>
      <Routes>
        <Route path="/customers/:id" element={<CustomerDetailPage />} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('CustomerDetailPage', () => {
  it('renders the customer fields', async () => {
    vi.spyOn(api, 'getCustomer').mockResolvedValue(customer as never)
    renderAt('1')
    expect(await screen.findByText('ada@x.io')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: /Ada Lovelace/ })).toBeInTheDocument()
  })

  it('shows the error message on 404', async () => {
    vi.spyOn(api, 'getCustomer').mockRejectedValue(new ApiError(404, 'Customer not found'))
    renderAt('nope')
    expect(await screen.findByText(/Customer not found/)).toBeInTheDocument()
  })
})
