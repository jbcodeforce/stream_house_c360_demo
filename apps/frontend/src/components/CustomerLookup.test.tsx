import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/analytics'
import { ApiError } from '../api/client'
import type { CustomerProfile, CustomerSearchResponse } from '../types/analytics'
import CustomerLookup from './CustomerLookup'

const thomas: CustomerProfile = {
  source_mode: 'mock',
  customer_id: 'id-thomas',
  first_name: 'Thomas',
  last_name: 'Brennan',
  customer_segment: 'SMB',
  generation_segment: 'Boomer+',
  gender: 'M',
  age_years: 56,
  city: 'Seattle',
  state: 'WA',
  country: 'US',
  customer_since: '2017-12-01',
  days_since_registration: 3251,
  missing_email_flag: 0,
  missing_phone_flag: 0,
  account_count: 6,
  total_balance: 318400,
  txn_count: 512,
  txn_total: 1024700,
}

function searchResult(matches: CustomerSearchResponse['matches']): CustomerSearchResponse {
  return { source_mode: 'mock', query: 'x', matches }
}

afterEach(() => vi.restoreAllMocks())

describe('CustomerLookup', () => {
  it('auto-loads the profile when exactly one customer matches', async () => {
    vi.spyOn(api, 'searchCustomers').mockResolvedValue(
      searchResult([
        { customer_id: 'id-thomas', first_name: 'Thomas', last_name: 'Brennan', customer_segment: 'SMB', city: 'Seattle', state: 'WA' },
      ]),
    )
    const profileSpy = vi.spyOn(api, 'getCustomerProfile').mockResolvedValue(thomas)

    render(<CustomerLookup />)
    await userEvent.type(screen.getByLabelText(/customer name/i), 'brennan')
    await userEvent.click(screen.getByRole('button', { name: /search/i }))

    await waitFor(() => expect(profileSpy).toHaveBeenCalledWith('id-thomas'))
    expect(await screen.findByText('Thomas Brennan')).toBeInTheDocument()
    expect(screen.getByText('$318,400')).toBeInTheDocument() // total balance
    expect(screen.getByText('512')).toBeInTheDocument() // txn count
  })

  it('shows a picker when multiple customers match and loads the chosen one', async () => {
    vi.spyOn(api, 'searchCustomers').mockResolvedValue(
      searchResult([
        { customer_id: 'id-a', first_name: 'Linda', last_name: 'Sharma', customer_segment: 'SMB', city: 'Austin', state: 'TX' },
        { customer_id: 'id-thomas', first_name: 'Thomas', last_name: 'Brennan', customer_segment: 'SMB', city: 'Seattle', state: 'WA' },
      ]),
    )
    const profileSpy = vi.spyOn(api, 'getCustomerProfile').mockResolvedValue(thomas)

    render(<CustomerLookup />)
    await userEvent.type(screen.getByLabelText(/customer name/i), 'a')
    await userEvent.click(screen.getByRole('button', { name: /search/i }))

    // Two candidates listed; profile not fetched until one is chosen.
    expect(await screen.findByText('Linda Sharma')).toBeInTheDocument()
    expect(profileSpy).not.toHaveBeenCalled()

    await userEvent.click(screen.getByRole('button', { name: /thomas brennan/i }))
    await waitFor(() => expect(profileSpy).toHaveBeenCalledWith('id-thomas'))
    expect(await screen.findByText('Boomer+')).toBeInTheDocument()
  })

  it('shows a no-match message when nothing is found', async () => {
    vi.spyOn(api, 'searchCustomers').mockResolvedValue(searchResult([]))
    render(<CustomerLookup />)
    await userEvent.type(screen.getByLabelText(/customer name/i), 'zzz')
    await userEvent.click(screen.getByRole('button', { name: /search/i }))
    expect(await screen.findByText(/no customers match/i)).toBeInTheDocument()
  })

  it('shows an error when the search request fails', async () => {
    vi.spyOn(api, 'searchCustomers').mockRejectedValue(new ApiError(500, 'search boom'))
    render(<CustomerLookup />)
    await userEvent.type(screen.getByLabelText(/customer name/i), 'x')
    await userEvent.click(screen.getByRole('button', { name: /search/i }))
    expect(await screen.findByText(/search boom/)).toBeInTheDocument()
  })
})
