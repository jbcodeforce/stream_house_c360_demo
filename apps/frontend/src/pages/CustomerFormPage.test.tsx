import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/customers'
import CustomerFormPage from './CustomerFormPage'

afterEach(() => vi.restoreAllMocks())

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/customers/new" element={<CustomerFormPage />} />
        <Route path="/customers/:id/edit" element={<CustomerFormPage />} />
        <Route path="/customers" element={<div>list</div>} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('CustomerFormPage', () => {
  it('validates required fields before submitting (create mode)', async () => {
    const create = vi.spyOn(api, 'createCustomer').mockResolvedValue({} as never)
    renderAt('/customers/new')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    expect(await screen.findByText(/first name is required/i)).toBeInTheDocument()
    expect(create).not.toHaveBeenCalled()
  })

  it('creates a customer and sends empty optionals as null', async () => {
    const create = vi.spyOn(api, 'createCustomer').mockResolvedValue({} as never)
    renderAt('/customers/new')
    await userEvent.type(screen.getByLabelText(/first name/i), 'Ada')
    await userEvent.type(screen.getByLabelText(/last name/i), 'Lovelace')
    await userEvent.type(screen.getByLabelText(/email/i), 'ada@x.io')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    await waitFor(() => expect(create).toHaveBeenCalledTimes(1))
    const payload = create.mock.calls[0][0]
    expect(payload).toMatchObject({ first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io' })
    expect(payload.phone).toBeNull()
  })

  it('normalizes a datetime date_of_birth to yyyy-MM-dd for the date input', async () => {
    vi.spyOn(api, 'getCustomer').mockResolvedValue({
      customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
      date_of_birth: '1985-03-12T00:00:00+00:00',
      country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '',
    } as never)
    renderAt('/customers/1/edit')
    const dob = await screen.findByLabelText(/date of birth/i)
    expect(dob).toHaveValue('1985-03-12')
  })

  it('loads an existing customer and updates via PUT in edit mode', async () => {
    vi.spyOn(api, 'getCustomer').mockResolvedValue({
      customer_id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@x.io',
      phone: '555', country: 'US', customer_since: '2020-01-01', created_at: '', updated_at: '',
    } as never)
    const update = vi.spyOn(api, 'updateCustomer').mockResolvedValue({} as never)
    const create = vi.spyOn(api, 'createCustomer')
    renderAt('/customers/1/edit')
    expect(await screen.findByDisplayValue('Ada')).toBeInTheDocument()
    await userEvent.clear(screen.getByLabelText(/city/i))
    await userEvent.type(screen.getByLabelText(/city/i), 'London')
    await userEvent.click(screen.getByRole('button', { name: /save/i }))
    await waitFor(() => expect(update).toHaveBeenCalledWith('1', expect.objectContaining({ city: 'London' })))
    expect(create).not.toHaveBeenCalled()
  })
})
