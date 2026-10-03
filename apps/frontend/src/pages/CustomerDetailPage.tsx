import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ApiError } from '../api/client'
import { getCustomer } from '../api/customers'
import type { Customer } from '../types/customer'

const FIELDS: [keyof Customer, string][] = [
  ['first_name', 'First name'],
  ['last_name', 'Last name'],
  ['email', 'Email'],
  ['phone', 'Phone'],
  ['date_of_birth', 'Date of birth'],
  ['gender', 'Gender'],
  ['address_line1', 'Address line 1'],
  ['address_line2', 'Address line 2'],
  ['city', 'City'],
  ['state', 'State'],
  ['postal_code', 'Postal code'],
  ['country', 'Country'],
  ['customer_since', 'Customer since'],
  ['segment', 'Segment'],
  ['status', 'Status'],
]

export default function CustomerDetailPage() {
  const { id } = useParams<{ id: string }>()
  const [customer, setCustomer] = useState<Customer | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    setLoading(true)
    setError(null)
    getCustomer(id)
      .then(setCustomer)
      .catch((e) => setError(e instanceof ApiError ? e.message : 'Failed to load customer'))
      .finally(() => setLoading(false))
  }, [id])

  if (loading) return <div className="status-msg">Loading…</div>
  if (error) return <div className="status-msg status-msg--error">{error}</div>
  if (!customer) return null

  return (
    <div className="detail">
      <div className="page-header">
        <h1>{customer.first_name} {customer.last_name}</h1>
        <div style={{ display: 'flex', gap: 10 }}>
          <Link to={`/customers/${customer.customer_id}/edit`} className="btn">Edit</Link>
          <Link to="/customers" className="btn btn--secondary">Back</Link>
        </div>
      </div>
      <dl>
        {FIELDS.map(([key, label]) => (
          <div key={key} style={{ display: 'contents' }}>
            <dt>{label}</dt>
            <dd>{(customer[key] as string | null) ?? '—'}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}
