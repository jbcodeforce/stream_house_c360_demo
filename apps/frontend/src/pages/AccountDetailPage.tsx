import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ApiError } from '../api/client'
import { getAccount } from '../api/accounts'
import type { Account } from '../types/account'

const FIELDS: [keyof Account, string][] = [
  ['account_number', 'Account number'],
  ['account_type', 'Account type'],
  ['customer_id', 'Customer ID'],
  ['currency', 'Currency'],
  ['balance', 'Balance'],
  ['credit_limit', 'Credit limit'],
  ['opened_date', 'Opened date'],
  ['closed_date', 'Closed date'],
  ['status', 'Status'],
]

export default function AccountDetailPage() {
  const { id } = useParams<{ id: string }>()
  const [account, setAccount] = useState<Account | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    setLoading(true)
    setError(null)
    getAccount(id)
      .then(setAccount)
      .catch((e) => setError(e instanceof ApiError ? e.message : 'Failed to load account'))
      .finally(() => setLoading(false))
  }, [id])

  if (loading) return <div className="status-msg">Loading…</div>
  if (error) return <div className="status-msg status-msg--error">{error}</div>
  if (!account) return null

  return (
    <div className="detail">
      <div className="page-header">
        <h1>Account {account.account_number}</h1>
        <div style={{ display: 'flex', gap: 10 }}>
          <Link to={`/accounts/${account.account_id}/edit`} className="btn">Edit</Link>
          <Link to="/accounts" className="btn btn--secondary">Back</Link>
        </div>
      </div>
      <dl>
        {FIELDS.map(([key, label]) => (
          <div key={key} style={{ display: 'contents' }}>
            <dt>{label}</dt>
            <dd>{(account[key] as string | number | null) ?? '—'}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}
