import { useEffect, useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { ApiError } from '../api/client'
import { createTransaction } from '../api/transactions'
import { listAccounts } from '../api/accounts'
import type { Account } from '../types/account'
import type { TransactionCreate } from '../types/transaction'

const TYPES = ['CREDIT', 'DEBIT', 'TRANSFER', 'FEE', 'INTEREST']
const CHANNELS = ['ONLINE', 'ATM', 'POS', 'MOBILE', 'BRANCH']
const STATUSES = ['COMPLETED', 'PENDING', 'FAILED']

export default function TransactionFormPage() {
  const navigate = useNavigate()
  const [accounts, setAccounts] = useState<Account[]>([])
  const [accountId, setAccountId] = useState('')
  const [transactionType, setTransactionType] = useState('CREDIT')
  const [amount, setAmount] = useState('')
  const [currency, setCurrency] = useState('USD')
  const [channel, setChannel] = useState('ONLINE')
  const [description, setDescription] = useState('')
  const [status, setStatus] = useState('COMPLETED')
  const [errors, setErrors] = useState<{ accountId?: string; amount?: string }>({})
  const [submitError, setSubmitError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    listAccounts()
      .then(setAccounts)
      .catch((e) => setSubmitError(e instanceof ApiError ? e.message : 'Failed to load accounts'))
      .finally(() => setLoading(false))
  }, [])

  function validate(): boolean {
    const next: typeof errors = {}
    if (!accountId) next.accountId = 'Account is required'
    if (amount.trim() === '' || Number.isNaN(Number(amount))) next.amount = 'A valid amount is required'
    setErrors(next)
    return Object.keys(next).length === 0
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setSubmitError(null)
    if (!validate()) return
    const account = accounts.find((a) => a.account_id === accountId)
    if (!account) {
      setErrors({ accountId: 'Account is required' })
      return
    }
    const payload: TransactionCreate = {
      account_id: account.account_id,
      customer_id: account.customer_id, // derived from the selected account
      transaction_type: transactionType,
      amount: Number(amount),
      currency: currency.trim() || 'USD',
      channel,
      status,
    }
    if (description.trim()) payload.description = description.trim()
    try {
      await createTransaction(payload)
      navigate('/transactions')
    } catch (err) {
      setSubmitError(err instanceof ApiError ? err.message : 'Failed to save transaction')
    }
  }

  if (loading) return <div className="status-msg">Loading…</div>

  return (
    <div>
      <div className="page-header">
        <h1>New transaction</h1>
      </div>
      <form className="form" onSubmit={onSubmit} noValidate>
        <div className="form__row">
          <label htmlFor="account_id">Account</label>
          <select id="account_id" value={accountId} onChange={(e) => setAccountId(e.target.value)}>
            <option value="">Select an account…</option>
            {accounts.map((a) => (
              <option key={a.account_id} value={a.account_id}>
                {a.account_number} ({a.account_type})
              </option>
            ))}
          </select>
          {errors.accountId && <span className="field-error">{errors.accountId}</span>}
        </div>

        <div className="form__row">
          <label htmlFor="transaction_type">Type</label>
          <select id="transaction_type" value={transactionType} onChange={(e) => setTransactionType(e.target.value)}>
            {TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>

        <div className="form__row">
          <label htmlFor="amount">Amount</label>
          <input id="amount" type="number" step="0.01" value={amount} onChange={(e) => setAmount(e.target.value)} />
          {errors.amount && <span className="field-error">{errors.amount}</span>}
        </div>

        <div className="form__row">
          <label htmlFor="currency">Currency</label>
          <input id="currency" value={currency} onChange={(e) => setCurrency(e.target.value)} />
        </div>

        <div className="form__row">
          <label htmlFor="channel">Channel</label>
          <select id="channel" value={channel} onChange={(e) => setChannel(e.target.value)}>
            {CHANNELS.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>

        <div className="form__row">
          <label htmlFor="description">Description</label>
          <input id="description" value={description} onChange={(e) => setDescription(e.target.value)} />
        </div>

        <div className="form__row">
          <label htmlFor="status">Status</label>
          <select id="status" value={status} onChange={(e) => setStatus(e.target.value)}>
            {STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>

        {submitError && <div className="form__error">{submitError}</div>}
        <div className="form__actions">
          <button type="submit" className="btn">Save</button>
          <button type="button" className="btn btn--secondary" onClick={() => navigate('/transactions')}>Cancel</button>
        </div>
      </form>
    </div>
  )
}
