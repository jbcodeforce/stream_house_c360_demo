import { Eye } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { ApiError } from '../api/client'
import { listTransactions } from '../api/transactions'
import { listAccounts } from '../api/accounts'
import { listCustomers } from '../api/customers'
import type { Transaction } from '../types/transaction'

export default function TransactionsPage() {
  const navigate = useNavigate()
  const [transactions, setTransactions] = useState<Transaction[]>([])
  const [accountNumbers, setAccountNumbers] = useState<Record<string, string>>({})
  const [customerNames, setCustomerNames] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function load() {
      try {
        const [txns, accts, custs] = await Promise.all([
          listTransactions(), listAccounts(), listCustomers(),
        ])
        setTransactions(txns)
        setAccountNumbers(Object.fromEntries(accts.map((a) => [a.account_id, a.account_number])))
        setCustomerNames(
          Object.fromEntries(custs.map((c) => [c.customer_id, `${c.first_name} ${c.last_name}`])),
        )
      } catch (e) {
        setError(e instanceof ApiError ? e.message : 'Failed to load transactions')
      } finally {
        setLoading(false)
      }
    }
    void load()
  }, [])

  return (
    <div>
      <div className="page-header">
        <h1>Transactions</h1>
        <Link to="/transactions/new" className="btn">Add transaction</Link>
      </div>

      {loading && <div className="status-msg">Loading…</div>}
      {error && <div className="status-msg status-msg--error">{error}</div>}

      {!loading && !error && (
        <table className="table">
          <thead>
            <tr>
              <th>Transacted</th><th>Type</th><th>Amount</th><th>Account #</th><th>Customer</th><th>Status</th><th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {transactions.map((t) => (
              <tr key={t.transaction_id}>
                <td>{t.transacted_at ? String(t.transacted_at).slice(0, 10) : '—'}</td>
                <td>{t.transaction_type}</td>
                <td>{t.amount} {t.currency ?? ''}</td>
                <td>{accountNumbers[t.account_id] ?? t.account_id}</td>
                <td>{customerNames[t.customer_id] ?? t.customer_id}</td>
                <td>{t.status ?? '—'}</td>
                <td>
                  <div className="row-actions">
                    <button className="icon-btn" aria-label="View" onClick={() => navigate(`/transactions/${t.transaction_id}`)}><Eye size={18} /></button>
                  </div>
                </td>
              </tr>
            ))}
            {transactions.length === 0 && (
              <tr><td colSpan={7} className="status-msg">No transactions yet.</td></tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
