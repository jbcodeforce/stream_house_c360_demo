import { Eye, Pencil, Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import ConfirmDialog from '../components/ConfirmDialog'
import { ApiError } from '../api/client'
import { deleteAccount, listAccounts } from '../api/accounts'
import { listCustomers } from '../api/customers'
import type { Account } from '../types/account'

export default function AccountsPage() {
  const navigate = useNavigate()
  const [accounts, setAccounts] = useState<Account[]>([])
  const [customerNames, setCustomerNames] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [pendingDelete, setPendingDelete] = useState<Account | null>(null)

  async function load() {
    setLoading(true)
    setError(null)
    try {
      const [accts, custs] = await Promise.all([listAccounts(), listCustomers()])
      setAccounts(accts)
      setCustomerNames(
        Object.fromEntries(custs.map((c) => [c.customer_id, `${c.first_name} ${c.last_name}`])),
      )
    } catch (e) {
      setError(e instanceof ApiError ? e.message : 'Failed to load accounts')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  async function confirmDelete() {
    if (!pendingDelete) return
    try {
      await deleteAccount(pendingDelete.account_id)
      setPendingDelete(null)
      await load()
    } catch (e) {
      setError(e instanceof ApiError ? e.message : 'Failed to delete account')
      setPendingDelete(null)
    }
  }

  return (
    <div>
      <div className="page-header">
        <h1>Accounts</h1>
        <Link to="/accounts/new" className="btn">Add account</Link>
      </div>

      {loading && <div className="status-msg">Loading…</div>}
      {error && <div className="status-msg status-msg--error">{error}</div>}

      {!loading && !error && (
        <table className="table">
          <thead>
            <tr>
              <th>Account #</th><th>Type</th><th>Customer</th><th>Balance</th><th>Status</th><th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {accounts.map((a) => (
              <tr key={a.account_id}>
                <td>{a.account_number}</td>
                <td>{a.account_type}</td>
                <td>{customerNames[a.customer_id] ?? a.customer_id}</td>
                <td>{a.balance ?? 0} {a.currency ?? ''}</td>
                <td>{a.status ?? '—'}</td>
                <td>
                  <div className="row-actions">
                    <button className="icon-btn" aria-label="View" onClick={() => navigate(`/accounts/${a.account_id}`)}><Eye size={18} /></button>
                    <button className="icon-btn" aria-label="Edit" onClick={() => navigate(`/accounts/${a.account_id}/edit`)}><Pencil size={18} /></button>
                    <button className="icon-btn icon-btn--danger" aria-label="Delete" onClick={() => setPendingDelete(a)}><Trash2 size={18} /></button>
                  </div>
                </td>
              </tr>
            ))}
            {accounts.length === 0 && (
              <tr><td colSpan={6} className="status-msg">No accounts yet.</td></tr>
            )}
          </tbody>
        </table>
      )}

      <ConfirmDialog
        open={pendingDelete !== null}
        title="Delete account"
        message={pendingDelete ? `Delete account ${pendingDelete.account_number}? This cannot be undone.` : ''}
        onConfirm={confirmDelete}
        onCancel={() => setPendingDelete(null)}
      />
    </div>
  )
}
