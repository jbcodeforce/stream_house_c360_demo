import { Navigate, Route, Routes } from 'react-router-dom'
import AppLayout from './components/AppLayout'
import CustomersPage from './pages/CustomersPage'
import CustomerDetailPage from './pages/CustomerDetailPage'
import CustomerFormPage from './pages/CustomerFormPage'
import AccountsPage from './pages/AccountsPage'
import AccountDetailPage from './pages/AccountDetailPage'
import AccountFormPage from './pages/AccountFormPage'
import TransactionsPage from './pages/TransactionsPage'
import TransactionDetailPage from './pages/TransactionDetailPage'
import TransactionFormPage from './pages/TransactionFormPage'
import GeneratedTransactionsPage from './pages/GeneratedTransactionsPage'
import DataProductsPage from './pages/DataProductsPage'

// Placeholder for Task 8's StatementGraphPage; keeps the /data-products/:id route
// compiling until Task 8 creates the real page and repoints this route.
function StatementGraphPlaceholder() {
  return <div className="status-msg">Statement graph coming soon…</div>
}

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/customers" replace />} />
        <Route path="/customers" element={<CustomersPage />} />
        <Route path="/customers/new" element={<CustomerFormPage />} />
        <Route path="/customers/:id" element={<CustomerDetailPage />} />
        <Route path="/customers/:id/edit" element={<CustomerFormPage />} />
        <Route path="/accounts" element={<AccountsPage />} />
        <Route path="/accounts/new" element={<AccountFormPage />} />
        <Route path="/accounts/:id" element={<AccountDetailPage />} />
        <Route path="/accounts/:id/edit" element={<AccountFormPage />} />
        <Route path="/transactions" element={<TransactionsPage />} />
        <Route path="/transactions/new" element={<TransactionFormPage />} />
        <Route path="/transactions/generated" element={<GeneratedTransactionsPage />} />
        <Route path="/transactions/:id" element={<TransactionDetailPage />} />
        <Route path="/data-products" element={<DataProductsPage />} />
        <Route path="/data-products/:id" element={<StatementGraphPlaceholder />} />
      </Route>
    </Routes>
  )
}
