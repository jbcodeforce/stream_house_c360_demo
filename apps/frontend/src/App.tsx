import { Navigate, Route, Routes } from 'react-router-dom'
import AppLayout from './components/AppLayout'
import PlaceholderPage from './pages/PlaceholderPage'
import CustomersPage from './pages/CustomersPage'
import CustomerDetailPage from './pages/CustomerDetailPage'
import CustomerFormPage from './pages/CustomerFormPage'
import ConfigPage from './pages/ConfigPage'

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<Navigate to="/customers" replace />} />
        <Route path="/customers" element={<CustomersPage />} />
        <Route path="/customers/new" element={<CustomerFormPage />} />
        <Route path="/customers/:id" element={<CustomerDetailPage />} />
        <Route path="/customers/:id/edit" element={<CustomerFormPage />} />
        <Route path="/accounts" element={<PlaceholderPage title="Accounts" />} />
        <Route path="/transactions" element={<PlaceholderPage title="Transactions" />} />
        <Route path="/settings" element={<ConfigPage />} />
      </Route>
    </Routes>
  )
}
