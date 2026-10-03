import { Users, Wallet, ArrowLeftRight, Settings } from 'lucide-react'
import { NavLink } from 'react-router-dom'

const links = [
  { to: '/customers', label: 'Customers', Icon: Users },
  { to: '/accounts', label: 'Accounts', Icon: Wallet },
  { to: '/transactions', label: 'Transactions', Icon: ArrowLeftRight },
  { to: '/settings', label: 'Settings', Icon: Settings },
]

export default function Sidebar() {
  return (
    <nav className="sidebar">
      {links.map(({ to, label, Icon }) => (
        <NavLink
          key={to}
          to={to}
          className={({ isActive }) => `sidebar__link${isActive ? ' sidebar__link--active' : ''}`}
        >
          <Icon size={18} />
          <span>{label}</span>
        </NavLink>
      ))}
    </nav>
  )
}
