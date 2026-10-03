import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import Sidebar from './Sidebar'

describe('Sidebar', () => {
  it('renders nav links for the three sections', () => {
    render(
      <MemoryRouter>
        <Sidebar />
      </MemoryRouter>,
    )
    expect(screen.getByRole('link', { name: /customers/i })).toHaveAttribute('href', '/customers')
    expect(screen.getByRole('link', { name: /accounts/i })).toHaveAttribute('href', '/accounts')
    expect(screen.getByRole('link', { name: /transactions/i })).toHaveAttribute(
      'href',
      '/transactions',
    )
    expect(screen.getByRole('link', { name: /settings/i })).toHaveAttribute('href', '/settings')
  })
})
