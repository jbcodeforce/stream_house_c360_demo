import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { ApiError } from '../api/client'
import { listDataProducts } from '../api/dataProducts'
import type { DataProduct } from '../types/dataProduct'

export default function DataProductsPage() {
  const [dataProducts, setDataProducts] = useState<DataProduct[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  async function load() {
    setLoading(true)
    setError(null)
    try {
      const products = await listDataProducts()
      setDataProducts(products)
    } catch (e) {
      setError(e instanceof ApiError ? e.message : 'Failed to load data products')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  return (
    <div>
      <div className="page-header">
        <h1>Data Products</h1>
      </div>

      {loading && <div className="status-msg">Loading…</div>}
      {error && <div className="status-msg status-msg--error">{error}</div>}

      {!loading && !error && (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th><th>Description</th><th>Statements</th>
            </tr>
          </thead>
          <tbody>
            {dataProducts.map((p) => (
              <tr key={p.id}>
                <td><Link to={`/data-products/${p.id}`} state={{ name: p.name }}>{p.name}</Link></td>
                <td>{p.description}</td>
                <td>{p.statement_count}</td>
              </tr>
            ))}
            {dataProducts.length === 0 && (
              <tr><td colSpan={3} className="status-msg">No data products yet.</td></tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
