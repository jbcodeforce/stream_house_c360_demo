import { useEffect, useState } from 'react'
import { ApiError } from '../api/client'
import { getConfig, updateConfig } from '../api/config'

export default function ConfigPage() {
  const [enabled, setEnabled] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getConfig()
      .then((c) => setEnabled(c.kafka_produce_enabled))
      .catch((e) => setError(e instanceof ApiError ? e.message : 'Failed to load config'))
      .finally(() => setLoading(false))
  }, [])

  async function onToggle() {
    const next = !enabled
    setEnabled(next) // optimistic
    setError(null)
    try {
      await updateConfig({ kafka_produce_enabled: next })
    } catch (e) {
      setEnabled(!next) // revert
      setError(e instanceof ApiError ? e.message : 'Failed to save config')
    }
  }

  if (loading) return <div className="status-msg">Loading…</div>

  return (
    <div>
      <div className="page-header">
        <h1>Settings</h1>
      </div>
      {error && <div className="status-msg status-msg--error">{error}</div>}
      <label className="switch-row">
        <input type="checkbox" checked={enabled} onChange={onToggle} />
        <span>Produce Kafka events on create / update / delete</span>
      </label>
      <p className="status-msg">
        When on, each customer create, update, or delete also emits a Kafka
        event (best-effort). Postgres remains the source of truth.
      </p>
    </div>
  )
}
