import { useEffect, useRef, useState } from 'react'
import { Bot, RotateCcw, Send } from 'lucide-react'
import { ApiError } from '../api/client'
import { getChatWelcome, resetChatSession, sendChatMessage } from '../api/chat'
import type { ChatMessage } from '../types/chat'

/** A rendered message; `error` marks a failed turn (shown but not sent as history). */
type DisplayMessage = ChatMessage & { error?: boolean }

/** A UUID for a new conversation; falls back when crypto.randomUUID is absent. */
function newSessionId(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) return crypto.randomUUID()
  return `sess-${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`
}

export default function ChatPage() {
  const [greeting, setGreeting] = useState<string | null>(null)
  const [messages, setMessages] = useState<DisplayMessage[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [sessionId, setSessionId] = useState<string | null>(null)
  const scrollRef = useRef<HTMLDivElement>(null)

  // Fetch the greeting on mount. This also confirms the frontend reaches the
  // backend (CHAT_FIRST_PROMPT propagation); on failure we degrade gracefully.
  useEffect(() => {
    let active = true
    getChatWelcome()
      .then((w) => { if (active) setGreeting(w.prompt) })
      .catch(() => { if (active) setGreeting('Hi there') })
    return () => { active = false }
  }, [])

  // Keep the newest message in view.
  useEffect(() => {
    const el = scrollRef.current
    if (el) el.scrollTop = el.scrollHeight
  }, [messages, sending])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const text = input.trim()
    if (!text || sending) return

    // Mint the session id on the first submit; reuse it for the whole conversation.
    let sid = sessionId
    if (!sid) {
      sid = newSessionId()
      setSessionId(sid)
    }

    const history: ChatMessage[] = messages
      .filter((m) => !m.error)
      .map(({ role, content }) => ({ role, content }))

    setMessages((prev) => [...prev, { role: 'user', content: text }])
    setInput('')
    setSending(true)
    try {
      const res = await sendChatMessage(text, history, sid)
      setMessages((prev) => [...prev, { role: 'assistant', content: res.reply }])
    } catch (err) {
      const msg = err instanceof ApiError ? err.message : 'Failed to reach the assistant.'
      setMessages((prev) => [...prev, { role: 'assistant', content: msg, error: true }])
    } finally {
      setSending(false)
    }
  }

  async function handleReset() {
    const sid = sessionId
    setSessionId(null)
    setMessages([])
    setInput('')
    if (sid) {
      // Best-effort: close the server-side session. A failure here is harmless.
      try {
        await resetChatSession(sid)
      } catch {
        // ignore — the server evicts idle sessions anyway
      }
    }
  }

  return (
    <div className="chat-page">
      <div className="page-header">
        <div>
          <h1>Assistant</h1>
          <p className="page-subtitle">
            Ask questions about your customer 360 data in natural language.
          </p>
        </div>
        {sessionId && (
          <div className="chat-session">
            <span className="chat-session__label">Session</span>
            <code className="chat-session__id" title={sessionId}>{sessionId}</code>
            <button
              type="button"
              className="btn btn--secondary btn--icon chat-session__reset"
              onClick={() => void handleReset()}
              disabled={sending}
            >
              <RotateCcw size={14} /> Reset
            </button>
          </div>
        )}
      </div>

      <div className="chat-panel">
        <div className="chat-panel__scroll" ref={scrollRef}>
          {messages.length === 0 ? (
            <div className="chat-empty">
              <div className="chat-empty__icon">
                <Bot size={28} />
              </div>
              <h2 className="chat-empty__greeting">{greeting ?? 'Hi there'}</h2>
              <p className="chat-empty__desc">
                Ask about your customers and accounts. Answers draw on the real-time
                C360 profile data.
              </p>
            </div>
          ) : (
            <ul className="chat-messages">
              {messages.map((m, i) => (
                <li key={i} className={`chat-msg chat-msg--${m.role}`}>
                  <div className={`chat-msg__bubble${m.error ? ' chat-msg__bubble--error' : ''}`}>
                    {m.content}
                  </div>
                </li>
              ))}
              {sending && (
                <li className="chat-msg chat-msg--assistant">
                  <div className="chat-msg__bubble chat-msg__bubble--thinking">Thinking…</div>
                </li>
              )}
            </ul>
          )}
        </div>

        <form className="chat-composer" onSubmit={handleSubmit}>
          <input
            className="chat-composer__input"
            placeholder="Ask a question about your customers…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={sending}
            aria-label="Message"
          />
          <button
            type="submit"
            className="chat-composer__send"
            disabled={sending || !input.trim()}
            aria-label="Send"
          >
            <Send size={18} />
          </button>
        </form>
      </div>
    </div>
  )
}
