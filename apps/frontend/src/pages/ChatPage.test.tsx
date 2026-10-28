import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as api from '../api/chat'
import { ApiError } from '../api/client'
import ChatPage from './ChatPage'

afterEach(() => vi.restoreAllMocks())

function renderPage() {
  return render(
    <MemoryRouter>
      <ChatPage />
    </MemoryRouter>,
  )
}

describe('ChatPage', () => {
  it('shows the greeting from the backend welcome endpoint', async () => {
    vi.spyOn(api, 'getChatWelcome').mockResolvedValue({ prompt: 'Hi, Tester', source_mode: 'stub' })
    renderPage()
    expect(await screen.findByText('Hi, Tester')).toBeInTheDocument()
  })

  it('falls back to a default greeting when welcome fails', async () => {
    vi.spyOn(api, 'getChatWelcome').mockRejectedValue(new ApiError(500, 'down'))
    renderPage()
    expect(await screen.findByText('Hi there')).toBeInTheDocument()
  })

  it('sends a message and renders the user question and assistant reply', async () => {
    vi.spyOn(api, 'getChatWelcome').mockResolvedValue({ prompt: 'Hi, Tester', source_mode: 'stub' })
    const send = vi
      .spyOn(api, 'sendChatMessage')
      .mockResolvedValue({ reply: 'A stub answer.', source_mode: 'stub' })
    renderPage()
    await screen.findByText('Hi, Tester')

    await userEvent.type(screen.getByLabelText('Message'), 'Who is customer 42?')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))

    expect(await screen.findByText('Who is customer 42?')).toBeInTheDocument()
    expect(await screen.findByText('A stub answer.')).toBeInTheDocument()
    expect(send).toHaveBeenCalledWith('Who is customer 42?', [], expect.any(String))
  })

  it('shows an error bubble when the send fails', async () => {
    vi.spyOn(api, 'getChatWelcome').mockResolvedValue({ prompt: 'Hi, Tester', source_mode: 'stub' })
    vi.spyOn(api, 'sendChatMessage').mockRejectedValue(new ApiError(500, 'boom'))
    renderPage()
    await screen.findByText('Hi, Tester')

    await userEvent.type(screen.getByLabelText('Message'), 'hello')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))

    expect(await screen.findByText('boom')).toBeInTheDocument()
  })

  it('passes prior turns as history on the second message', async () => {
    vi.spyOn(api, 'getChatWelcome').mockResolvedValue({ prompt: 'Hi, Tester', source_mode: 'stub' })
    const send = vi
      .spyOn(api, 'sendChatMessage')
      .mockResolvedValue({ reply: 'First reply.', source_mode: 'stub' })
    renderPage()
    await screen.findByText('Hi, Tester')

    await userEvent.type(screen.getByLabelText('Message'), 'first')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))
    await screen.findByText('First reply.')

    await userEvent.type(screen.getByLabelText('Message'), 'second')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))

    expect(send).toHaveBeenLastCalledWith(
      'second',
      [
        { role: 'user', content: 'first' },
        { role: 'assistant', content: 'First reply.' },
      ],
      expect.any(String),
    )
  })

  it('reuses the same session id across turns and shows it after the first send', async () => {
    vi.spyOn(api, 'getChatWelcome').mockResolvedValue({ prompt: 'Hi, Tester', source_mode: 'stub' })
    const send = vi
      .spyOn(api, 'sendChatMessage')
      .mockResolvedValue({ reply: 'ok', source_mode: 'stub' })
    renderPage()
    await screen.findByText('Hi, Tester')

    // No session chip before the first message.
    expect(screen.queryByText('Session')).not.toBeInTheDocument()

    await userEvent.type(screen.getByLabelText('Message'), 'first')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))
    await screen.findByText('ok')

    expect(screen.getByText('Session')).toBeInTheDocument()
    const firstSid = send.mock.calls[0][2]
    expect(firstSid).toBeTruthy()

    await userEvent.type(screen.getByLabelText('Message'), 'second')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))
    await waitFor(() => expect(send).toHaveBeenCalledTimes(2))
    expect(send.mock.calls[1][2]).toBe(firstSid) // same session id reused
  })

  it('reset clears the session and the conversation', async () => {
    vi.spyOn(api, 'getChatWelcome').mockResolvedValue({ prompt: 'Hi, Tester', source_mode: 'stub' })
    const send = vi
      .spyOn(api, 'sendChatMessage')
      .mockResolvedValue({ reply: 'ok', source_mode: 'stub' })
    const reset = vi.spyOn(api, 'resetChatSession').mockResolvedValue({ cleared: true })
    renderPage()
    await screen.findByText('Hi, Tester')

    await userEvent.type(screen.getByLabelText('Message'), 'first')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))
    await screen.findByText('ok')
    const sid = send.mock.calls[0][2] as string

    await userEvent.click(screen.getByRole('button', { name: /reset/i }))
    await waitFor(() => expect(reset).toHaveBeenCalledWith(sid))

    // Conversation cleared → empty state back, session chip gone.
    expect(await screen.findByText('Hi, Tester')).toBeInTheDocument()
    expect(screen.queryByText('Session')).not.toBeInTheDocument()

    // Next message mints a NEW session id.
    await userEvent.type(screen.getByLabelText('Message'), 'again')
    await userEvent.click(screen.getByRole('button', { name: /send/i }))
    await waitFor(() => expect(send).toHaveBeenCalledTimes(2))
    expect(send.mock.calls[1][2]).not.toBe(sid)
  })
})
