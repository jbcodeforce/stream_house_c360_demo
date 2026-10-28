import type { ChatMessage, ChatResponse, WelcomeResponse } from '../types/chat'
import { request } from './client'

/** The empty-state greeting, sourced from the backend's CHAT_FIRST_PROMPT. */
export function getChatWelcome(): Promise<WelcomeResponse> {
  return request<WelcomeResponse>('/chat/welcome')
}

/** Send a question plus prior turns; returns the assistant's reply.
 *  `sessionId` ties the turn to a reusable server-side agent/MCP connection. */
export function sendChatMessage(
  message: string,
  history: ChatMessage[],
  sessionId?: string,
): Promise<ChatResponse> {
  return request<ChatResponse>('/chat', {
    method: 'POST',
    body: JSON.stringify({ message, history, session_id: sessionId }),
  })
}

/** Close and forget the server-side session (the chat's reset action). */
export function resetChatSession(sessionId: string): Promise<{ cleared: boolean }> {
  return request<{ cleared: boolean }>('/chat/reset', {
    method: 'POST',
    body: JSON.stringify({ session_id: sessionId }),
  })
}
