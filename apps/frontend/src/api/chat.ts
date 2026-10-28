import type { ChatMessage, ChatResponse, WelcomeResponse } from '../types/chat'
import { request } from './client'

/** The empty-state greeting, sourced from the backend's CHAT_FIRST_PROMPT. */
export function getChatWelcome(): Promise<WelcomeResponse> {
  return request<WelcomeResponse>('/chat/welcome')
}

/** Send a question plus prior turns; returns the assistant's reply. */
export function sendChatMessage(message: string, history: ChatMessage[]): Promise<ChatResponse> {
  return request<ChatResponse>('/chat', {
    method: 'POST',
    body: JSON.stringify({ message, history }),
  })
}
