export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
}

export interface ChatRequest {
  message: string
  history: ChatMessage[]
  session_id?: string
}

export interface ChatResponse {
  reply: string
  source_mode: string
  session_id?: string | null
}

export interface WelcomeResponse {
  prompt: string
  source_mode: string
}
