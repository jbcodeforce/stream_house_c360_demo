export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
}

export interface ChatRequest {
  message: string
  history: ChatMessage[]
}

export interface ChatResponse {
  reply: string
  source_mode: string
}

export interface WelcomeResponse {
  prompt: string
  source_mode: string
}
