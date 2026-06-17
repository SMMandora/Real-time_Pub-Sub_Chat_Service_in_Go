export type FrameType = 'join' | 'leave' | 'send' | 'typing' | 'message' | 'system' | 'presence' | 'error'

export interface Frame {
  type: FrameType
  room?: string
  id?: number
  text?: string
  from?: string
  ts?: number
  event?: string
  members?: string[]
  message?: string
  token?: string
}

export interface Room {
  id: string
  name: string
  description: string
  visibility: 'public' | 'private'
  online: number
}

export interface CreatedRoom extends Room {
  invite_token?: string
}

export interface Member {
  username: string
  status: 'online' | 'away' | 'offline'
}

export interface ChatMessage {
  id: number
  room: string
  from: string
  text: string
  ts: number
}
