import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { ChatView } from './ChatView'
import type { ChatMessage, Room } from '../types'

const room: Room = { id: 'general', name: 'General', description: '', visibility: 'public', online: 3 }

const messages: ChatMessage[] = [
  { id: 1, room: 'general', from: 'alice', text: 'Hello there!', ts: 1700000000 },
  { id: 2, room: 'general', from: 'bob', text: 'Hey Alice!', ts: 1700000060 },
]

describe('ChatView / MessageList', () => {
  it('renders messages with usernames and text', () => {
    render(
      <ChatView
        room={room}
        messages={messages}
        typing={null}
        onSend={() => {}}
        onTyping={() => {}}
        onToggleMembers={() => {}}
        showMembersToggle={false}
        activeRoom="general"
      />
    )
    expect(screen.getByText('alice')).toBeTruthy()
    expect(screen.getByText('Hello there!')).toBeTruthy()
    expect(screen.getByText('bob')).toBeTruthy()
    expect(screen.getByText('Hey Alice!')).toBeTruthy()
  })

  it('shows typing indicator when someone is typing', () => {
    render(
      <ChatView
        room={room}
        messages={[]}
        typing="carol"
        onSend={() => {}}
        onTyping={() => {}}
        onToggleMembers={() => {}}
        showMembersToggle={false}
        activeRoom="general"
      />
    )
    expect(screen.getByText(/carol is typing/)).toBeTruthy()
  })

  it('renders room header with name and online count', () => {
    render(
      <ChatView
        room={room}
        messages={[]}
        typing={null}
        onSend={() => {}}
        onTyping={() => {}}
        onToggleMembers={() => {}}
        showMembersToggle={false}
        activeRoom="general"
      />
    )
    expect(screen.getByText(/# General/)).toBeTruthy()
    expect(screen.getByText(/3 online/)).toBeTruthy()
  })
})
