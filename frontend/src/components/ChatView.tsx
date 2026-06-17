import { useEffect, useRef, useState, useCallback } from 'react'
import type { ChatMessage, Room } from '../types'
import s from './ChatView.module.css'

const MAX_CHARS = 2000

const ACCENT_COLORS = [
  '#4f8ef7', '#7c6af7', '#e97af7', '#f76a6a', '#f7a26a',
  '#6af7c5', '#6ad4f7', '#b0f76a',
]

function avatarColor(username: string) {
  let hash = 0
  for (let i = 0; i < username.length; i++) hash = username.charCodeAt(i) + ((hash << 5) - hash)
  return ACCENT_COLORS[Math.abs(hash) % ACCENT_COLORS.length]
}

function initials(username: string) {
  return username.slice(0, 2).toUpperCase()
}

function formatTime(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function formatDate(ts: number) {
  const d = new Date(ts * 1000)
  const today = new Date()
  const yesterday = new Date(today)
  yesterday.setDate(today.getDate() - 1)
  if (d.toDateString() === today.toDateString()) return 'Today'
  if (d.toDateString() === yesterday.toDateString()) return 'Yesterday'
  return d.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' })
}

function dayKey(ts: number) {
  const d = new Date(ts * 1000)
  return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`
}

// ── DateDivider ─────────────────────────────────────────────────────────────

function DateDivider({ ts }: { ts: number }) {
  return (
    <div className={s.dateDivider}>
      <div className={s.dateLine} />
      <span className={s.dateLabel}>{formatDate(ts)}</span>
      <div className={s.dateLine} />
    </div>
  )
}

// ── MessageItem ─────────────────────────────────────────────────────────────

function MessageItem({ msg }: { msg: ChatMessage }) {
  return (
    <div className={s.message}>
      <div className={s.avatar} style={{ background: avatarColor(msg.from) }}>
        {initials(msg.from)}
      </div>
      <div className={s.msgBody}>
        <div className={s.msgMeta}>
          <span className={s.msgFrom}>{msg.from}</span>
          <span className={s.msgTime}>{formatTime(msg.ts)}</span>
        </div>
        <div className={s.msgText}>{msg.text}</div>
      </div>
    </div>
  )
}

// ── TypingIndicator ──────────────────────────────────────────────────────────

function TypingIndicator({ typing }: { typing: string | null }) {
  if (!typing) return <div className={s.typing} />
  return (
    <div className={s.typing}>
      {typing} is typing
      <span className={s.typingDots}>
        <span /><span /><span />
      </span>
    </div>
  )
}

// ── Composer ─────────────────────────────────────────────────────────────────

interface ComposerProps {
  room: string
  onSend: (text: string) => void
  onTyping: (room: string) => void
}

function Composer({ room, onSend, onTyping }: ComposerProps) {
  const [text, setText] = useState('')
  const lastTypingSent = useRef(0)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  function handleInput(e: React.ChangeEvent<HTMLTextAreaElement>) {
    setText(e.target.value)
    const now = Date.now()
    if (now - lastTypingSent.current > 1500) {
      onTyping(room)
      lastTypingSent.current = now
    }
    // auto-resize
    const el = e.target
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, 120)}px`
  }

  function handleKey(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit()
    }
  }

  function submit() {
    const t = text.trim()
    if (!t || t.length > MAX_CHARS) return
    onSend(t)
    setText('')
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
    }
  }

  const over = text.length > MAX_CHARS
  const remaining = MAX_CHARS - text.length

  return (
    <div className={s.composer}>
      <div className={s.composerInner}>
        <textarea
          ref={textareaRef}
          className={s.composerInput}
          rows={1}
          value={text}
          onChange={handleInput}
          onKeyDown={handleKey}
          placeholder={`Message #${room}`}
        />
        {text.length > MAX_CHARS - 200 && (
          <span className={`${s.charCounter} ${over ? s.charCounterOver : ''}`}>
            {remaining}
          </span>
        )}
        <button className={s.sendBtn} onClick={submit} disabled={!text.trim() || over}>
          ↑
        </button>
      </div>
    </div>
  )
}

// ── RoomHeader ────────────────────────────────────────────────────────────────

interface RoomHeaderProps {
  room: Room | undefined
  onToggleMembers: () => void
  showMembersToggle: boolean
}

function RoomHeader({ room, onToggleMembers, showMembersToggle }: RoomHeaderProps) {
  return (
    <div className={s.header}>
      <span className={s.headerName}>{room ? `# ${room.name}` : ''}</span>
      {room && (
        <span className={s.headerOnline}>
          <span>●</span> {room.online} online
        </span>
      )}
      <div className={s.headerSpacer} />
      {showMembersToggle && (
        <button className={s.membersToggle} onClick={onToggleMembers}>
          Members
        </button>
      )}
    </div>
  )
}

// ── MessageList ───────────────────────────────────────────────────────────────

function MessageList({ messages }: { messages: ChatMessage[] }) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length])

  const items: React.ReactNode[] = []
  let lastDay = ''

  for (const msg of messages) {
    const day = dayKey(msg.ts)
    if (day !== lastDay) {
      items.push(<DateDivider key={`div-${day}`} ts={msg.ts} />)
      lastDay = day
    }
    items.push(<MessageItem key={msg.id} msg={msg} />)
  }

  return (
    <div className={s.messageList}>
      {items.length === 0 && (
        <div className={s.systemLine}>No messages yet — say hello!</div>
      )}
      {items}
      <div ref={bottomRef} />
    </div>
  )
}

// ── ChatView ──────────────────────────────────────────────────────────────────

interface ChatViewProps {
  room: Room | undefined
  messages: ChatMessage[]
  typing: string | null
  onSend: (text: string) => void
  onTyping: (room: string) => void
  onToggleMembers: () => void
  showMembersToggle: boolean
  activeRoom: string
}

export function ChatView({
  room, messages, typing, onSend, onTyping, onToggleMembers, showMembersToggle, activeRoom,
}: ChatViewProps) {
  const handleSend = useCallback((text: string) => onSend(text), [onSend])

  return (
    <div className={s.chatView}>
      <RoomHeader room={room} onToggleMembers={onToggleMembers} showMembersToggle={showMembersToggle} />
      <MessageList messages={messages} />
      <TypingIndicator typing={typing} />
      <Composer room={activeRoom} onSend={handleSend} onTyping={onTyping} />
    </div>
  )
}
