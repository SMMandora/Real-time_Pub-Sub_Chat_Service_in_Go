import { useCallback, useEffect, useRef, useState } from 'react'
import type { Frame, ChatMessage, Member, Room } from './types'
import { listRooms, listMembers } from './api'

export type Status = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'offline'

export function useChat(username: string | null) {
  const [status, setStatus] = useState<Status>('idle')
  const [rooms, setRooms] = useState<Room[]>([])
  const [activeRoom, setActiveRoom] = useState<string | null>(null)
  const [messages, setMessages] = useState<Record<string, ChatMessage[]>>({})
  const [members, setMembers] = useState<Member[]>([])
  const [typing, setTyping] = useState<Record<string, string | null>>({})
  const [errors, setErrors] = useState<string[]>([])
  const [recovering, setRecovering] = useState(false)

  const wsRef = useRef<WebSocket | null>(null)
  const lastSeen = useRef<Record<string, number>>({})
  const activeRef = useRef<string | null>(null)
  const backoff = useRef(1000)
  const typingTimers = useRef<Record<string, ReturnType<typeof setTimeout>>>({})
  const closedByUser = useRef(false)
  activeRef.current = activeRoom

  const refreshRooms = useCallback(() => { listRooms().then(setRooms).catch(() => {}) }, [])
  const refreshMembers = useCallback((room: string) => {
    if (room === activeRef.current) listMembers(room).then(setMembers).catch(() => {})
  }, [])

  const handle = useCallback((f: Frame) => {
    switch (f.type) {
      case 'message': {
        if (f.id == null || !f.room) return
        const room = f.room, id = f.id
        lastSeen.current[room] = Math.max(lastSeen.current[room] ?? 0, id)
        setMessages((m) => {
          const list = m[room] ?? []
          if (list.some((x) => x.id === id)) return m
          const next = [...list, { id, room, from: f.from ?? '', text: f.text ?? '', ts: f.ts ?? 0 }]
            .sort((a, b) => a.id - b.id)
          return { ...m, [room]: next }
        })
        if (room === activeRef.current) setRecovering(false)
        break
      }
      case 'system':
        if (f.room) refreshMembers(f.room)
        break
      case 'presence':
        if (f.room) {
          refreshMembers(f.room)
          setRooms((rs) => rs.map((r) => r.id === f.room ? { ...r, online: f.members?.length ?? r.online } : r))
        }
        break
      case 'typing': {
        if (!f.room || !f.from) return
        const room = f.room
        setTyping((t) => ({ ...t, [room]: f.from! }))
        clearTimeout(typingTimers.current[room])
        typingTimers.current[room] = setTimeout(() => setTyping((t) => ({ ...t, [room]: null })), 3000)
        break
      }
      case 'error':
        setErrors((e) => [...e, f.message ?? 'error'])
        break
    }
  }, [refreshMembers])

  const connect = useCallback(() => {
    if (!username) return
    setStatus(wsRef.current ? 'reconnecting' : 'connecting')
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${proto}://${location.host}/ws?username=${encodeURIComponent(username)}`)
    wsRef.current = ws
    ws.onopen = () => {
      setStatus('connected')
      backoff.current = 1000
      refreshRooms()
      const room = activeRef.current
      if (room) {
        setRecovering(true)
        ws.send(JSON.stringify({ type: 'join', room, id: lastSeen.current[room] ?? 0 }))
        refreshMembers(room)
      }
    }
    ws.onmessage = (e) => { try { handle(JSON.parse(e.data)) } catch { /* ignore */ } }
    ws.onclose = () => {
      wsRef.current = null
      if (closedByUser.current) { setStatus('offline'); return }
      setStatus('reconnecting')
      const wait = backoff.current
      backoff.current = Math.min(backoff.current * 2, 15000)
      setTimeout(connect, wait)
    }
    ws.onerror = () => ws.close()
  }, [username, handle, refreshRooms, refreshMembers])

  useEffect(() => {
    if (!username) return
    closedByUser.current = false
    connect()
    return () => { closedByUser.current = true; wsRef.current?.close() }
  }, [username, connect])

  const join = useCallback((room: string, token?: string) => {
    setActiveRoom(room)
    activeRef.current = room
    refreshMembers(room)
    wsRef.current?.send(JSON.stringify({ type: 'join', room, token: token ?? '', id: lastSeen.current[room] ?? 0 }))
  }, [refreshMembers])

  const leave = useCallback(() => { setActiveRoom(null); activeRef.current = null; setMembers([]) }, [])
  const send = useCallback((room: string, text: string) => {
    wsRef.current?.send(JSON.stringify({ type: 'send', room, text }))
  }, [])
  const sendTyping = useCallback((room: string) => {
    wsRef.current?.send(JSON.stringify({ type: 'typing', room }))
  }, [])
  const dismissError = useCallback((i: number) => setErrors((e) => e.filter((_, idx) => idx !== i)), [])

  return {
    status, rooms, activeRoom,
    messages: activeRoom ? (messages[activeRoom] ?? []) : [],
    members, typing: activeRoom ? (typing[activeRoom] ?? null) : null,
    errors, recovering,
    join, leave, send, sendTyping, refreshRooms, dismissError,
  }
}
