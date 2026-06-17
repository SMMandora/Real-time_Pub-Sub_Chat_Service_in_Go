import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { useChat } from './useChat'

// ── Mock WebSocket ──────────────────────────────────────────────────────────

class FakeWebSocket {
  onopen: (() => void) | null = null
  onmessage: ((e: { data: string }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  sent: string[] = []

  constructor(_url: string) {
    allWS.push(this)
    lastWS = this
  }

  send(data: string) { this.sent.push(data) }
  close() { if (this.onclose) this.onclose() }

  _fireOpen() { if (this.onopen) this.onopen() }
  _fireMessage(data: object) {
    if (this.onmessage) this.onmessage({ data: JSON.stringify(data) })
  }
  _fireClose() { if (this.onclose) this.onclose() }
}

let lastWS: FakeWebSocket | null = null
const allWS: FakeWebSocket[] = []

// ── Mock fetch ──────────────────────────────────────────────────────────────

function mockFetch() {
  global.fetch = vi.fn().mockImplementation((url: string) => {
    if (url === '/api/rooms') {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ rooms: [] }) })
    }
    if (String(url).includes('/members')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ members: [] }) })
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) })
  })
}

// ── Setup / teardown ────────────────────────────────────────────────────────

beforeEach(() => {
  allWS.length = 0
  lastWS = null
  mockFetch()
  // @ts-expect-error replacing global
  global.WebSocket = FakeWebSocket
})

afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

// ── Helpers ─────────────────────────────────────────────────────────────────

function openSocket() {
  act(() => { lastWS!._fireOpen() })
}

// ── Tests ────────────────────────────────────────────────────────────────────

describe('useChat', () => {
  it('status becomes connected after open', () => {
    const { result } = renderHook(() => useChat('alice'))
    expect(result.current.status).toBe('connecting')
    openSocket()
    expect(result.current.status).toBe('connected')
  })

  it('dedups messages by id', () => {
    const { result } = renderHook(() => useChat('alice'))
    openSocket()

    act(() => { result.current.join('general') })

    const msg = { type: 'message', id: 42, room: 'general', from: 'bob', text: 'hi', ts: 1000 }

    act(() => { lastWS!._fireMessage(msg) })
    act(() => { lastWS!._fireMessage(msg) }) // duplicate

    expect(result.current.messages).toHaveLength(1)
    expect(result.current.messages[0].id).toBe(42)
    expect(result.current.messages[0].from).toBe('bob')
  })

  it('typing auto-clears after 3 seconds', () => {
    vi.useFakeTimers()
    const { result } = renderHook(() => useChat('alice'))
    openSocket()

    act(() => { result.current.join('general') })

    act(() => {
      lastWS!._fireMessage({ type: 'typing', room: 'general', from: 'carol' })
    })

    expect(result.current.typing).toBe('carol')

    act(() => { vi.advanceTimersByTime(3001) })

    expect(result.current.typing).toBeNull()
  })

  it('reconnects and replays since lastSeen', () => {
    vi.useFakeTimers()
    const { result } = renderHook(() => useChat('alice'))
    openSocket()

    // Join a room and receive a message so lastSeen is set
    act(() => { result.current.join('general') })

    act(() => {
      lastWS!._fireMessage({ type: 'message', id: 7, room: 'general', from: 'bob', text: 'hey', ts: 1 })
    })

    // Simulate unexpected close (socket closes itself, not by user)
    // We need to access onclose without going through the user-initiated close path.
    const firstWS = lastWS!
    act(() => {
      // Directly fire onclose — closedByUser.current is false because user didn't call close()
      if (firstWS.onclose) firstWS.onclose()
    })

    expect(result.current.status).toBe('reconnecting')

    // Advance past backoff (1000ms default)
    act(() => { vi.advanceTimersByTime(1100) })

    // A new WebSocket should have been created
    expect(allWS).toHaveLength(2)
    const secondWS = allWS[1]

    // Fire open on new socket
    act(() => { if (secondWS.onopen) secondWS.onopen() })

    expect(result.current.status).toBe('connected')

    // The re-join should carry lastSeen id = 7
    const joinMsg = secondWS.sent.find((s) => {
      try { return JSON.parse(s).type === 'join' } catch { return false }
    })
    expect(joinMsg).toBeDefined()
    const parsed = JSON.parse(joinMsg!)
    expect(parsed.room).toBe('general')
    expect(parsed.id).toBe(7)
  })
})
