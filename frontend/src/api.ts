import type { Room, CreatedRoom, Member } from './types'

export async function listRooms(): Promise<Room[]> {
  const r = await fetch('/api/rooms')
  if (!r.ok) throw new Error('failed to list rooms')
  return (await r.json()).rooms ?? []
}

export async function createRoom(input: {
  id: string; name: string; description: string; visibility: 'public' | 'private'
}): Promise<CreatedRoom> {
  const r = await fetch('/api/rooms', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  })
  if (r.status === 409) throw new Error('a room with that id already exists')
  if (r.status === 400) throw new Error('invalid room')
  if (!r.ok) throw new Error('failed to create room')
  return r.json()
}

export async function listMembers(room: string): Promise<Member[]> {
  const r = await fetch(`/api/rooms/${encodeURIComponent(room)}/members`)
  if (!r.ok) throw new Error('failed to load members')
  return (await r.json()).members ?? []
}
