import { useState } from 'react'
import type { Room } from '../types'
import s from './Directory.module.css'

interface Props {
  rooms: Room[]
  onJoin: (id: string, token?: string) => void
}

export function Directory({ rooms, onJoin }: Props) {
  const [tokenFor, setTokenFor] = useState<string | null>(null)
  const [tokenVal, setTokenVal] = useState('')

  function handleJoin(room: Room) {
    if (room.visibility === 'private') {
      setTokenFor(room.id)
      setTokenVal('')
    } else {
      onJoin(room.id)
    }
  }

  function submitToken(id: string) {
    onJoin(id, tokenVal.trim())
    setTokenFor(null)
    setTokenVal('')
  }

  return (
    <div className={s.wrap}>
      <div className={s.heading}>Room Directory</div>
      <div className={s.subheading}>Join a room to start chatting</div>
      <div className={s.grid}>
        {rooms.length === 0 && (
          <div className={s.empty}>No rooms yet — create one!</div>
        )}
        {rooms.map((r) => (
          <div key={r.id} className={s.card}>
            <div className={s.cardTop}>
              <div className={s.cardName}>{r.name}</div>
              <span className={`${s.badge} ${r.visibility === 'private' ? s.badgePrivate : s.badgePublic}`}>
                {r.visibility}
              </span>
            </div>
            {r.description && <div className={s.cardDesc}>{r.description}</div>}
            <div className={s.cardFooter}>
              <span className={s.onlineBadge}>
                <span>●</span> {r.online} online
              </span>
              <button className={s.joinBtn} onClick={() => handleJoin(r)}>
                Join
              </button>
            </div>
            {tokenFor === r.id && (
              <div className={s.tokenField}>
                <input
                  className={s.tokenInput}
                  placeholder="Invite token"
                  value={tokenVal}
                  onChange={(e) => setTokenVal(e.target.value)}
                  autoFocus
                  onKeyDown={(e) => e.key === 'Enter' && submitToken(r.id)}
                />
                <button className={s.joinBtn} onClick={() => submitToken(r.id)}>
                  Go
                </button>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
