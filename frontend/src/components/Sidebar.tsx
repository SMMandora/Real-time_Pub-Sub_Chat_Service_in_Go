import type { Room } from '../types'
import type { Status } from '../useChat'
import s from './Sidebar.module.css'

interface Props {
  username: string
  status: Status
  rooms: Room[]
  activeRoom: string | null
  onJoin: (id: string) => void
  onCreateRoom: () => void
}

function statusDotClass(status: Status) {
  if (status === 'connected') return s.dotConnected
  if (status === 'reconnecting') return s.dotReconnecting
  if (status === 'offline') return s.dotOffline
  return s.dotIdle
}

export function Sidebar({ username, status, rooms, activeRoom, onJoin, onCreateRoom }: Props) {
  return (
    <div className={s.sidebar}>
      <div className={s.header}>
        <div className={s.logo}>⚡ Pulse</div>
        <div className={s.userBadge}>
          <span className={`${s.dot} ${statusDotClass(status)}`} />
          <span className={s.userName}>{username}</span>
        </div>
      </div>

      <div className={s.roomsSection}>
        <div className={s.sectionLabel}>Rooms</div>
        {rooms.length === 0 && (
          <div className={s.emptyRooms}>No rooms yet</div>
        )}
        {rooms.map((r) => (
          <button
            key={r.id}
            className={`${s.roomItem} ${activeRoom === r.id ? s.roomItemActive : ''}`}
            onClick={() => onJoin(r.id)}
          >
            <span className={s.roomSigil}>{r.visibility === 'private' ? '🔒' : '#'}</span>
            <span className={s.roomName}>{r.name}</span>
            {r.online > 0 && <span className={s.roomOnline}>{r.online}</span>}
          </button>
        ))}
      </div>

      <div className={s.footer}>
        <button className={s.createBtn} onClick={onCreateRoom}>
          + Create room
        </button>
      </div>
    </div>
  )
}
