import type { Member } from '../types'
import s from './MembersPanel.module.css'

interface Props {
  members: Member[]
}

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

interface Group { label: string; status: Member['status'] }
const GROUPS: Group[] = [
  { label: 'Online', status: 'online' },
  { label: 'Away', status: 'away' },
  { label: 'Offline', status: 'offline' },
]

export function MembersPanel({ members }: Props) {
  return (
    <div className={s.panel}>
      <div className={s.panelTitle}>Members — {members.length}</div>
      {members.length === 0 && <div className={s.empty}>No members yet</div>}
      {GROUPS.map(({ label, status }) => {
        const group = members.filter((m) => m.status === status)
        if (group.length === 0) return null
        return (
          <div key={status} className={s.group}>
            <div className={s.groupLabel}>{label} — {group.length}</div>
            {group.map((m) => (
              <div key={m.username} className={s.member}>
                <div className={s.avatar} style={{ background: avatarColor(m.username) }}>
                  {initials(m.username)}
                </div>
                <span className={s.memberName}>{m.username}</span>
                <span className={`${s.statusDot} ${s[status]}`} />
              </div>
            ))}
          </div>
        )
      })}
    </div>
  )
}
