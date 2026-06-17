import { useState } from 'react'
import { createRoom } from '../api'
import type { CreatedRoom } from '../types'
import s from './CreateRoomModal.module.css'

interface Props {
  onClose: () => void
  onCreated: (room: CreatedRoom) => void
}

export function CreateRoomModal({ onClose, onCreated }: Props) {
  const [id, setId] = useState('')
  const [name, setName] = useState('')
  const [desc, setDesc] = useState('')
  const [visibility, setVisibility] = useState<'public' | 'private'>('public')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [created, setCreated] = useState<CreatedRoom | null>(null)
  const [copied, setCopied] = useState(false)

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    if (!id.trim() || !name.trim()) return
    setLoading(true)
    setError('')
    try {
      const room = await createRoom({ id: id.trim(), name: name.trim(), description: desc.trim(), visibility })
      setCreated(room)
      onCreated(room)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to create room')
    } finally {
      setLoading(false)
    }
  }

  function copyToken() {
    if (created?.invite_token) {
      navigator.clipboard.writeText(created.invite_token).then(() => {
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      })
    }
  }

  return (
    <div className={s.overlay} onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className={s.modal}>
        <div className={s.titleRow}>
          <div className={s.title}>Create Room</div>
          <button className={s.close} onClick={onClose}>✕</button>
        </div>

        {error && <div className={s.error}>{error}</div>}

        {created?.invite_token && (
          <div className={s.tokenBox}>
            <div className={s.tokenLabel}>Invite Token</div>
            <div className={s.tokenRow}>
              <span className={s.tokenValue}>{created.invite_token}</span>
              <button className={s.copyBtn} onClick={copyToken}>
                {copied ? 'Copied!' : 'Copy'}
              </button>
            </div>
          </div>
        )}

        {!created && (
          <form onSubmit={submit}>
            <div className={s.field}>
              <label className={s.label} htmlFor="room-id">Room ID</label>
              <input id="room-id" className={s.input} value={id}
                onChange={(e) => setId(e.target.value)} placeholder="e.g. general" autoFocus />
            </div>
            <div className={s.field}>
              <label className={s.label} htmlFor="room-name">Display Name</label>
              <input id="room-name" className={s.input} value={name}
                onChange={(e) => setName(e.target.value)} placeholder="e.g. General" />
            </div>
            <div className={s.field}>
              <label className={s.label} htmlFor="room-desc">Description</label>
              <textarea id="room-desc" className={s.textarea} value={desc}
                onChange={(e) => setDesc(e.target.value)} placeholder="What's this room for?" />
            </div>
            <div className={s.field}>
              <label className={s.label}>Visibility</label>
              <div className={s.toggleRow}>
                <button type="button"
                  className={`${s.toggleBtn} ${visibility === 'public' ? s.toggleBtnActive : ''}`}
                  onClick={() => setVisibility('public')}>Public</button>
                <button type="button"
                  className={`${s.toggleBtn} ${visibility === 'private' ? s.toggleBtnActive : ''}`}
                  onClick={() => setVisibility('private')}>Private</button>
              </div>
            </div>
            <div className={s.actions}>
              <button type="button" className={s.cancelBtn} onClick={onClose}>Cancel</button>
              <button type="submit" className={s.submitBtn}
                disabled={loading || !id.trim() || !name.trim()}>
                {loading ? 'Creating…' : 'Create'}
              </button>
            </div>
          </form>
        )}

        {created && (
          <div className={s.actions}>
            <button className={s.submitBtn} onClick={onClose}>Done</button>
          </div>
        )}
      </div>
    </div>
  )
}
