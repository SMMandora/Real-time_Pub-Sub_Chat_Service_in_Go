import { useState } from 'react'
import s from './LoginGate.module.css'

interface Props {
  onLogin: (username: string) => void
  error?: string | null
}

const RE = /^[A-Za-z0-9_-]{1,32}$/

export function LoginGate({ onLogin, error }: Props) {
  const [value, setValue] = useState('')
  const [localErr, setLocalErr] = useState('')

  function submit(e: React.FormEvent) {
    e.preventDefault()
    const v = value.trim()
    if (!RE.test(v)) {
      setLocalErr('Username must be 1–32 chars: letters, digits, _ or -')
      return
    }
    setLocalErr('')
    onLogin(v)
  }

  const displayErr = error || localErr

  return (
    <div className={s.wrap}>
      <div className={s.card}>
        <div className={s.logo}>⚡ Pulse</div>
        <div className={s.subtitle}>Real-time chat — enter a username to begin</div>
        <form onSubmit={submit}>
          <label className={s.label} htmlFor="username">Username</label>
          <input
            id="username"
            className={s.input}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder="e.g. alice"
            autoFocus
            autoComplete="off"
          />
          {displayErr && <div className={s.error}>{displayErr}</div>}
          <button className={s.btn} type="submit" disabled={!value.trim()}>
            Join
          </button>
        </form>
      </div>
    </div>
  )
}
