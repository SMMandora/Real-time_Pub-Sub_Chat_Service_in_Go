import type { Status } from '../useChat'
import s from './ConnectionBanner.module.css'

interface Props {
  status: Status
  recovering: boolean
  activeRoom: string | null
  lastSeenId?: number
}

export function ConnectionBanner({ status, recovering, activeRoom, lastSeenId }: Props) {
  const show = status === 'reconnecting' || status === 'offline' || recovering

  if (!show) return null

  return (
    <div className={s.banner}>
      {(status === 'reconnecting') && (
        <div className={s.toast + ' ' + s.reconnecting}>
          <span className={s.spinner} />
          Connection lost. Reconnecting…
        </div>
      )}
      {status === 'offline' && (
        <div className={s.toast + ' ' + s.offline}>
          Offline — check your connection
        </div>
      )}
      {recovering && activeRoom && (
        <div className={s.toast + ' ' + s.recovering}>
          <span className={s.spinner} />
          Recovering messages{lastSeenId != null ? ` since #${lastSeenId}` : ''}…
        </div>
      )}
    </div>
  )
}
