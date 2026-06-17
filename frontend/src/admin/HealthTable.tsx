import type { ServiceHealth } from './metricsApi'
import s from './HealthTable.module.css'

interface Props {
  services: ServiceHealth[]
}

export function HealthTable({ services }: Props) {
  return (
    <div className={s.container}>
      <div className={s.heading}>Service Health</div>
      {services.length === 0 ? (
        <div className={s.empty}>No services</div>
      ) : (
        <table className={s.table}>
          <thead>
            <tr>
              <th className={s.th}>Service</th>
              <th className={s.th}>Status</th>
            </tr>
          </thead>
          <tbody>
            {services.map((svc) => (
              <tr key={svc.job} className={s.row}>
                <td className={s.td}>{svc.job}</td>
                <td className={s.td}>
                  <span className={svc.up ? s.badgeUp : s.badgeDown}>
                    {svc.up ? 'Healthy' : 'Down'}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
