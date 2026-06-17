import s from './MetricCard.module.css'

interface Props {
  label: string
  value: number | null
  unit?: string
  decimals?: number
}

export function MetricCard({ label, value, unit = '', decimals = 0 }: Props) {
  const display = value == null ? '—' : `${value.toFixed(decimals)}${unit}`

  return (
    <div className={s.card}>
      <div className={s.label}>{label}</div>
      <div className={s.value}>{display}</div>
    </div>
  )
}
