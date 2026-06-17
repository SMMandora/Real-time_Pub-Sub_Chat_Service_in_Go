import s from './Toaster.module.css'

interface Props {
  errors: string[]
  onDismiss: (i: number) => void
}

export function Toaster({ errors, onDismiss }: Props) {
  if (errors.length === 0) return null
  return (
    <div className={s.container}>
      {errors.map((err, i) => (
        <div key={i} className={s.toast}>
          <span className={s.text}>{err}</span>
          <button className={s.dismiss} onClick={() => onDismiss(i)}>✕</button>
        </div>
      ))}
    </div>
  )
}
