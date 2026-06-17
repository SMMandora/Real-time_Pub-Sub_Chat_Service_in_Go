import { ResponsiveContainer, LineChart, Line, Tooltip, XAxis } from 'recharts'
import s from './TrendChart.module.css'

interface Props {
  title: string
  data: { t: number; v: number }[]
  unit?: string
}

function formatTime(ts: number) {
  const d = new Date(ts)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

export function TrendChart({ title, data, unit = '' }: Props) {
  return (
    <div className={s.chart}>
      <div className={s.title}>{title}</div>
      {data.length === 0 ? (
        <div className={s.empty}>No data</div>
      ) : (
        <ResponsiveContainer width="100%" height={100}>
          <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 4 }}>
            <XAxis
              dataKey="t"
              tickFormatter={formatTime}
              tick={{ fill: 'var(--text-muted)', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
            />
            <Tooltip
              formatter={(val) => [`${Number(val).toFixed(2)}${unit}`, '']}
              labelFormatter={(l) => formatTime(Number(l))}
              contentStyle={{
                background: 'var(--bg-modal)',
                border: '1px solid var(--border)',
                borderRadius: 'var(--radius)',
                fontSize: '12px',
                color: 'var(--text)',
              }}
              itemStyle={{ color: 'var(--accent)' }}
            />
            <Line
              type="monotone"
              dataKey="v"
              stroke="var(--accent)"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
