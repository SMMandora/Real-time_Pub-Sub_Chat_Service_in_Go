import { useEffect, useState } from 'react'
import { queryInstant, queryRange, queryHealth } from './metricsApi'
import type { ServiceHealth } from './metricsApi'
import { MetricCard } from './MetricCard'
import { TrendChart } from './TrendChart'
import { HealthTable } from './HealthTable'
import s from './AdminDashboard.module.css'

const CONNECTIONS_Q = 'sum(chat_active_connections)'
const MESSAGES_RATE_Q = 'sum(rate(chat_messages_total[1m]))'
const FANOUT_P99_Q = 'histogram_quantile(0.99, sum(rate(chat_fanout_latency_seconds_bucket[5m])) by (le)) * 1000'
const QUEUE_DEPTH_Q = 'sum(chat_persist_queue_depth)'
const PERSISTED_RATE_Q = 'sum(rate(chat_messages_persisted_total[1m]))'
const REPLICAS_Q = 'count(chat_active_connections)'

interface DashState {
  connections: number | null
  messagesRate: number | null
  fanoutP99: number | null
  queueDepth: number | null
  persistedRate: number | null
  replicas: number | null
  connTrend: { t: number; v: number }[]
  msgTrend: { t: number; v: number }[]
  fanoutTrend: { t: number; v: number }[]
  health: ServiceHealth[]
}

const EMPTY: DashState = {
  connections: null,
  messagesRate: null,
  fanoutP99: null,
  queueDepth: null,
  persistedRate: null,
  replicas: null,
  connTrend: [],
  msgTrend: [],
  fanoutTrend: [],
  health: [],
}

function round(v: number, decimals = 0): number {
  const factor = Math.pow(10, decimals)
  return Math.round(v * factor) / factor
}

async function fetchAll(): Promise<DashState> {
  const [
    connections, messagesRate, fanoutP99, queueDepth, persistedRate, replicas,
    connTrend, msgTrend, fanoutTrend, health,
  ] = await Promise.all([
    queryInstant(CONNECTIONS_Q),
    queryInstant(MESSAGES_RATE_Q),
    queryInstant(FANOUT_P99_Q),
    queryInstant(QUEUE_DEPTH_Q),
    queryInstant(PERSISTED_RATE_Q),
    queryInstant(REPLICAS_Q),
    queryRange(CONNECTIONS_Q, 900, 15),
    queryRange(MESSAGES_RATE_Q, 900, 15),
    queryRange(FANOUT_P99_Q, 900, 15),
    queryHealth(),
  ])

  return {
    connections: round(connections),
    messagesRate: round(messagesRate, 2),
    fanoutP99: round(fanoutP99, 1),
    queueDepth: round(queueDepth),
    persistedRate: round(persistedRate, 2),
    replicas: round(replicas),
    connTrend: connTrend.map((p) => ({ t: p.t, v: round(p.v) })),
    msgTrend: msgTrend.map((p) => ({ t: p.t, v: round(p.v, 2) })),
    fanoutTrend: fanoutTrend.map((p) => ({ t: p.t, v: round(p.v, 1) })),
    health,
  }
}

interface Props {
  onBack: () => void
}

export function AdminDashboard({ onBack }: Props) {
  const [data, setData] = useState<DashState>(EMPTY)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  useEffect(() => {
    let cancelled = false

    async function load() {
      const result = await fetchAll()
      if (!cancelled) {
        setData(result)
        setLastUpdated(new Date())
      }
    }

    load()
    const id = setInterval(load, 5000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  const zeroToNull = (v: number | null) => (v === 0 ? null : v)

  return (
    <div className={s.dashboard}>
      <div className={s.header}>
        <div className={s.headerLeft}>
          <button className={s.backBtn} onClick={onBack}>← Chat</button>
          <h1 className={s.title}>Admin Dashboard</h1>
        </div>
        {lastUpdated && (
          <div className={s.updated}>
            Updated {lastUpdated.toLocaleTimeString()}
          </div>
        )}
      </div>

      <div className={s.body}>
        {/* Metric cards */}
        <section>
          <div className={s.sectionLabel}>Live Metrics</div>
          <div className={s.cardsGrid}>
            <MetricCard label="Active Connections" value={data.connections} />
            <MetricCard label="Messages / sec" value={zeroToNull(data.messagesRate)} decimals={2} />
            <MetricCard label="Fan-out p99" value={zeroToNull(data.fanoutP99)} unit=" ms" decimals={1} />
            <MetricCard label="Queue Depth" value={data.queueDepth} />
            <MetricCard label="Persisted / sec" value={zeroToNull(data.persistedRate)} decimals={2} />
            <MetricCard label="Gateway Replicas" value={data.replicas} />
          </div>
        </section>

        {/* Trend charts */}
        <section>
          <div className={s.sectionLabel}>Trends (last 15 min)</div>
          <div className={s.chartsGrid}>
            <TrendChart title="Active Connections" data={data.connTrend} />
            <TrendChart title="Messages / sec" data={data.msgTrend} />
            <TrendChart title="Fan-out p99 (ms)" data={data.fanoutTrend} unit=" ms" />
          </div>
        </section>

        {/* Health table */}
        <section>
          <HealthTable services={data.health} />
        </section>
      </div>
    </div>
  )
}
