interface PromResult { metric: Record<string, string>; value?: [number, string]; values?: [number, string][] }

async function promFetch(path: string): Promise<PromResult[]> {
  try {
    const r = await fetch(path)
    if (!r.ok) return []
    const j = await r.json()
    return j?.data?.result ?? []
  } catch {
    return []
  }
}

export async function queryInstant(promql: string): Promise<number> {
  const res = await promFetch(`/api/metrics/query?query=${encodeURIComponent(promql)}`)
  const v = res[0]?.value?.[1]
  return v != null && !Number.isNaN(Number(v)) ? Number(v) : 0
}

export async function queryRange(promql: string, rangeSec: number, stepSec: number): Promise<{ t: number; v: number }[]> {
  const end = Math.floor(Date.now() / 1000)
  const start = end - rangeSec
  const res = await promFetch(`/api/metrics/query_range?query=${encodeURIComponent(promql)}&start=${start}&end=${end}&step=${stepSec}`)
  const series = res[0]?.values ?? []
  return series.map(([t, v]) => ({ t: t * 1000, v: Number(v) || 0 }))
}

export interface ServiceHealth { job: string; up: boolean }

export async function queryHealth(): Promise<ServiceHealth[]> {
  const res = await promFetch(`/api/metrics/query?query=${encodeURIComponent('up')}`)
  return res.map((r) => ({ job: r.metric.job ?? r.metric.instance ?? 'unknown', up: r.value?.[1] === '1' }))
}
