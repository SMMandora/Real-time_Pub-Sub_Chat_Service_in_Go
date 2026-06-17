# F5 — Admin Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Checkbox steps.

**Goal:** An in-app admin/observability dashboard (metric cards, trend charts, health table) polling the gateway's Prometheus proxy.

**Architecture:** `frontend/src/admin/` — a `metricsApi` over `/api/metrics/query[_range]`, a polling `AdminDashboard`, `MetricCard`/`TrendChart`/`HealthTable`; `App` gets a Chat↔Admin view toggle. `recharts` for charts.

**Commit on `main`, no push, no attribution.** Frontend gate: `cd frontend && npm run typecheck && npm test && npm run build`. Node/npm + disk available.

---

### Task 1: metricsApi + dependency + tests

- [ ] **Step 1: Add recharts** — `cd frontend && npm install recharts`.

- [ ] **Step 2: `frontend/src/admin/metricsApi.ts`**
```ts
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
```

- [ ] **Step 3: Test** — `frontend/src/admin/metricsApi.test.ts`: mock `fetch` to return a vector (`{data:{result:[{metric:{},value:[123,"42"]}]}}`) → `queryInstant` returns 42; a matrix (`values:[[1,"1"],[2,"2"]]`) → `queryRange` returns 2 points; an empty result → 0/[]; a non-ok response → 0/[]. Run `npm test`.

- [ ] **Step 4: Commit** — `git add frontend/src/admin frontend/package.json frontend/package-lock.json && git -c commit.gpgsign=false commit -m "Add admin metricsApi over the Prometheus proxy"`.

---

### Task 2: Dashboard components + nav

- [ ] **Step 1:** Create `frontend/src/admin/MetricCard.tsx` (label + rounded value via `Math.round`/`toFixed`; optional unit suffix), `TrendChart.tsx` (a `recharts` `ResponsiveContainer`+`LineChart` from `{t,v}[]`, dark-styled, no axes clutter), `HealthTable.tsx` (rows of job + Healthy/Down badge), and `AdminDashboard.tsx` that on mount and every 5s (a `setInterval`, cleared on unmount) loads:
  - cards: active connections `sum(chat_active_connections)`, messages/sec `sum(rate(chat_messages_total[1m]))`, fan-out p99 ms `histogram_quantile(0.99, sum(rate(chat_fanout_latency_seconds_bucket[5m])) by (le)) * 1000`, queue depth `sum(chat_persist_queue_depth)`, persisted/sec `sum(rate(chat_messages_persisted_total[1m]))`, gateway replicas `count(chat_active_connections)`.
  - charts (range 900s, step 15s): connections `sum(chat_active_connections)`, messages/sec `sum(rate(chat_messages_total[1m]))`, fan-out p99 ms (same expr as the card).
  - health: `queryHealth()`.
  Round all displayed numbers. Empty data → card shows `—`, chart shows empty. Style to match the reference admin screen (dark, metric cards grid, charts, table).

- [ ] **Step 2: Nav toggle** — in `App.tsx` (and the sidebar/header), add a Chat ↔ Admin switch: a `view` state (`'chat' | 'admin'`); when `admin`, render `AdminDashboard` in the main pane (the WebSocket/useChat stays connected). A sidebar link "Admin" sets `view='admin'`; a back/Chat link returns.

- [ ] **Step 3: Typecheck + render test** — add `frontend/src/admin/MetricCard.test.tsx` (renders label + value). Run:
```bash
cd frontend && npm run typecheck && npm test 2>&1 | tail -10
```
Expected: clean typecheck, all tests pass.

- [ ] **Step 4: Commit** — `git add frontend/src && git -c commit.gpgsign=false commit -m "Add admin dashboard view (cards, charts, health) with nav toggle"`.

---

### Task 3: Build + final verification

- [ ] **Step 1: Build into web/**

```bash
cd /h/Real-time_Pub-Sub_Chat_Service_in_Go/frontend
npm run build && ls ../web/assets
```
Expected: build succeeds; `web/assets` refreshed.

- [ ] **Step 2: Full gate**

```bash
cd /h/Real-time_Pub-Sub_Chat_Service_in_Go/frontend && npm run typecheck && npm test 2>&1 | tail -5
cd /h/Real-time_Pub-Sub_Chat_Service_in_Go && go build ./...
```
Expected: typecheck + tests pass; gateway builds.

- [ ] **Step 3: Commit the build**

```bash
git add web frontend
git -c commit.gpgsign=false commit -m "Build admin dashboard into web/"
```

---

## Self-Review Notes

- **Spec coverage:** metricsApi instant/range/health (Task 1), six cards + three charts + health table polling 5s (Task 2), nav toggle (Task 2), recharts dependency (Task 1), tests (Tasks 1-2), build into web/ (Task 3). Degrades to placeholders when Prometheus is down (promFetch swallows errors → 0/[]).
- **PromQL** matches the slice-6a metric names exactly (`chat_active_connections`, `chat_messages_total`, `chat_fanout_latency_seconds_bucket`, `chat_persist_queue_depth`, `chat_messages_persisted_total`).
- **Numbers rounded** before display (the design-system rule and general hygiene).
