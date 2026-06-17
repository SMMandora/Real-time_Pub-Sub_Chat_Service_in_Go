import { describe, it, expect, vi, afterEach } from 'vitest'
import { queryInstant, queryRange, queryHealth } from './metricsApi'

function mockFetch(body: unknown, ok = true) {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
    ok,
    json: () => Promise.resolve(body),
  }))
}

afterEach(() => { vi.restoreAllMocks() })

describe('queryInstant', () => {
  it('returns the numeric value from a vector result', async () => {
    mockFetch({ data: { result: [{ metric: {}, value: [123, '42'] }] } })
    expect(await queryInstant('up')).toBe(42)
  })

  it('returns 0 for an empty result', async () => {
    mockFetch({ data: { result: [] } })
    expect(await queryInstant('up')).toBe(0)
  })

  it('returns 0 for a non-ok response', async () => {
    mockFetch({}, false)
    expect(await queryInstant('up')).toBe(0)
  })

  it('returns 0 when fetch throws', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('network')))
    expect(await queryInstant('up')).toBe(0)
  })
})

describe('queryRange', () => {
  it('returns points from a matrix result', async () => {
    mockFetch({ data: { result: [{ metric: {}, values: [[1, '1'], [2, '2']] }] } })
    const pts = await queryRange('up', 900, 15)
    expect(pts).toHaveLength(2)
    expect(pts[0]).toEqual({ t: 1000, v: 1 })
    expect(pts[1]).toEqual({ t: 2000, v: 2 })
  })

  it('returns [] for an empty result', async () => {
    mockFetch({ data: { result: [] } })
    expect(await queryRange('up', 900, 15)).toEqual([])
  })

  it('returns [] for a non-ok response', async () => {
    mockFetch({}, false)
    expect(await queryRange('up', 900, 15)).toEqual([])
  })
})

describe('queryHealth', () => {
  it('maps up=1 to up:true and up=0 to up:false', async () => {
    mockFetch({
      data: {
        result: [
          { metric: { job: 'gateway' }, value: [1, '1'] },
          { metric: { job: 'postgres' }, value: [1, '0'] },
        ],
      },
    })
    const health = await queryHealth()
    expect(health).toEqual([
      { job: 'gateway', up: true },
      { job: 'postgres', up: false },
    ])
  })

  it('falls back to instance label when job is missing', async () => {
    mockFetch({ data: { result: [{ metric: { instance: 'localhost:9090' }, value: [1, '1'] }] } })
    const health = await queryHealth()
    expect(health[0].job).toBe('localhost:9090')
  })
})
