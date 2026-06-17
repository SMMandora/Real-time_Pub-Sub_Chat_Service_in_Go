import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { MetricCard } from './MetricCard'

describe('MetricCard', () => {
  it('renders the label', () => {
    render(<MetricCard label="Active Connections" value={42} />)
    expect(screen.getByText('Active Connections')).toBeTruthy()
  })

  it('renders a rounded integer value', () => {
    render(<MetricCard label="Connections" value={123} />)
    expect(screen.getByText('123')).toBeTruthy()
  })

  it('renders value with unit suffix', () => {
    render(<MetricCard label="Latency" value={12.3} unit=" ms" decimals={1} />)
    expect(screen.getByText('12.3 ms')).toBeTruthy()
  })

  it('renders — when value is null', () => {
    render(<MetricCard label="Messages" value={null} />)
    expect(screen.getByText('—')).toBeTruthy()
  })

  it('renders value with decimals', () => {
    render(<MetricCard label="Rate" value={3.14} decimals={2} />)
    expect(screen.getByText('3.14')).toBeTruthy()
  })
})
