// k6 WebSocket load test for the chat gateway.
//
// Uses the built-in `k6/experimental/websockets` module (the maintained
// successor to xk6-websockets — no custom k6 build needed). Each VU opens a
// connection, joins a room, and sends a message every few seconds; the round
// trip of its own message back through Redis fan-out is recorded as
// `fanout_latency_ms`, with a p99 < 50ms threshold (the spec's SLO).
//
// Run:
//   WS_URL=ws://<gateway>/ws k6 run loadtest/ws-load.js
//   # smaller smoke run:
//   VUS=200 PEAK=500 k6 run loadtest/ws-load.js

import { WebSocket } from 'k6/experimental/websockets';
import { setTimeout, setInterval, clearInterval } from 'k6/experimental/timers';
import { Trend, Counter } from 'k6/metrics';

const fanoutLatency = new Trend('fanout_latency_ms', true);
const wsErrors = new Counter('ws_errors');

const WS_URL = __ENV.WS_URL || 'ws://localhost:8080/ws';
const ROOM = __ENV.ROOM || 'loadtest';
const HOLD_MS = Number(__ENV.HOLD_MS || 30000);     // how long each VU stays connected
const SEND_EVERY_MS = Number(__ENV.SEND_EVERY_MS || 5000);

// Ramp to 10k concurrent connections by default; override with PEAK for smaller runs.
const PEAK = Number(__ENV.PEAK || 10000);

export const options = {
  scenarios: {
    chat: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '1m', target: Math.ceil(PEAK / 10) },
        { duration: '3m', target: PEAK },
        { duration: '2m', target: PEAK },
        { duration: '1m', target: 0 },
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    fanout_latency_ms: ['p(99)<50'],
    ws_errors: ['count<1'],
  },
};

export default function () {
  const user = `u${__VU}_${__ITER}`;
  const ws = new WebSocket(`${WS_URL}?username=${encodeURIComponent(user)}`);
  let sentAt = 0;
  let ticker;

  ws.onopen = () => {
    ws.send(JSON.stringify({ type: 'join', room: ROOM }));
    ticker = setInterval(() => {
      sentAt = Date.now();
      ws.send(JSON.stringify({ type: 'send', room: ROOM, text: 'ping' }));
    }, SEND_EVERY_MS);
    setTimeout(() => {
      if (ticker) clearInterval(ticker);
      ws.close();
    }, HOLD_MS);
  };

  ws.onmessage = (e) => {
    const f = JSON.parse(e.data);
    // Record the round trip of this VU's own message returning via fan-out.
    if (f.type === 'message' && f.from === user && sentAt > 0) {
      fanoutLatency.add(Date.now() - sentAt);
      sentAt = 0;
    }
  };

  ws.onerror = () => {
    wsErrors.add(1);
    if (ticker) clearInterval(ticker);
  };
}
