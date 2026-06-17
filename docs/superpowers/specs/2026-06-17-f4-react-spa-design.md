# Design — F4: React SPA

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Frontend program (slice F4 of F1–F5)

## Context

Replace the throwaway `web/index.html` with a polished React 18 + Vite +
TypeScript single-page app matching the reference design (dark theme, blue
accent): a room directory, a 3-pane live chat (rooms · messages · members),
a room-creation modal, and robust connection-recovery UX. Mobile responsive.
The admin dashboard is a separate slice (F5).

## Visual brief (match the reference)

- **Dark-first** theme: deep navy/slate backgrounds, light text, a **blue accent**
  (primary actions, active room, links). A light theme is out of scope for v1 —
  ship dark only.
- Flat, modern, generous spacing; rounded cards; subtle 1px borders; status dots
  (green online / amber away / gray offline). Avatars are initials circles.
- Layouts from the reference: **Room Directory** card grid (name, description,
  public/private badge, "● N online", Join); **Live Chat** = left room list,
  center messages (avatars, names, timestamps, date dividers, typing line,
  composer with send + char counter), right **Members** panel grouped
  Online/Away/Offline with status badges; **Room Creation Modal** (name,
  description, public/private toggle, generated invite token); **connection
  recovery** (status pill + "Reconnecting…" / "Recovering messages since
  last_seen_id…" toast with retry).
- Sparklines and message reactions/edits from the reference are **out of scope**
  (no backing data / explicit non-goals).

## Stack & build

- **React 18 + Vite + TypeScript**, **CSS modules** + a `theme.css` of CSS
  variables (colors, spacing, radius) for the dark palette.
- Source in `frontend/`. `vite.config.ts`: `build.outDir = "../web"`,
  `build.emptyOutDir = true`; dev server proxy: `/ws` (ws: true) and `/api` →
  `http://localhost:8080`.
- The gateway serves the built SPA from `web/` (its existing static handler);
  the SPA is a single `index.html` + hashed assets. The chi catch-all already
  serves `web/`, but a deep-link refresh needs SPA fallback — **the build emits a
  single-page app; unknown non-`/api`/`/ws` paths should serve `index.html`.**
  Since the demo navigates client-side without deep links, the existing static
  handler is sufficient for v1 (root serves `index.html`).

## API contract (already built)

- **WebSocket** `GET /ws?username=<name>` (username `^[A-Za-z0-9_-]{1,32}$`, else
  the upgrade is 400 → re-prompt). Client→server frames:
  `{"type":"join","room":R,"token":T?,"id":lastSeenId?}`,
  `{"type":"send","room":R,"text":S}`, `{"type":"leave","room":R}`,
  `{"type":"typing","room":R}`. Server→client frames:
  `message {id,room,from,text,ts}`, `system {room,event,from}`,
  `presence {room,members:[username]}`, `typing {room,from}`,
  `error {message}`.
- **REST:** `GET /api/rooms` → `{rooms:[{id,name,description,visibility,online}]}`;
  `POST /api/rooms {id,name,description,visibility}` → `{id,name,description,visibility,online,invite_token?}`
  (409 dup, 400 bad visibility); `GET /api/rooms/{id}`; `DELETE /api/rooms/{id}`;
  `GET /api/rooms/{room}/members` → `{members:[{username,status}]}` (status ∈
  online/away/offline); `GET /api/rooms/{room}/messages?limit=&before=` →
  `{messages:[{id,from,text,ts}]}` (for "load older").

## Component tree

```
main.tsx → App
  LoginGate              (username entry; sets username, mounts AppShell)
  AppShell
    Sidebar
      Logo + UserBadge (username, connection dot)
      RoomList         (compact list of rooms; active highlight)
      "Create room" → CreateRoomModal
    Main
      Directory        (card grid, shown when no room is open; Join cards)
      ChatView         (shown when a room is open)
        RoomHeader     (name, online count, members toggle on mobile)
        MessageList    (DateDivider, MessageItem, SystemLine)
        TypingIndicator
        Composer       (input + char counter + send)
      MembersPanel     (Online/Away/Offline groups, status dots)
    ConnectionBanner   (reconnect / recovering-since toast)
    Toaster            (error frames: rate limit, invalid token, dup room)
```

## State & data flow — `useChat` hook (the core)

A single hook owns the WebSocket and chat state (a reducer is fine):

- **connect(username):** open `ws://host/ws?username=`; on open → status
  `connected`; load rooms (`GET /api/rooms`). On `onmessage`, dispatch by frame
  type.
- **messages:** per-room list; `message` frames appended, **deduped by `id`**;
  track `lastSeenId` per room (max id seen) for reconnect replay.
- **presence:** `presence` frame replaces the room's member-username list (drives
  the header online count and a quick presence view); the full roster (with
  away/offline) comes from `GET /api/rooms/{room}/members`, refetched on join,
  on each `presence` frame, and on a ~20s interval (for time-based
  away→offline).
- **typing:** `typing` frame sets `<user> is typing` for that room, auto-cleared
  ~3s after the last typing frame; the local composer emits a throttled `typing`
  frame (≤ every 1.5s).
- **join(room, token?):** send `join` with `token` and the room's `lastSeenId` as
  `id` (replay-since); history replays as `message` frames (deduped). Switch the
  active room.
- **send(room, text):** send `send`.
- **reconnect:** on `onclose`, status `reconnecting`, exponential backoff
  (e.g. 1s→2s→…→max 15s); on reconnect, re-`join` the active room with its
  `lastSeenId` (the recovery toast shows "Recovering messages since
  last_seen_id…", clearing when the first replayed/live message arrives). Status
  pill reflects connecting/connected/reconnecting/offline.
- **errors:** `error` frames push a toast (rate limit, invalid invite token);
  a 400 on the WS upgrade (bad username) bounces back to the LoginGate.

`api.ts` wraps the REST calls (typed). `types.ts` mirrors the frames and REST
shapes.

## Error handling

- Bad username → WS 400 → re-prompt.
- `error` frames → toast; the connection stays open.
- Disconnect → status pill + auto-reconnect + replay-since on reconnect.
- Private room join with a wrong token → `error` toast; room not entered.

## Testing

- **Vitest + @testing-library/react** (jsdom, runs headless): unit-test
  `useChat` against a **mock WebSocket** — join, message dedup by id, presence
  replace, typing auto-clear, reconnect path re-joins with `lastSeenId`; plus a
  render test for `MessageList` and `RoomList`. A mock `fetch` for `api.ts`.
- **Build gate:** `tsc --noEmit` typecheck + `vite build` (outputs to `web/`)
  must succeed.

## Dockerfile

Add a **Node build stage** before the Go builder: `node:22` image, `npm ci` in
`frontend/`, `npm run build` → `web/`; the `gateway` image then copies the
populated `web/`. (The Go builder stays; only the gateway target gains the built
assets.)

## Acceptance criteria

- `npm install && npm run build` in `frontend/` produces the SPA in `web/`;
  `tsc --noEmit` passes; `npm test` (Vitest) passes.
- The app: enter a username → see the room directory (real rooms + online
  counts) → create a room (public or private, private shows its token) → join
  (token required for private) → chat with live messages, history replay,
  typing, a members panel (online/away/offline), and a connection-recovery
  banner on disconnect.
- Dark theme matching the reference; mobile responsive (sidebar/members collapse).
