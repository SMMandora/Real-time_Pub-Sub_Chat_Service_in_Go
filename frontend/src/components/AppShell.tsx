import { useState } from 'react'
import type { Room, CreatedRoom } from '../types'
import type { Status } from '../useChat'
import { Sidebar } from './Sidebar'
import { Directory } from './Directory'
import { ChatView } from './ChatView'
import { MembersPanel } from './MembersPanel'
import { CreateRoomModal } from './CreateRoomModal'
import { ConnectionBanner } from './ConnectionBanner'
import { Toaster } from './Toaster'
import type { Member, ChatMessage } from '../types'
import s from './AppShell.module.css'

interface Props {
  username: string
  status: Status
  rooms: Room[]
  activeRoom: string | null
  messages: ChatMessage[]
  members: Member[]
  typing: string | null
  errors: string[]
  recovering: boolean
  onJoin: (id: string, token?: string) => void
  onLeave: () => void
  onSend: (text: string) => void
  onTyping: (room: string) => void
  onRefreshRooms: () => void
  onDismissError: (i: number) => void
}

export function AppShell({
  username, status, rooms, activeRoom, messages, members,
  typing, errors, recovering,
  onJoin, onSend, onTyping, onRefreshRooms, onDismissError,
}: Props) {
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [membersOpen, setMembersOpen] = useState(false)

  function handleCreated(room: CreatedRoom) {
    onRefreshRooms()
    onJoin(room.id)
    setShowCreateModal(false)
  }

  const activeRoomObj = activeRoom ? rooms.find((r) => r.id === activeRoom) : undefined
  const isMobile = typeof window !== 'undefined' && window.innerWidth <= 720

  function closeDrawers() {
    setSidebarOpen(false)
    setMembersOpen(false)
  }

  return (
    <div className={s.shell}>
      {/* Mobile toolbar */}
      <div className={s.mobileToolbar}>
        <button className={s.mobileToggleBtn} onClick={() => setSidebarOpen(v => !v)}>☰</button>
        <span style={{ flex: 1, textAlign: 'center', fontWeight: 600, color: 'var(--text)' }}>
          {activeRoom ? `# ${activeRoomObj?.name ?? activeRoom}` : 'Pulse'}
        </span>
        {activeRoom && (
          <button className={s.mobileToggleBtn} onClick={() => setMembersOpen(v => !v)}>👥</button>
        )}
      </div>

      {/* Mobile overlay */}
      {(sidebarOpen || membersOpen) && (
        <div className={s.overlay} onClick={closeDrawers} />
      )}

      {/* Sidebar */}
      <div className={`${s.sidebar ?? ''} ${sidebarOpen ? s.sidebarOpen ?? '' : ''}`}>
        <Sidebar
          username={username}
          status={status}
          rooms={rooms}
          activeRoom={activeRoom}
          onJoin={(id) => { onJoin(id); closeDrawers() }}
          onCreateRoom={() => { setShowCreateModal(true); closeDrawers() }}
        />
      </div>

      {/* Main area */}
      <div className={s.main}>
        <div className={s.mainContent}>
          {!activeRoom && (
            <Directory rooms={rooms} onJoin={(id, token) => onJoin(id, token)} />
          )}
          {activeRoom && (
            <ChatView
              room={activeRoomObj}
              messages={messages}
              typing={typing}
              onSend={onSend}
              onTyping={onTyping}
              onToggleMembers={() => setMembersOpen(v => !v)}
              showMembersToggle={isMobile}
              activeRoom={activeRoom}
            />
          )}
        </div>

        {/* Members panel */}
        {activeRoom && (
          <div className={`${s.membersPanel ?? ''} ${membersOpen ? s.membersPanelOpen ?? '' : ''}`}>
            <MembersPanel members={members} />
          </div>
        )}
      </div>

      {/* Overlays */}
      <ConnectionBanner status={status} recovering={recovering} activeRoom={activeRoom} />
      <Toaster errors={errors} onDismiss={onDismissError} />

      {showCreateModal && (
        <CreateRoomModal
          onClose={() => setShowCreateModal(false)}
          onCreated={handleCreated}
        />
      )}
    </div>
  )
}
