import { useState } from 'react'
import { useChat } from './useChat'
import { LoginGate } from './components/LoginGate'
import { AppShell } from './components/AppShell'
import { AdminDashboard } from './admin/AdminDashboard'

export default function App() {
  const [username, setUsername] = useState<string | null>(null)
  const [view, setView] = useState<'chat' | 'admin'>('chat')
  const chat = useChat(username)

  if (!username) {
    return <LoginGate onLogin={setUsername} />
  }

  if (view === 'admin') {
    return <AdminDashboard onBack={() => setView('chat')} />
  }

  return (
    <AppShell
      username={username}
      status={chat.status}
      rooms={chat.rooms}
      activeRoom={chat.activeRoom}
      messages={chat.messages}
      members={chat.members}
      typing={chat.typing}
      errors={chat.errors}
      recovering={chat.recovering}
      onJoin={chat.join}
      onLeave={chat.leave}
      onSend={(text) => chat.activeRoom && chat.send(chat.activeRoom, text)}
      onTyping={chat.sendTyping}
      onRefreshRooms={chat.refreshRooms}
      onDismissError={chat.dismissError}
      onOpenAdmin={() => setView('admin')}
    />
  )
}
