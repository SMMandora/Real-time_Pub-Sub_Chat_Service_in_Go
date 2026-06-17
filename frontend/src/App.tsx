import { useState } from 'react'
import { useChat } from './useChat'
import { LoginGate } from './components/LoginGate'
import { AppShell } from './components/AppShell'

export default function App() {
  const [username, setUsername] = useState<string | null>(null)
  const chat = useChat(username)

  if (!username) {
    return <LoginGate onLogin={setUsername} />
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
    />
  )
}
