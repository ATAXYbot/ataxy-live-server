const WebSocket = require('ws');
const http = require('http');
const express = require('express');
const { ExpressPeerServer } = require('peer');

const app = express();
const server = http.createServer(app);
const peerServer = ExpressPeerServer(server, { debug: false, path: '/' });
app.use('/peerjs', peerServer);

app.get('/', (_req, res) => {
  res.json({ ok: true, service: 'ataxy-live-server', protocol: 'voice-room-signaling-v2' });
});

const wss = new WebSocket.Server({ noServer: true });
const rooms = new Map();
const MAX_SEEN_MESSAGES = 2000;

server.on('upgrade', (request, socket, head) => {
  // PeerJS owns this upgrade path. ExpressPeerServer handles it.
  if (request.url.startsWith('/peerjs')) return;
  wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request));
});

function send(ws, value) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(value));
}

function getRoom(roomId) {
  const id = String(roomId);
  if (!rooms.has(id)) rooms.set(id, new Map());
  return rooms.get(id);
}

function relay(room, senderId, message, targetId) {
  const encoded = JSON.stringify(message);
  if (targetId) {
    const recipient = room.get(String(targetId));
    if (recipient && recipient.ws.readyState === WebSocket.OPEN) recipient.ws.send(encoded);
    return;
  }
  for (const [uid, member] of room) {
    if (uid !== String(senderId) && member.ws.readyState === WebSocket.OPEN) member.ws.send(encoded);
  }
}

wss.on('connection', ws => {
  let currentRoom = null;
  let currentUserId = null;
  const seenMessages = new Set();
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch { return; }

    const payload = data.payload || data;
    const senderId = String(data.senderId || payload.senderId || currentUserId || 'user');
    const targetId = data.targetId || payload.targetId;
    const event = data.event || payload.event || data.type;
    const msgId = data.msgId || payload.msgId;

    // Every client transport may deliver the same message. Deduplicate it per socket.
    if (msgId) {
      if (seenMessages.has(msgId)) return;
      seenMessages.add(msgId);
      if (seenMessages.size > MAX_SEEN_MESSAGES) seenMessages.delete(seenMessages.values().next().value);
    }

    if (data.type === 'join') {
      const nextRoom = String(data.roomId || '');
      if (!nextRoom) return;
      currentRoom = nextRoom;
      currentUserId = senderId;
      ws.roomId = currentRoom;
      ws.userId = currentUserId;
      const room = getRoom(currentRoom);

      // Replace stale connections for the same user instead of broadcasting ghosts.
      const previous = room.get(currentUserId);
      if (previous && previous.ws !== ws) {
        try { previous.ws.close(1000, 'replaced'); } catch {}
      }
      room.set(currentUserId, { ws, state: data.userState || payload || { user_id: currentUserId } });

      const states = [...room.entries()]
        .filter(([uid]) => uid !== currentUserId)
        .map(([, member]) => member.state);
      send(ws, { type: 'presence_sync', state: states });

      relay(room, currentUserId, {
        type: 'peer_join', event: 'peer_join', roomId: currentRoom,
        senderId: currentUserId,
        payload: { ...(payload || {}), senderId: currentUserId }
      });
      return;
    }

    if (!currentRoom || !currentUserId || !rooms.has(currentRoom)) return;
    const room = rooms.get(currentRoom);
    const envelope = {
      type: data.type === 'ROOM_EVENT' || data.type === 'room_broadcast' ? 'ROOM_EVENT' : (data.type || 'broadcast'),
      event,
      roomId: currentRoom,
      senderId: currentUserId,
      ...(targetId ? { targetId: String(targetId) } : {}),
      ...(msgId ? { msgId } : {}),
      payload: payload || {}
    };

    if (data.type === 'update_state') {
      const member = room.get(currentUserId);
      if (member) member.state = data.userState || payload;
      return;
    }

    // Relay all signaling and room events, including peer_join/leave and SFU events.
    relay(room, currentUserId, envelope, targetId);
  });

  ws.on('close', () => {
    if (!currentRoom || !currentUserId || !rooms.has(currentRoom)) return;
    const room = rooms.get(currentRoom);
    const member = room.get(currentUserId);
    // Do not remove a newer replacement connection.
    if (member && member.ws !== ws) return;
    room.delete(currentUserId);
    relay(room, currentUserId, {
      type: 'peer_leave', event: 'peer_leave', roomId: currentRoom,
      senderId: currentUserId, payload: { senderId: currentUserId, user_id: currentUserId }
    });
    relay(room, currentUserId, {
      type: 'presence_leave', event: 'presence_leave', roomId: currentRoom,
      senderId: currentUserId, payload: { user_id: currentUserId }
    });
    if (room.size === 0) rooms.delete(currentRoom);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`ATAXY Live Server listening on port ${PORT}`));
