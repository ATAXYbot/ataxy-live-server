const WebSocket = require('ws');
const http = require('http');
const express = require('express');
const { ExpressPeerServer } = require('peer');

const app = express();
const server = http.createServer(app);

// Initialize PeerJS Server for WebRTC signaling
const peerServer = ExpressPeerServer(server, {
    debug: false,
    path: '/'
});

// Route PeerJS traffic to the /peerjs endpoint
app.use('/peerjs', peerServer);

// Default health check endpoint
app.get('/', (req, res) => {
    res.send('ATAXY Live WebSocket & PeerJS Server is running perfectly.');
});

// Voice Rooms WebSocket server with manual upgrade routing
const wss = new WebSocket.Server({ noServer: true });

// Traffic Cop: Manually route WebSocket upgrades 
server.on('upgrade', (request, socket, head) => {
    // If the request is for PeerJS, let ExpressPeerServer handle it silently
    if (request.url.startsWith('/peerjs')) {
        return;
    }

    // Otherwise, route it to our custom Voice Rooms WebSocket server
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

// Map of roomId -> Map of userId -> { ws, state }
const rooms = new Map();

wss.on('connection', (ws, req) => {
    let currentRoom = null;
    let currentUserId = null;
    ws.isAlive = true;

    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message.toString());
            const { type, roomId, senderId, targetId, event, payload, userState } = data;

            // Handle joining (supports both legacy userState and new voice room format)
            if (type === 'join') {
                currentRoom = String(roomId);
                currentUserId = String(senderId || (userState && userState.user_id) || "user");
                ws.userId = currentUserId;
                ws.roomId = currentRoom;

                if (!rooms.has(currentRoom)) {
                    rooms.set(currentRoom, new Map());
                }

                const room = rooms.get(currentRoom);
                room.set(currentUserId, { ws: ws, state: userState || { user_id: currentUserId } });
                console.log(`User ${currentUserId} joined room: ${currentRoom}`);

                // Legacy presence sync response
                const allStates = [];
                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId) allStates.push(clientData.state);
                }
                ws.send(JSON.stringify({ type: 'presence_sync', state: allStates }));

                // Broadcast join to other members
                const joinMsg = JSON.stringify({
                    type: 'peer_join',
                    event: 'peer_join',
                    roomId: currentRoom,
                    senderId: currentUserId,
                    payload: payload || userState || {}
                });

                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                        clientData.ws.send(joinMsg);
                        // Also send legacy presence_join for backwards compatibility
                        clientData.ws.send(JSON.stringify({
                            type: 'presence_join',
                            payload: userState || { user_id: currentUserId }
                        }));
                    }
                }
            } 
            else if (type === 'webrtc_signal' || type === 'signal') {
                if (!currentRoom || !rooms.has(currentRoom)) return;
                const room = rooms.get(currentRoom);

                if (targetId) {
                    const targetClient = room.get(String(targetId));
                    if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
                        targetClient.ws.send(JSON.stringify({
                            type: 'webrtc_signal',
                            event: event || 'webrtc_signal',
                            senderId: currentUserId || senderId,
                            targetId: String(targetId),
                            payload: payload || data
                        }));
                    }
                } else {
                    const signalMsg = JSON.stringify({
                        type: 'webrtc_signal',
                        event: event || 'webrtc_signal',
                        senderId: currentUserId || senderId,
                        payload: payload || data
                    });
                    for (let [uid, clientData] of room.entries()) {
                        if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                            clientData.ws.send(signalMsg);
                        }
                    }
                }
            }
            else if (type === 'room_broadcast' || type === 'ROOM_EVENT') {
                if (!currentRoom || !rooms.has(currentRoom)) return;
                const room = rooms.get(currentRoom);
                const broadcastMsg = JSON.stringify({
                    type: 'ROOM_EVENT',
                    event: event,
                    senderId: currentUserId || senderId,
                    payload: payload
                });

                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                        clientData.ws.send(broadcastMsg);
                    }
                }
            }
            else if (type === 'update_state') {
                if (currentRoom && rooms.has(currentRoom) && currentUserId) {
                    const room = rooms.get(currentRoom);
                    if (room.has(currentUserId)) room.get(currentUserId).state = userState;
                }
            }
            else if (type === 'broadcast') {
                if (currentRoom && rooms.has(currentRoom)) {
                    const room = rooms.get(currentRoom);
                    const bPayload = JSON.stringify({ type: 'broadcast', event: event || data.event, payload: payload || data.payload });
                    for (let [uid, clientData] of room.entries()) {
                        if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                            clientData.ws.send(bPayload);
                        }
                    }
                }
            }
        } catch (e) {
            console.error("Error parsing message", e);
        }
    });

    ws.on('close', () => {
        if (currentRoom && currentUserId && rooms.has(currentRoom)) {
            const room = rooms.get(currentRoom);
            room.delete(currentUserId);
            console.log(`User ${currentUserId} left room: ${currentRoom}`);

            const leavePayload = JSON.stringify({
                type: 'peer_leave',
                roomId: currentRoom,
                senderId: currentUserId,
                payload: { user_id: currentUserId }
            });

            for (let [uid, clientData] of room.entries()) {
                if (clientData.ws.readyState === WebSocket.OPEN) {
                    clientData.ws.send(leavePayload);
                    clientData.ws.send(JSON.stringify({ type: 'presence_leave', payload: { user_id: currentUserId } }));
                }
            }
            if (room.size === 0) rooms.delete(currentRoom);
        }
    });
});

// Heartbeat interval to keep connections alive
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`✅ ATAXY Server (WebSocket + PeerJS) listening on port ${PORT}`);
});
