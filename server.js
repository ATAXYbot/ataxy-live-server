const WebSocket = require('ws');
const http = require('http');
const express = require('express');
const { ExpressPeerServer } = require('peer');

const app = express();
const server = http.createServer(app);

// Initialize PeerJS Server for WebRTC signaling
const peerServer = ExpressPeerServer(server, {
    debug: true,
    path: '/'
});

// Route PeerJS traffic to the /peerjs endpoint
app.use('/peerjs', peerServer);

// Default health check endpoint
app.get('/', (req, res) => {
    res.send('ATAXY Live WebSocket & PeerJS Server is running perfectly.');
});

// Keep existing raw WebSocket logic for Advance Rooms
const wss = new WebSocket.Server({ server });

// Map of roomId -> Map of userId -> { ws, state }
const rooms = new Map();

wss.on('connection', (ws, req) => {
    let currentRoom = null;
    let currentUserId = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            // Handle joining and Presence Sync
            if (data.type === 'join') {
                currentRoom = data.roomId;
                currentUserId = String(data.userState.user_id);

                if (!rooms.has(currentRoom)) {
                    rooms.set(currentRoom, new Map());
                }

                const room = rooms.get(currentRoom);
                
                room.set(currentUserId, { ws: ws, state: data.userState });
                console.log(`User ${currentUserId} joined room: ${currentRoom}`);

                const allStates = [];
                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId) allStates.push(clientData.state);
                }
                
                ws.send(JSON.stringify({ type: 'presence_sync', state: allStates }));

                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                        clientData.ws.send(JSON.stringify({
                            type: 'presence_join',
                            payload: data.userState
                        }));
                    }
                }
            } 
            else if (data.type === 'update_state') {
                 if (currentRoom && rooms.has(currentRoom) && currentUserId) {
                     const room = rooms.get(currentRoom);
                     if (room.has(currentUserId)) room.get(currentUserId).state = data.userState;
                 }
            }
            else if (data.type === 'broadcast') {
                if (currentRoom && rooms.has(currentRoom)) {
                    const room = rooms.get(currentRoom);
                    const payload = JSON.stringify({ type: 'broadcast', event: data.event, payload: data.payload });
                    
                    for (let [uid, clientData] of room.entries()) {
                        if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                            clientData.ws.send(payload);
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

            const leavePayload = JSON.stringify({ type: 'presence_leave', payload: { user_id: currentUserId } });
            for (let [uid, clientData] of room.entries()) {
                if (clientData.ws.readyState === WebSocket.OPEN) {
                    clientData.ws.send(leavePayload);
                }
            }
            if (room.size === 0) rooms.delete(currentRoom);
        }
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`ATAXY Server (WebSocket + PeerJS) listening on port ${PORT}`);
});
