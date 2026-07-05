const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ATAXY Live WebSocket Server is running perfectly.');
});

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
                
                // Add/Update the user in the room
                room.set(currentUserId, {
                    ws: ws,
                    state: data.userState
                });

                console.log(`User ${currentUserId} joined room: ${currentRoom}`);

                // Send the current room state (all other users) to the new user
                const allStates = [];
                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId) {
                        allStates.push(clientData.state);
                    }
                }
                
                ws.send(JSON.stringify({
                    type: 'presence_sync',
                    state: allStates
                }));

                // Broadcast to everyone else that this user joined
                for (let [uid, clientData] of room.entries()) {
                    if (uid !== currentUserId && clientData.ws.readyState === WebSocket.OPEN) {
                        clientData.ws.send(JSON.stringify({
                            type: 'presence_join',
                            payload: data.userState
                        }));
                    }
                }
            } 
            
            // Handle state updates (mute/unmute/seat change)
            else if (data.type === 'update_state') {
                 if (currentRoom && rooms.has(currentRoom) && currentUserId) {
                     const room = rooms.get(currentRoom);
                     if (room.has(currentUserId)) {
                         room.get(currentUserId).state = data.userState;
                     }
                 }
            }
            
            // Handle broadcasting chats, kicks, settings, etc.
            else if (data.type === 'broadcast') {
                if (currentRoom && rooms.has(currentRoom)) {
                    const room = rooms.get(currentRoom);
                    
                    const payload = JSON.stringify({
                        type: 'broadcast',
                        event: data.event,
                        payload: data.payload
                    });
                    
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

    // Automatically handle user disconnecting (Force close, internet drop)
    ws.on('close', () => {
        if (currentRoom && currentUserId && rooms.has(currentRoom)) {
            const room = rooms.get(currentRoom);
            room.delete(currentUserId);
            
            console.log(`User ${currentUserId} left room: ${currentRoom}`);

            // Tell everyone else in the room that this user left
            const leavePayload = JSON.stringify({
                type: 'presence_leave',
                payload: { user_id: currentUserId }
            });

            for (let [uid, clientData] of room.entries()) {
                if (clientData.ws.readyState === WebSocket.OPEN) {
                    clientData.ws.send(leavePayload);
                }
            }

            // Cleanup empty rooms to save memory
            if (room.size === 0) {
                rooms.delete(currentRoom);
            }
        }
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`ATAXY WebSocket server listening on port ${PORT}`);
});

