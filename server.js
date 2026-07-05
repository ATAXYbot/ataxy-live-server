const WebSocket = require('ws');
const http = require('http');

// Create a basic HTTP server for health checks (Render requires this)
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ATAXY Live WebSocket Server is running perfectly.');
});

const wss = new WebSocket.Server({ server });

// Store active rooms and their connected clients
const rooms = new Map();

wss.on('connection', (ws, req) => {
    let currentRoom = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            // Handle joining a specific voice room
            if (data.type === 'join') {
                currentRoom = data.roomId;
                if (!rooms.has(currentRoom)) {
                    rooms.set(currentRoom, new Set());
                }
                rooms.get(currentRoom).add(ws);
                console.log(`User joined room: ${currentRoom}`);
            } 
            
            // Handle broadcasting chats, seats, and settings to the room
            else if (data.type === 'broadcast') {
                if (currentRoom && rooms.has(currentRoom)) {
                    const roomClients = rooms.get(currentRoom);
                    
                    const payload = JSON.stringify({
                        event: data.event,
                        payload: data.payload
                    });
                    
                    // Send to everyone in the room except the sender
                    for (let client of roomClients) {
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(payload);
                        }
                    }
                }
            }
        } catch (e) {
            console.error("Error parsing message", e);
        }
    });

    // Handle user disconnecting
    ws.on('close', () => {
        if (currentRoom && rooms.has(currentRoom)) {
            rooms.get(currentRoom).delete(ws);
            // Cleanup empty rooms to save memory
            if (rooms.get(currentRoom).size === 0) {
                rooms.delete(currentRoom);
            }
        }
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`ATAXY WebSocket server listening on port ${PORT}`);
});

