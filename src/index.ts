import { WebSocketServer, WebSocket } from "ws";
import { createClient } from "redis";

const publishClient = createClient();
const subscribeClient = createClient();

const subscriptions: Record<string, { ws: WebSocket; rooms: string[] }> = {};
const subscribedRooms = new Set<string>();

async function main() {
    await publishClient.connect();
    await subscribeClient.connect();

    const wss = new WebSocketServer({ port: 8080 });
    console.log("WebSocket server running on ws://localhost:8080");


    wss.on("connection", (userSocket: WebSocket) => {
        const id = randomId();
        subscriptions[id] = { ws: userSocket, rooms: [] };
        console.log("👤 New user connected:", id);

        userSocket.on("message", async (data) => {
            let parsedMessage: any;
            try {
                parsedMessage = JSON.parse(data.toString());
            } catch (err) {
                console.error("Invalid JSON from client:", err);
                return;
            }

            const type = parsedMessage.type;
            if (type === "SUBSCRIBE") {
                const roomId: string | undefined = parsedMessage.room || parsedMessage.roomId;
                if (!roomId) return;

                if (!subscriptions[id].rooms.includes(roomId)) {
                    subscriptions[id].rooms.push(roomId);
                }

                if (!subscribedRooms.has(roomId)) {
                    console.log("📡 Subscribing Redis to room:", roomId);
                    subscribedRooms.add(roomId);
                    await subscribeClient.subscribe(roomId, (message: string) => {
                        try {
                            const parsed = JSON.parse(message);
                            const publishedRoomId = parsed.roomId || parsed.room;
                            const publishedMessage = parsed.message;
                            Object.keys(subscriptions).forEach((userId) => {
                                const { ws, rooms } = subscriptions[userId];
                                if (rooms.includes(publishedRoomId) && ws.readyState === WebSocket.OPEN) {
                                    ws.send(JSON.stringify({ roomId: publishedRoomId, message: publishedMessage }));
                                }
                            });
                        } catch (err) {
                            console.error("Failed to handle pub/sub message:", err);
                        }
                    });
                }
                return;
            }

            if (type === "UNSUBSCRIBE") {
                const roomId: string | undefined = parsedMessage.room || parsedMessage.roomId;
                if (!roomId) return;

                subscriptions[id].rooms = subscriptions[id].rooms.filter((r) => r !== roomId);

                if (!isRoomSubscribedByAnyUser(roomId)) {
                    console.log("Unsubscribing Redis from room:", roomId);
                    subscribedRooms.delete(roomId);
                    await subscribeClient.unsubscribe(roomId);
                }
                return;
            }

            if (type === "sendMessage" || type === "PUBLISH") {
                const message = parsedMessage.message;
                const roomId: string | undefined = parsedMessage.room || parsedMessage.roomId;
                if (!roomId) return;

                await publishClient.publish(
                    roomId,
                    JSON.stringify({
                        roomId,
                        message,
                    })
                );
                return;
            }
        });

        userSocket.on("close", async () => {
            const userRooms = subscriptions[id]?.rooms ?? [];
            delete subscriptions[id];

            for (const roomId of userRooms) {
                if (!isRoomSubscribedByAnyUser(roomId)) {
                    subscribedRooms.delete(roomId);
                    console.log("Unsubscribing Redis from room (last disconnected):", roomId);
                    await subscribeClient.unsubscribe(roomId);
                }
            }

            console.log("👤 User disconnected:", id);
        });
    });
}

main().catch((err) => {
    console.error("Failed to start:", err);
    process.exit(1);
});

function randomId() {
    return Math.random().toString(36).substring(2, 10);
}

function isRoomSubscribedByAnyUser(roomId: string) {
    for (const { rooms } of Object.values(subscriptions)) {
        if (rooms.includes(roomId)) return true;
    }
    return false;
}