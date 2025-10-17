import { WebSocketServer,WebSocket } from "ws";
import { createClient } from "redis";
const publishClient = new createClient();
const subscribeClient = new createClient();

await publishClient.connect();
await subscribeClient.connect();

const wss = new WebSocketServer({port : 8080});
console.log("✅ WebSocket server running on ws://localhost:8080");
const subscriptions = {};
wss.on('connection',(userSocket)=>{
    const id = randomId();
    subscriptions[id] = {
        ws:userSocket,
        rooms:[]
    };
   console.log("👤 New user connected:", id);
})
function randomId() {
    return Math.random().toString(36).substring(2, 10);
}
