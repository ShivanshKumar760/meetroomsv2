import express from 'express';
import http from 'node:http';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { MongoClient } from 'mongodb';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 3000;
const POD_NAME = process.env.POD_NAME || os.hostname();

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/chat?directConnection=true';

const app = express();

const server = http.createServer(app);
const wss = new WebSocketServer({server,maxPayload: 64*1024});

app.use(express.static(path.join(__dirname,"public")));


// ---------------------------------------------------------------------------
// HOW THIS WORKS WITH MORE THAN ONE POD
//
// 1. Room STATE lives in MongoDB (`rooms` collection), not in a per-pod Map,
//    so any pod can answer "does this room exist / is it full?".
// 2. Message DELIVERY goes through MongoDB too: every chat/signal/peer event
//    is inserted into the `relay` collection. EVERY pod watches that
//    collection with a change stream and forwards each event to whichever of
//    ITS OWN local sockets belong to that room. A pod never needs to know
//    which pod the other person is connected to.
//
// A WebSocket still lives on exactly one pod. Video itself is peer-to-peer
// (or via TURN) and never passes through these pods.
// ---------------------------------------------------------------------------

let rooms; // MongoDB collection: { _id: roomCode, members: [clientId], createdAt }
let relay; // MongoDB collection: { roomId, from, payload (JSON string), createdAt }
let mongoClient;
let changeStream = null;
let ready = false;
let shuttingDown = false;

// roomId -> Set<ws> for sockets connected to THIS pod only
const localRooms = new Map();

const ROOM_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const sleep = (ms)=>new Promise((r)=>setTimeout(r,ms));

function generateRoomCode(length = 5) {
  let code = '';
  for (let i = 0; i < length; i++) {
    code += ROOM_CODE_ALPHABET[crypto.randomInt(ROOM_CODE_ALPHABET.length)];
  }
  return code;
}

function sendRaw(ws,str){
    if (ws.readyState === ws.OPEN) ws.send(str);
}
function send(ws,data){
    sendRaw(ws,JSON.stringify(data));
}

function addLocal(ws, roomId) {
  let set = localRooms.get(roomId);
  if (!set) localRooms.set(roomId, (set = new Set()));
  set.add(ws);
}
function removeLocal(ws, roomId) {
  const set = localRooms.get(roomId);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) localRooms.delete(roomId);
}

// ---- relay: publish to MongoDB, deliver from the change stream -------------
async function publish(roomId,fromClientId,obj){
    await relay.insertOne({
    roomId,
    from: fromClientId,
    payload: JSON.stringify(obj),
    createdAt: new Date(),
    });
}

function deliver(doc){
    if(!doc) return;
    const set = localRooms.get(doc.roomId);
    if(!set) return; //nobody from this room is connected to this pod
    for (const peer of set){
        if(peer.clientId !== doc.from) sendRaw(peer,doc.payload);
    }
}

// async function watchRelay() {
//     while(!shuttingDown){
//         try{
//             changeStream = relay.watch([{$match:{operationType:'insert'}}]);
//             const first = await changeStream.tryNext();
//             if(!first) deliver(first.fullDocument);
//             ready = true;
//             console.log(`[${POD_NAME}] watching relay change stream`);
//             for await (const change of changeStream) deliver(change.fullDocument);
//         }catch(err){
//             if(shutdownDown) break;
//             console.error(`[${POD_NAME}] change stream error:`, err.message);
//         }
//         ready = false;
//         if (!shuttingDown) await sleep(1000);
//     }
// }

async function watchRelay() {
  while (!shuttingDown) {
    try {
      changeStream = relay.watch([{ $match: { operationType: 'insert' } }]);
      const first = await changeStream.tryNext(); // forces the cursor to open
      if (first) deliver(first.fullDocument);
      ready = true;
      console.log(`[${POD_NAME}] watching relay change stream`);
      for await (const change of changeStream) deliver(change.fullDocument);
    } catch (err) {
      if (shuttingDown) break;
      console.error(`[${POD_NAME}] change stream error:`, err.message);
    }
    ready = false;
    if (!shuttingDown) await sleep(1000);
  }
}

async function leaveRoom(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;
  ws.roomId = null;
  removeLocal(ws, roomId);

  await rooms.updateOne({ _id: roomId }, { $pull: { members: ws.clientId } });
  await rooms.deleteOne({ _id: roomId, members: { $size: 0 } });
  await publish(roomId, ws.clientId, { type: 'peer_left' });
}


async function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'create_room': {
      if (ws.roomId) return send(ws, { type: 'error', message: 'Already in a room' });
      for (let attempt = 0; attempt < 5; attempt++) {
        const roomId = generateRoomCode();
        try {
          // _id is unique, so a collision fails atomically instead of racing.
          await rooms.insertOne({ _id: roomId, members: [ws.clientId], createdAt: new Date() });
        } catch (err) {
          if (err.code === 11000) continue; // duplicate code, try another
          throw err;
        }
        ws.roomId = roomId;
        addLocal(ws, roomId);
        return send(ws, { type: 'room_created', roomId });
      }
      throw new Error('Could not allocate a room code');
    }

    case 'join_room': {
      if (ws.roomId) return send(ws, { type: 'error', message: 'Already in a room' });
      const roomId = String(msg.roomId || '').toUpperCase().trim();

      // Atomic "join if fewer than 2 members": two people joining at the same
      // moment from different pods cannot both get the last seat.
      const joined = await rooms.findOneAndUpdate(
        { _id: roomId, $expr: { $lt: [{ $size: '$members' }, 2] } },
        { $addToSet: { members: ws.clientId } },
        { returnDocument: 'after' }
      );

      if (!joined) {
        const exists = await rooms.findOne({ _id: roomId }, { projection: { _id: 1 } });
        return send(ws, {
          type: 'error',
          message: exists ? 'Room is full' : 'Room not found',
        });
      }

      ws.roomId = roomId;
      addLocal(ws, roomId);
      send(ws, { type: 'joined', roomId });
      await publish(roomId, ws.clientId, { type: 'peer_joined' });
      return;
    }

    case 'chat': {
      if (!ws.roomId) return;
      if (typeof msg.text !== 'string' || !msg.text.trim()) return;
      return publish(ws.roomId, ws.clientId, {
        type: 'chat',
        text: msg.text.trim().slice(0, 1000),
      });
    }

    // WebRTC signaling (SDP offer/answer, ICE candidates): relayed untouched.
    case 'signal': {
      if (!ws.roomId) return;
      if (typeof msg.data !== 'object' || msg.data === null) return;
      return publish(ws.roomId, ws.clientId, { type: 'signal', data: msg.data });
    }

    default:
      return;
  }
}

wss.on('connection', (ws) => {
  if (!ready || shuttingDown) {
    ws.close(1013, 'Server not ready, try again'); // 1013 = try again later
    return;
  }

  ws.clientId = crypto.randomUUID();
  ws.roomId = null;
  ws.isAlive = true;
  // Handle each socket's messages strictly in order. Without this, an SDP
  // offer and the ICE candidates that follow it could hit MongoDB out of order.
  ws.queue = Promise.resolve();

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!msg || typeof msg !== 'object') return;

    ws.queue = ws.queue
      .then(() => handleMessage(ws, msg))
      .catch((err) => {
        console.error(`[${POD_NAME}] handler error:`, err.message);
        send(ws, { type: 'error', message: 'Server error, please try again' });
      });
  });

  ws.on('close', () => {
    ws.queue = ws.queue
      .then(() => leaveRoom(ws))
      .catch((err) => console.error(`[${POD_NAME}] leave error:`, err.message));
  });

  ws.on('error', (err) => console.error(`[${POD_NAME}] socket error:`, err.message));
});

// Drop dead connections, and keep idle sockets alive through the load balancer.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

// ---- health + TURN ------------------------------------------------------------
// Readiness: 503 until the change stream is open, and again during shutdown,
// so Kubernetes only routes traffic to pods that can actually relay messages.
app.get('/healthz',(req,res)=>{
    const ok = ready && !shuttingDown;
    res.status(ok ? 200:503).json({ ready: ok, pod: POD_NAME });
})

const CREDENTIAL_TTL_SECONDS = 86400;
const CACHE_MS = 6 * 60 * 60 * 1000;
const FALLBACK_ICE = [{ urls: 'stun:stun.l.google.com:19302' }];
let cachedIce = null;
let inflight = null;

async function fetchCloudflareIceServers() {
  const { CLOUDFLARE_TURN_KEY_ID, CLOUDFLARE_TURN_API_TOKEN } = process.env;
  if (!CLOUDFLARE_TURN_KEY_ID || !CLOUDFLARE_TURN_API_TOKEN) {
    throw new Error('CLOUDFLARE_TURN_KEY_ID / CLOUDFLARE_TURN_API_TOKEN not set');
  }
  const r = await fetch(
    `https://rtc.live.cloudflare.com/v1/turn/keys/${CLOUDFLARE_TURN_KEY_ID}/credentials/generate-ice-servers`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${CLOUDFLARE_TURN_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ttl: CREDENTIAL_TTL_SECONDS }),
    }
  );
  if (!r.ok) throw new Error('Cloudflare returned ' + r.status);
  const data = await r.json();
  if (!Array.isArray(data.iceServers) || data.iceServers.length === 0) {
    throw new Error('Cloudflare response had no iceServers');
  }
  return data.iceServers;
}

async function getIceServers() {
  if (cachedIce && cachedIce.expiresAt > Date.now()) return cachedIce.iceServers;
  if (!inflight) {
    inflight = fetchCloudflareIceServers()
      .then((iceServers) => {
        cachedIce = { iceServers, expiresAt: Date.now() + CACHE_MS };
        return iceServers;
      })
      .finally(() => {
        inflight = null;
      });
  }
  return inflight;
}

app.get('/ice-config', async (req, res) => {
  res.set('Cache-Control', 'no-store');
  try {
    res.json({ iceServers: await getIceServers() });
  } catch (err) {
    console.error('ICE config error:', err.message);
    res.json({ iceServers: FALLBACK_ICE });
  }
});


async function connectMongo() {
  while (!shuttingDown) {
    try {
      mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 5000 });
      await mongoClient.connect();
      return;
    } catch (err) {
      console.error(`[${POD_NAME}] MongoDB not reachable yet:`, err.message);
      await mongoClient?.close().catch(() => {});
      await sleep(3000);
    }
  }
}

async function start() {
  await connectMongo();
  if (shuttingDown) return;
  const db = mongoClient.db();
  rooms = db.collection('rooms');
  relay = db.collection('relay');

  // Safety nets: abandoned rooms expire after 12h (e.g. a pod crashed without
  // cleaning up); relay events only need to live for a moment.
  await rooms.createIndex({ createdAt: 1 }, { expireAfterSeconds: 12 * 3600 });
  await relay.createIndex({ createdAt: 1 }, { expireAfterSeconds: 60 });

  watchRelay(); // runs for the life of the process; sets `ready` when listening
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  ready = false;
  console.log(`[${POD_NAME}] ${signal}: shutting down`);
  clearInterval(heartbeat);
  server.close();

  // Free this pod's rooms so the people on other pods get "peer left" now,
  // instead of waiting for the TTL.
  await Promise.allSettled([...wss.clients].map((ws) => leaveRoom(ws)));
  for (const ws of wss.clients) ws.close(1001, 'Server shutting down');

  await changeStream?.close().catch(() => {});
  await mongoClient?.close().catch(() => {});
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Listen first so probes get a (503) answer while MongoDB is still coming up.
server.listen(PORT, () => {
  console.log(`[${POD_NAME}] chat + video server listening on port ${PORT}`);
  start().catch((err) => {
    console.error('Startup failed:', err);
    process.exit(1);
  });
});