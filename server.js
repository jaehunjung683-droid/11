/* IRON VEIL TACTICAL FRONT V2.0 WebSocket server
 * Node.js 20+ / ws
 * Basic multiplayer relay and room/match lifecycle for the V2.0 HTML client.
 */
'use strict';

const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 3000);
const MAX_PLAYERS = Math.max(2, Math.min(20, Number(process.env.MAX_PLAYERS || 20)));
const MATCH_DURATION_MS = Math.max(60, Number(process.env.MATCH_DURATION_SECONDS || 300)) * 1000;
const ROUND_READY_MS = 15000;
const TICK_MS = 100;
const MAPS = ['map1','map2','map3','map4','map5','map6','map7','map8'];
const CHANNELS = ['1','2','3'];
const TEAMS = ['RED','BLUE','GREEN','GOLD'];

const rooms = new Map();
const clients = new Map();

const server = http.createServer((req, res) => {
  const path = new URL(req.url, 'http://localhost').pathname;
  if (path === '/' || path === '/health') {
    const body = JSON.stringify({
      ok: true,
      service: 'IRON VEIL TACTICAL FRONT V2.0 WebSocket Server',
      clients: clients.size,
      rooms: rooms.size,
      maxPlayers: MAX_PLAYERS,
      time: new Date().toISOString()
    }, null, 2);
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(body);
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocket.Server({ server, maxPayload: 32 * 1024 });
const now = () => Date.now();
const randId = (n = 8) => crypto.randomBytes(n).toString('hex');
const safeName = (s) => String(s || '').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 18);
const num = (v, fallback = 0, min = -100000, max = 100000) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
};
const send = (ws, data) => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (_) {}
  }
};
const sendError = (ws, message) => send(ws, { type: 'error', message });
const members = (room) => [...room.players.values()];
const aliveCount = (room) => members(room).filter(p => p.alive).length;

function getRoomFor(ws) {
  const id = clients.get(ws);
  const p = id && findPlayer(id);
  return p ? rooms.get(p.roomCode) : null;
}
function findPlayer(id) {
  for (const room of rooms.values()) {
    const p = room.players.get(id);
    if (p) return p;
  }
  return null;
}
function publicPlayer(p) {
  return {
    id: p.id, name: p.name, x: p.x, y: p.y, z: p.z,
    yaw: p.yaw, pitch: p.pitch, weapon: p.weapon,
    team: p.team, alive: p.alive, kills: p.kills, deaths: p.deaths,
    ready: p.ready
  };
}
function leaderboard(room) {
  return members(room).slice().sort((a,b) => b.kills - a.kills).map(p => ({
    id: p.id, name: p.name, team: p.team, kills: p.kills, deaths: p.deaths
  }));
}
function broadcast(room, data, exceptId = null) {
  for (const p of room.players.values()) {
    if (p.id !== exceptId) send(p.ws, data);
  }
}
function statePacket(room) {
  return {
    type: 'state',
    roomCode: room.code,
    channelId: room.channelId,
    hostId: room.hostId,
    serverType: room.serverType,
    matchMode: room.matchMode,
    players: members(room).map(publicPlayer),
    scores: room.scores,
    leaderboard: leaderboard(room),
    matchStarted: room.matchStarted,
    roundState: room.roundState,
    endsAt: room.endsAt || 0,
    mapId: room.mapId
  };
}
function publishState(room) {
  broadcast(room, statePacket(room));
}
function sendLobby(room) {
  broadcast(room, {
    type: 'match_lobby',
    roomCode: room.code,
    roomName: room.name,
    channelId: room.channelId,
    hostId: room.hostId,
    players: members(room).map(publicPlayer),
    serverType: room.serverType,
    matchMode: room.matchMode,
    teamSize: room.teamSize,
    teamCount: room.teamCount,
    mapId: room.mapId,
    mapVotes: room.mapVotes,
    roundState: room.roundState
  });
}
function normalizeChannel(value) { return CHANNELS.includes(String(value)) ? String(value) : '1'; }
function roomSummary(room) {
  return { roomCode: room.code, name: room.name, channelId: room.channelId, matchMode: room.matchMode,
    players: room.players.size, maxPlayers: MAX_PLAYERS, hostName: room.players.get(room.hostId)?.name || '',
    status: room.matchStarted ? 'in_game' : 'waiting', joinable: !room.matchStarted && room.players.size < MAX_PLAYERS };
}
function listServers(ws) {
  const channels = CHANNELS.map(channelId => ({ channelId,
    players: [...rooms.values()].filter(r => r.channelId === channelId).reduce((n,r) => n + r.players.size, 0),
    rooms: [...rooms.values()].filter(r => r.channelId === channelId).length }));
  send(ws, { type: 'server_list', channels, rooms: [...rooms.values()].map(roomSummary) });
}
function makeRoom(channelId, matchMode, teamSize, teamCount, name) {
  let code;
  do { code = String(Math.floor(100000 + Math.random() * 900000)); } while (rooms.has(code));
  const room = { code, channelId, serverType: 'mixed', matchMode, teamSize, teamCount,
    name: String(name || '').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 32) || `방 ${code}`,
    players: new Map(), hostId: null, scores: { RED: 0, BLUE: 0, GREEN: 0, GOLD: 0 },
    matchStarted: false, roundState: 'lobby', endsAt: 0, mapId: 'map1', mapVotes: {}, weaponSelectAt: 0 };
  rooms.set(code, room);
  return room;
}
function assignTeam(room) {
  if (room.matchMode === 'ffa') return TEAMS[0];
  const counts = Object.fromEntries(TEAMS.map(t => [t, 0]));
  for (const p of room.players.values()) counts[p.team] = (counts[p.team] || 0) + 1;
  const teamLimit = room.matchMode === 'team' ? 2 : Math.max(2, room.teamCount);
  const allowed = TEAMS.slice(0, Math.min(4, teamLimit));
  return allowed.sort((a,b) => counts[a] - counts[b])[0] || 'RED';
}
function joinRoom(ws, msg) {
  const old = getRoomFor(ws);
  if (old) leaveRoom(ws);
  const channel = normalizeChannel(msg.channelId);
  const matchMode = msg.matchMode === 'team' ? 'team' : 'ffa';
  const teamSize = Math.max(1, Math.min(10, Number(msg.teamSize) || 1));
  const teamCount = Math.max(2, Math.min(4, Number(msg.teamCount) || 2));
  const room = [...rooms.values()].find(r => r.channelId === channel && r.matchMode === matchMode && !r.matchStarted && r.players.size < MAX_PLAYERS)
    || makeRoom(channel, matchMode, teamSize, teamCount, '');
  addPlayerToRoom(ws, room, msg);
}
function createRoom(ws, msg) {
  const name = safeName(msg.name);
  if (!name) return sendError(ws, 'Enter a nickname before joining.');
  const room = makeRoom(normalizeChannel(msg.channelId), msg.matchMode === 'team' ? 'team' : 'ffa',
    Math.max(1, Math.min(10, Number(msg.teamSize) || 1)), Math.max(2, Math.min(4, Number(msg.teamCount) || 2)), msg.roomName);
  addPlayerToRoom(ws, room, msg);
}
function joinSpecificRoom(ws, msg) {
  const room = rooms.get(String(msg.roomCode || '').trim());
  if (!room) return sendError(ws, 'Room not found. Refresh the room list.');
  addPlayerToRoom(ws, room, msg);
}
function quickMatch(ws, msg) {
  const channel = normalizeChannel(msg.channelId);
  const matchMode = msg.matchMode === 'team' ? 'team' : 'ffa';
  const room = [...rooms.values()].filter(r => r.channelId === channel && r.matchMode === matchMode && !r.matchStarted && r.players.size < MAX_PLAYERS)
    .sort((a,b) => b.players.size - a.players.size || a.code.localeCompare(b.code))[0];
  if (room) return addPlayerToRoom(ws, room, msg);
  return createRoom(ws, msg);
}
function addPlayerToRoom(ws, room, msg) {
  if (getRoomFor(ws)) leaveRoom(ws);
  if (!rooms.has(room.code)) rooms.set(room.code, room);
  if (room.players.size >= MAX_PLAYERS || room.matchStarted) return sendError(ws, 'This room is not accepting players.');
  const name = safeName(msg.name);
  if (!name) return sendError(ws, 'Enter a nickname before joining.');
  const id = randId(8);
  const p = { id, ws, roomCode: room.code, name, x: 0, y: 1.6, z: 0, yaw: 0, pitch: 0, weapon: 'rifle',
    team: assignTeam(room), alive: true, kills: 0, deaths: 0, ready: false, lastStateAt: 0, lastHitAt: 0, joinedAt: now() };
  room.players.set(id, p); clients.set(ws, id);
  if (!room.hostId) room.hostId = id;
  send(ws, { type: 'room_joined', playerId: id, roomCode: room.code, team: p.team, matchMode: room.matchMode, serverType: 'mixed', channelId: room.channelId, mapId: room.mapId });
  sendLobby(room); publishState(room); publishRoomLists();
}
function publishRoomLists() {
  for (const ws of wss.clients) if (ws.readyState === WebSocket.OPEN && !clients.has(ws)) listServers(ws);
}
function leaveRoom(ws) {
  const id = clients.get(ws);
  if (!id) return;
  clients.delete(ws);
  for (const room of rooms.values()) {
    if (!room.players.has(id)) continue;
    room.players.delete(id);
    delete room.mapVotes[id];
    if (room.hostId === id) room.hostId = members(room).sort((a,b) => a.joinedAt - b.joinedAt || a.id.localeCompare(b.id))[0]?.id || null;
    if (room.players.size === 0) rooms.delete(room.code);
    else {
      if (room.players.size < 2 && room.matchStarted) { room.matchStarted = false; room.roundState = 'lobby'; room.endsAt = 0; }
      sendLobby(room);
      publishState(room);
    }
    break;
  }
  publishRoomLists();
}
function startMatch(room) {
  if (!room || room.matchStarted || room.players.size === 0) return;
  room.matchStarted = true;
  room.roundState = 'weapon_select';
  room.endsAt = now() + MATCH_DURATION_MS;
  room.weaponSelectAt = now() + ROUND_READY_MS;
  const totals = Object.values(room.mapVotes).reduce((counts, mapId) => { counts[mapId] = (counts[mapId] || 0) + 1; return counts; }, {});
  const highest = Math.max(0, ...Object.values(totals));
  const tiedMaps = Object.keys(totals).filter(mapId => totals[mapId] === highest);
  room.mapId = highest ? tiedMaps[crypto.randomInt(tiedMaps.length)] : room.mapId;
  room.mapVotes = {};
  broadcast(room, { type: 'match_started', mapId: room.mapId, endsAt: room.endsAt });
  publishState(room);
}
function resetPlayer(p) {
  p.alive = true; p.x = 0; p.y = 1.6; p.z = 0;
}
function handleMessage(ws, msg) {
  if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;
  if (msg.type === 'list_servers') return listServers(ws);
  if (msg.type === 'join_server' || msg.type === 'quick_match') return quickMatch(ws, msg);
  if (msg.type === 'create_room') return createRoom(ws, msg);
  if (msg.type === 'join_room') return joinSpecificRoom(ws, msg);
  const id = clients.get(ws);
  const player = id && findPlayer(id);
  const room = player && rooms.get(player.roomCode);
  if (!player || !room) return sendError(ws, '먼저 서버 채널에 참가하세요.');

  switch (msg.type) {
    case 'player_state': {
      if (now() - player.lastStateAt < 25) return;
      player.lastStateAt = now();
      player.x = num(msg.x, player.x, -10000, 10000);
      player.y = num(msg.y, player.y, -10000, 10000);
      player.z = num(msg.z, player.z, -10000, 10000);
      player.yaw = num(msg.yaw, player.yaw, -1000, 1000);
      player.pitch = num(msg.pitch, player.pitch, -1000, 1000);
      player.weapon = String(msg.weapon || player.weapon).slice(0, 40);
      return;
    }
    case 'choose_weapon':
      player.weapon = String(msg.weapon || player.weapon).slice(0, 40);
      player.ready = false;
      return;
    case 'ready':
      player.ready = true;
      if (room.matchStarted && room.roundState === 'weapon_select') {
        const alive = members(room);
        if (alive.every(p => p.ready) || now() >= room.weaponSelectAt) {
          room.roundState = 'live';
          room.endsAt = now() + MATCH_DURATION_MS;
          broadcast(room, { type: 'round_started', endsAt: room.endsAt });
          publishState(room);
        }
      }
      return;
    case 'start_match':
      if (player.id !== room.hostId) return sendError(ws, 'Only the room host can start the match.');
      startMatch(room);
      return;
    case 'vote_map':
      if (room.matchStarted || room.roundState !== 'lobby') return sendError(ws, 'Map voting is closed.');
      if (MAPS.includes(msg.mapId)) room.mapVotes[player.id] = msg.mapId;
      sendLobby(room);
      return;
    case 'transfer_host': {
      if (player.id !== room.hostId) return sendError(ws, 'Only the room host can transfer host privileges.');
      const target = room.players.get(String(msg.targetId));
      if (!target || target.id === player.id) return sendError(ws, 'Choose another player in this room.');
      room.hostId = target.id;
      sendLobby(room);
      publishState(room);
      return;
    }
    case 'kick': {
      if (player.id !== room.hostId) return sendError(ws, '방장만 플레이어를 내보낼 수 있습니다.');
      const target = room.players.get(String(msg.targetId));
      if (!target || target.id === player.id) return;
      send(target.ws, { type: 'error', message: '방장에 의해 방에서 내보내졌습니다.' });
      target.ws.close(1000, 'Kicked');
      return;
    }
    case 'hit': {
      if (!room.matchStarted || room.roundState !== 'live') return;
      const target = room.players.get(String(msg.targetId));
      if (!target || target.id === player.id || !target.alive) return;
      if (now() - player.lastHitAt < 80) return;
      player.lastHitAt = now();
      const dx = target.x-player.x, dy = target.y-player.y, dz = target.z-player.z;
      if (Math.hypot(dx,dy,dz) > 250) return;
      const damage = num(msg.damage, 1, 1, 250);
      target.hp = (target.hp ?? 100) - damage;
      if (target.hp <= 0) {
        target.alive = false; target.deaths += 1; player.kills += 1;
        if (room.matchMode === 'ffa') room.scores.RED += 0;
        else room.scores[player.team] = (room.scores[player.team] || 0) + 1;
        send(target.ws, { type: 'player_died', playerId: target.id, attackerId: player.id });
        broadcast(room, { type: 'hit_confirmed', attackerId: player.id, targetId: target.id, killed: true, head: !!msg.head });
        setTimeout(() => {
          if (!rooms.has(room.code) || !room.players.has(target.id)) return;
          resetPlayer(target); target.hp = 100;
          send(target.ws, { type: 'respawn', playerId: target.id, x: target.x, y: target.y, z: target.z });
          publishState(room);
        }, 2500);
      } else {
        broadcast(room, { type: 'hit_confirmed', attackerId: player.id, targetId: target.id, killed: false, head: !!msg.head });
      }
      publishState(room);
      return;
    }
    case 'respawn':
      if (!player.alive) { resetPlayer(player); player.hp = 100; }
      send(ws, { type: 'respawn', playerId: player.id, x: player.x, y: player.y, z: player.z });
      publishState(room);
      return;
    default:
      return;
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw.toString());
      handleMessage(ws, msg);
    } catch (_) {
      sendError(ws, '메시지를 처리할 수 없습니다.');
    }
  });
  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

setInterval(() => {
  const t = now();
  for (const room of rooms.values()) {
    if (room.matchStarted && room.roundState === 'weapon_select' && t >= room.weaponSelectAt) {
      room.roundState = 'live';
      room.endsAt = t + MATCH_DURATION_MS;
      broadcast(room, { type: 'round_started', endsAt: room.endsAt });
      publishState(room);
    } else if (room.matchStarted && room.roundState === 'live' && room.endsAt && t >= room.endsAt) {
      room.matchStarted = false;
      room.roundState = 'lobby';
      room.endsAt = 0;
      for (const p of room.players.values()) { p.ready = false; p.alive = true; p.hp = 100; }
      broadcast(room, { type: 'round_end', message: '경기 종료 · 다음 경기를 준비하세요.' });
      sendLobby(room);
      publishState(room);
    } else if (room.players.size > 0) {
      publishState(room);
    }
  }
}, TICK_MS);

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 30000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`IRON VEIL V2.0 server listening on port ${PORT}`);
});
process.on('SIGTERM', () => {
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
});
