'use strict';

const { spawn } = require('node:child_process');
const { once } = require('node:events');
const http = require('node:http');
const net = require('node:net');
const path = require('node:path');
const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');

const root = path.resolve(__dirname, '..');
let serverProcess;
let port;
const clients = [];

async function freePort() {
  const probe = net.createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const chosen = probe.address().port;
  await new Promise(resolve => probe.close(resolve));
  return chosen;
}

async function requestHealth() {
  return new Promise((resolve, reject) => {
    http.get(`http://127.0.0.1:${port}/health`, res => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(body) }));
    }).on('error', reject);
  });
}

class Peer {
  constructor(socket) {
    this.socket = socket;
    this.messages = [];
    this.waiters = [];
    socket.on('message', raw => {
      const message = JSON.parse(raw.toString());
      const index = this.waiters.findIndex(waiter => waiter.predicate(message));
      if (index >= 0) this.waiters.splice(index, 1)[0].resolve(message);
      else this.messages.push(message);
    });
  }

  send(message) { this.socket.send(JSON.stringify(message)); }

  waitFor(predicate, timeoutMs = 3000) {
    const index = this.messages.findIndex(predicate);
    if (index >= 0) return Promise.resolve(this.messages.splice(index, 1)[0]);
    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve: value => { clearTimeout(timer); resolve(value); } };
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter(item => item !== waiter);
        reject(new Error('Timed out waiting for server message'));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  async sendAndWait(message, expected) {
    const predicate = typeof expected === 'function' ? expected : item => item.type === expected;
    const waiting = this.waitFor(predicate);
    this.send(message);
    return waiting;
  }

  close() { if (this.socket.readyState < WebSocket.CLOSING) this.socket.close(); }
}

async function connectPeer() {
  const socket = new WebSocket(`ws://127.0.0.1:${port}`);
  await once(socket, 'open');
  const peer = new Peer(socket);
  clients.push(peer);
  return peer;
}

after(async () => {
  for (const peer of clients) peer.close();
  if (serverProcess && serverProcess.exitCode === null) {
    serverProcess.kill('SIGTERM');
    await Promise.race([once(serverProcess, 'exit'), new Promise(resolve => setTimeout(resolve, 2000))]);
  }
});

test('channels, room flow, map voting, host transfer, and manual match start work end to end', async () => {
  port = await freePort();
  serverProcess = spawn(process.execPath, [path.join(root, 'server.js')], {
    cwd: root,
    env: { ...process.env, PORT: String(port), MAX_PLAYERS: '8', MATCH_DURATION_SECONDS: '60' },
    stdio: 'ignore'
  });

  let health;
  for (let attempt = 0; attempt < 40; attempt++) {
    try { health = await requestHealth(); break; }
    catch { await new Promise(resolve => setTimeout(resolve, 100)); }
  }
  assert.equal(health?.status, 200);
  assert.equal(health?.body.ok, true);

  const host = await connectPeer();
  const missingName = host.sendAndWait({ type: 'create_room', channelId: '2', matchMode: 'team' }, 'error');
  assert.match((await missingName).message, /nickname/i);

  const joinedHost = await host.sendAndWait({ type: 'create_room', channelId: '2', matchMode: 'team', teamSize: 2, teamCount: 2, name: 'Alpha', roomName: 'Test Room' }, 'room_joined');
  assert.equal(joinedHost.channelId, '2');
  assert.equal(joinedHost.matchMode, 'team');

  const guest = await connectPeer();
  const listing = await guest.sendAndWait({ type: 'list_servers' }, 'server_list');
  assert.deepEqual(listing.channels.map(channel => channel.channelId), ['1', '2', '3']);
  assert.deepEqual(listing.rooms.map(room => room.roomCode), [joinedHost.roomCode]);
  assert.equal(listing.rooms[0].joinable, true);
  const joinedGuest = await guest.sendAndWait({ type: 'join_room', roomCode: joinedHost.roomCode, name: 'Bravo' }, 'room_joined');
  assert.equal(joinedGuest.roomCode, joinedHost.roomCode);
  const quick = await connectPeer();
  const quickJoined = await quick.sendAndWait({ type: 'quick_match', channelId: '2', matchMode: 'team', name: 'Charlie' }, 'room_joined');
  assert.equal(quickJoined.roomCode, joinedHost.roomCode);
  quick.close();

  const lobbyAfterFirstVote = host.sendAndWait({ type: 'vote_map', mapId: 'map2' }, message => message.type === 'match_lobby' && message.mapVotes[joinedHost.playerId] === 'map2');
  await lobbyAfterFirstVote;
  const hostVoteUpdate = host.sendAndWait({ type: 'vote_map', mapId: 'map4' }, message => message.type === 'match_lobby' && message.mapVotes[joinedHost.playerId] === 'map4');
  await hostVoteUpdate;
  const guestVoteUpdate = guest.sendAndWait({ type: 'vote_map', mapId: 'map4' }, message => message.type === 'match_lobby' && message.mapVotes[joinedGuest.playerId] === 'map4');
  await guestVoteUpdate;
  const transfer = host.sendAndWait({ type: 'transfer_host', targetId: joinedGuest.playerId }, message => message.type === 'match_lobby' && message.hostId === joinedGuest.playerId);
  const hostTransferLobby = await transfer;
  assert.equal(hostTransferLobby.hostId, joinedGuest.playerId);
  const guestTransferLobby = await guest.waitFor(message => message.type === 'match_lobby' && message.hostId === joinedGuest.playerId);
  assert.equal(guestTransferLobby.mapVotes[joinedHost.playerId], 'map4');
  assert.equal(Object.values(guestTransferLobby.mapVotes).filter(map => map === 'map4').length, 2);
  assert.equal(Object.keys(guestTransferLobby.mapVotes).length, 2);

  const deniedStart = host.sendAndWait({ type: 'start_match' }, 'error');
  assert.match((await deniedStart).message, /host/i);
  await new Promise(resolve => setTimeout(resolve, 5200));
  assert.equal(host.messages.some(message => message.type === 'match_started'), false, 'matches must wait for the host to start them');
  const started = await guest.sendAndWait({ type: 'start_match' }, 'match_started');
  assert.equal(started.mapId, 'map4');

  const hostDeparture = host.waitFor(message => message.type === 'match_lobby' && message.hostId === joinedHost.playerId && message.players.length === 1);
  guest.close();
  const reassigned = await hostDeparture;
  assert.equal(reassigned.players.length, 1);
  assert.equal(reassigned.hostId, joinedHost.playerId);

  host.close();
});
