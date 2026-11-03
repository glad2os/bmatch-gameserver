const {test} = require('node:test');
const assert = require('node:assert/strict');
const {sign} = require('jsonwebtoken');
const {Pool} = require('pg');
const {JWTToken} = require('../config/config');
const {joinLobby, generateGame} = require('../sockets/lobbyws');
const {openedBubble, userPause, joinServer, disconnectServer, listenForExpiredMessages} = require('../sockets/gamews');
const {setTimer, getRemainingTime} = require('../utils/getGameData');
const {endGame} = require('../utils/endGameHandler');
const {handleUserAuthentication} = require('../utils/userAuth');

function environment() {
    const values = new Map();
    const ttls = new Map();
    const redis = {
        get: async key => values.get(key) ?? null,
        set: async (key, value, options = {}) => {
            if (options.NX && values.has(key)) return null;
            values.set(key, value); if (options.EX) ttls.set(key, options.EX); return 'OK';
        },
        hGet: async (key, field) => values.get(key)?.[field] ?? null,
        hGetAll: async key => ({...(values.get(key) || {})}),
        hSet: async (key, field, value) => {
            const hash = values.get(key) || {};
            const fields = typeof field === 'object' ? field : {[field]: value};
            for (const [name, val] of Object.entries(fields)) hash[name] = String(val);
            values.set(key, hash);
        },
        hSetNX: async (key, field, value) => {
            if (await redis.hGet(key, field) !== null) return false;
            await redis.hSet(key, field, value); return true;
        },
        hIncrBy: async (key, field, amount) => {
            const value = Number(await redis.hGet(key, field) || 0) + amount;
            await redis.hSet(key, field, value); return value;
        },
        hDel: async (key, field) => { delete values.get(key)?.[field]; },
        exists: async key => Number(values.has(key)),
        expire: async (key, seconds) => { ttls.set(key, seconds); return true; },
        ttl: async key => ttls.get(key) ?? -1,
        del: async key => values.delete(key)
    };
    const events = [];
    const io = {to: target => ({emit: (name, value) => events.push({target, name, value})})};
    const consul = {catalog: {service: {nodes: async () => [{ServiceAddress: 'postgres', ServicePort: 5432}]}}};
    const channel = {publish() {}, ack() {}, consume: async (queue, handler) => { channel.consumer = handler; }};
    const socket = id => ({id, handlers: {}, on(name, handler) { this.handlers[name] = handler; }, join: async () => {}});
    async function user(id) {
        const profile = {id, username: `player${id}`, mmr: 2000};
        await redis.set(`gameserver:user:${id}`, JSON.stringify(profile));
        return sign({}, JWTToken, {subject: String(id), expiresIn: '1h'});
    }
    async function game() {
        await redis.hSet('Game:game', {
            LobbyData: JSON.stringify({players: [1, 2], bots: [], readyPlayers: 0}),
            CurrentPlayer: JSON.stringify({id: 1, username: 'player1', type: 'Player', actionPoints: 2}),
            GameArea: JSON.stringify(Array.from({length: 100}, (_, i) => Math.floor(i / 2) + 1)),
            GamePaused: 'false', LastActionId: '0',
            'Action:0': JSON.stringify({openBubbles: [], requestedBubbles: {}})
        });
        await redis.expire('Game:game', 21600);
    }
    return {redis, io, consul, channel, socket, user, game, events, ttls};
}

// No external database is required; cached safe profiles exercise real JWT/auth paths.
Pool.prototype.query = async () => ({rows: [{status: 'REGISTERED'}]});

test('four simultaneous human joins persist; fifth and unauthorized bot are rejected', async () => {
    const e = environment();
    const clients = [];
    for (let id = 1; id <= 5; id++) {
        const token = await e.user(id);
        const socket = e.socket(`ws${id}`);
        joinLobby(e.io, socket, e.consul, e.redis);
        generateGame(e.io, socket, e.redis, e.consul);
        clients.push({socket, token});
    }
    await Promise.all(clients.slice(0, 4).map(({socket, token}) => socket.handlers.join({lobbyID: 'player1#1000-3000', token})));
    assert.equal(JSON.parse(await e.redis.hGet('Lobby:player1#1000-3000', 'Players')).length, 4);
    await clients[4].socket.handlers.join({lobbyID: 'player1#1000-3000', token: clients[4].token});
    assert.equal(JSON.parse(e.events.at(-1).value).message, 'Lobby is full');
    await clients[1].socket.handlers.join({lobbyID: 'player1#1000-3000', token: clients[1].token, type: 'Bot'});
    assert.match(JSON.parse(e.events.at(-1).value).message, /Only the lobby admin/);
    await clients[1].socket.handlers.generateGame({token: clients[1].token});
    assert.match(JSON.parse(e.events.at(-1).value).message, /Only the lobby admin/);
    await clients[0].socket.handlers.generateGame({token: clients[0].token});
    const uuid = e.events.at(-1).value.uuid;
    assert.ok(uuid);
    assert.equal(await e.redis.get('UserGame:4'), uuid);
    assert.equal(e.ttls.get(`Game:${uuid}`), 21600);
});

test('simultaneous repeated clicks cannot create a matched pair', async () => {
    const e = environment(); await e.game();
    const token = await e.user(1); const socket = e.socket('ws1');
    openedBubble(e.io, socket, e.consul, e.redis, e.channel);
    await Promise.all([0, 0, 0].map(bubbleId => socket.handlers.sendOpenedBubble({token, gameUUID: 'game', bubbleId})));
    const action = JSON.parse(await e.redis.hGet('Game:game', 'Action:1'));
    assert.deepEqual(action.openBubbles, []);
    assert.equal(action.requestedBubbles.bubbleId, 0);
    assert.equal(await e.redis.hGet('Game:game', 'LastActionId'), '1');
    await socket.handlers.sendOpenedBubble({token, gameUUID: 'game', bubbleId: 1});
    const matched = JSON.parse(await e.redis.hGet('Game:game', 'Action:2'));
    assert.equal(matched.openBubbles.length, 2);
    await socket.handlers.sendOpenedBubble({token, gameUUID: 'game', bubbleId: 0});
    assert.equal(await e.redis.hGet('Game:game', 'LastActionId'), '2');
});

test('pause rejects spectators, stops timer, resumes remainder and uses per-user cooldown', async () => {
    const e = environment(); await e.game();
    const player = e.socket('player'); const guest = e.socket('guest');
    const token = await e.user(1); const token2 = await e.user(2); const guestToken = await e.user(3);
    userPause(e.io, player, e.consul, e.redis, e.channel);
    userPause(e.io, guest, e.consul, e.redis, e.channel);
    await setTimer(e.channel, e.redis, 'game', 17);
    await guest.handlers.userPause({token: guestToken, gameUUID: 'game'});
    assert.equal(await e.redis.hGet('Game:game', 'GameUserPaused'), null);
    await player.handlers.userPause({token, gameUUID: 'game'});
    assert.equal(await getRemainingTime(e.redis, 'game'), 0);
    assert.equal(JSON.parse(await e.redis.hGet('Game:game', 'GameUserPaused')).paused, 'true');
    await player.handlers.userPause({token, gameUUID: 'game'});
    assert.ok(await getRemainingTime(e.redis, 'game') > 16);
    await player.handlers.userPause({token, gameUUID: 'game'});
    assert.match(JSON.parse(e.events.at(-1).value).message, /5 minutes/);
    await player.handlers.userPause({token: token2, gameUUID: 'game'});
    assert.equal(JSON.parse(await e.redis.hGet('Game:game', 'GameUserPaused')).paused, 'true');
});

test('stale timers are ignored and repeated idle turns end game exactly once', async () => {
    const e = environment(); await e.game(); await e.user(1); await e.user(2);
    let acks = 0; e.channel.ack = () => acks++;
    await listenForExpiredMessages(e.io, e.redis, e.channel, e.consul);
    await setTimer(e.channel, e.redis, 'game', 30);
    await e.channel.consumer({content: Buffer.from(JSON.stringify({gameUUID: 'game', turnId: 0}))});
    assert.equal(await e.redis.hGet('Game:game', 'IdleTimeouts'), null);
    for (let i = 0; i < 4; i++) {
        const turnId = Number(await e.redis.hGet('Game:game', 'TurnId'));
        await e.channel.consumer({content: Buffer.from(JSON.stringify({gameUUID: 'game', turnId}))});
    }
    await endGame(e.io, e.redis, e.channel, e.consul, 'game');
    assert.equal(acks, 5);
    assert.equal(await e.redis.hGet('Game:game', 'GameOver'), '1');
    assert.equal(e.events.filter(event => event.name === 'gameOver').length, 1);
    assert.equal(e.ttls.get('Game:game'), 600);
});

test('guests receive action zero and disconnect removes socket tracking', async () => {
    const e = environment(); await e.game(); await e.user(1); await e.user(2);
    const socket = e.socket('guest');
    joinServer(e.io, socket, e.consul, e.redis, e.channel);
    disconnectServer(e.io, socket, e.redis);
    await socket.handlers.join({gameUUID: 'game'});
    assert.ok(e.events.some(event => event.name === 'gameAction' && event.value.openBubbles.length === 0));
    assert.deepEqual(JSON.parse(await e.redis.hGet('Game:game', 'GuestsSockets')), ['guest']);
    await socket.handlers.disconnect();
    assert.deepEqual(JSON.parse(await e.redis.hGet('Game:game', 'GuestsSockets')), []);
});

test('authentication has the same UserId before and after cache creation', async () => {
    const e = environment(); await e.game(); const token = await e.user(1);
    const data = {token, gameUUID: 'game'}; const socket = e.socket('ws1');
    const first = await handleUserAuthentication(e.io, data, socket, e.redis, e.consul);
    const cached = await handleUserAuthentication(e.io, data, socket, e.redis, e.consul);
    assert.deepEqual(first, cached);
    assert.equal(first.UserId, 1);
    assert.equal(JSON.parse(await e.redis.hGet('Game:game', 'LobbyData')).readyPlayers, 1);
});
