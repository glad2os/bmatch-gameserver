// Run against the local Compose stack with Node 18 or newer.
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {io} = require('socket.io-client');
const sockets = [];
let socketError;
const api = process.env.SMOKE_API_URL || 'http://localhost:8002';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function waitFor(label, predicate) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
        if (socketError) throw socketError;
        const value = predicate();
        if (value) return value;
        await delay(20);
    }
    throw new Error(`Timeout: ${label}`);
}
async function request(path, body, token) {
    const response = await fetch(`${api}/api/user/${path}`, {
        method: body ? 'POST' : 'GET',
        headers: {'Content-Type': 'application/json', ...(token ? {Authorization: `Bearer ${token}`} : {})},
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(15000)
    });
    const result = await response.json();
    assert.equal(response.status, 200, `${path}: ${JSON.stringify(result)}`);
    return result;
}
async function account() {
    const username = `smoke_${randomUUID().replace(/-/g, '').slice(0, 16)}`;
    const credentials = {username, email: `${username}@example.com`, password: 'SmokeDev123'};
    await request('create', credentials);
    const {message: token} = await request('login', credentials);
    const profile = await request('profile', null, token);
    assert.equal(profile.username, username);
    return {token, profile};
}
async function connect(url) {
    const socket = io(url, {transports: ['websocket'], reconnection: false, autoConnect: false});
    sockets.push(socket);
    socket.events = [];
    socket.onAny((name, value) => socket.events.push({name, value}));
    socket.on('systemMessage', raw => {
        const message = typeof raw === 'string' ? JSON.parse(raw).message : raw.message;
        if (message !== socket.expectedError) socketError = new Error(message);
    });
    socket.on('connect_error', error => { socketError = error; });
    socket.connect();
    await waitFor('socket connection', () => socket.connected);
    return socket;
}
async function event(socket, name, predicate = () => true, from = 0) {
    return (await waitFor(name, () => socket.events.slice(from).find(e => e.name === name && predicate(e.value)))).value;
}
async function main() {
    const {token, profile} = await account();
    console.log('Registration, login and profile OK');
    const lobby = await connect(process.env.SMOKE_LOBBY_URL || 'http://localhost:8004');
    const lobbyID = `${profile.username}#1000-3000`;
    lobby.emit('join', {lobbyID, token});
    await event(lobby, 'playerList', list => JSON.parse(list.Players || '[]').length === 1);
    for (let count = 2; count <= 4; count++) {
        lobby.emit('join', {lobbyID, token, type: 'Bot'});
        await event(lobby, 'playerList', list => JSON.parse(list.Players || '[]').length === count);
    }
    console.log('Lobby with three bots OK');
    const extra = await account();
    const fifth = await connect(process.env.SMOKE_LOBBY_URL || 'http://localhost:8004');
    fifth.expectedError = 'Lobby is full';
    fifth.emit('join', {lobbyID, token: extra.token});
    await event(fifth, 'systemMessage', raw => JSON.parse(raw).message === 'Lobby is full');
    fifth.disconnect();
    lobby.emit('generateGame', {token});
    const {uuid: gameUUID} = await event(lobby, 'gameUUID');
    const game = await connect(process.env.SMOKE_GAME_URL || 'http://localhost:8005');
    game.emit('join', {gameUUID, token});
    await event(game, 'currentPlayer', player => player.id === profile.id);
    await event(game, 'timeRequested', seconds => seconds > 0);
    await event(game, 'gameAction');
    let offset = game.events.length;
    game.emit('sendOpenedBubble', {gameUUID, token, bubbleId: 0});
    await event(game, 'openBubble', b => b.bubbleId === 0, offset);
    await event(game, 'gameAction', a => a.requestedBubbles.bubbleId === 0, offset);
    offset = game.events.length;
    game.emit('sendOpenedBubble', {gameUUID, token, bubbleId: 0});
    await delay(250);
    assert.equal(game.events.slice(offset).filter(e => e.name === 'openBubble').length, 0, 'Duplicate click accepted');
    // A matching pair retains the turn; try disjoint pairs until a miss.
    for (let bubbleId = 1; bubbleId < 100; bubbleId += 2) {
        offset = game.events.length;
        game.emit('sendOpenedBubble', {gameUUID, token, bubbleId});
        await event(game, 'gameAction', () => true, offset);
        await event(game, 'openBubble', b => b.bubbleId === bubbleId, offset);
        if (game.events.slice(offset).some(e => e.name === 'currentPlayer' && e.value.type === 'Bot')) {
            console.log('Move, duplicate rejection and turn change OK');
            return;
        }
        assert.ok(bubbleId < 99, 'No turn change after all pairs');
        game.emit('sendOpenedBubble', {gameUUID, token, bubbleId: bubbleId + 1});
        await event(game, 'gameAction', a => a.requestedBubbles.bubbleId === bubbleId + 1, offset);
    }
}
main().catch(error => {
    console.error(error);
    process.exitCode = 1;
}).finally(() => sockets.forEach(socket => socket.disconnect()));
