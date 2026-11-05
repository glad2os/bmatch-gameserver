const {
    getLobbyData, getGamePlayers, getCurrentGamePlayer, setCurrentGamePlayer,
    getReadyPlayers, getActionPointer, getGameArea, getUserFromRedisByUserId,
    setAction, setActionPointer, getAction, setPaused, getPaused, setNextPlayer,
    emitOpenBubbles, emitCloseBubbles, getGameWebSocketsFromRedis,
    getGameUUIDByGameWS, setTimer, getUserPaused, setUserPaused,
    getRemainingTime, cancelTimer, linkUserWithWebSocket, addGameWebSocketsGuestsToRedis, isActivePlayer,
    isDebugGame
} = require('../utils/getGameData');
const {setStatusById} = require('../utils/getPostgresqlUserData');
const {getPostgresConfig} = require('../config/config');
const {emitSystemMessage, extractAndVerifyJWT} = require('../utils/getLobbyData');
const {performBotActions} = require('../gameLogic/pseudoRandomBotLogic');
const {handleUserAuthentication} = require('../utils/userAuth');
const {endGame} = require('../utils/endGameHandler');
const {validateMove} = require('../gameLogic/validateMove');
const {withGameLock} = require('../utils/gameLock');
const {recordEvent, playerRef} = require('../utils/gameEvents');
const {idleGameTimeoutMs} = require('../config/config');

async function setupRabbitMQ(channel) {
    await channel.assertExchange('game_timer_delayed', 'x-delayed-message', {
        durable: true, arguments: {'x-delayed-type': 'direct'}
    });
    await channel.assertQueue('game_timer_fired', {durable: true});
    await channel.bindQueue('game_timer_fired', 'game_timer_delayed', 'fire');
}

async function isActive(redisClient, uuid) {
    return await redisClient.exists(`Game:${uuid}`) && !await redisClient.hGet(`Game:${uuid}`, 'GameOver');
}

async function finishIfComplete(io, redisClient, channel, consul, uuid) {
    const action = await getAction(redisClient, uuid, await getActionPointer(redisClient, uuid));
    if (action && action.openBubbles.length === 100) {
        await endGame(io, redisClient, channel, consul, uuid, 'completed');
        return true;
    }
    return !await isActive(redisClient, uuid);
}

const botRunningKey = uuid => `Game:${uuid}:BotRunning`;

// Called only while holding the game lock. Bot turns are started in the background and take the
// lock only for their own state changes; the lease also covers recovery after a restart.
async function continueTurn(io, redisClient, channel, consul, uuid) {
    if (await finishIfComplete(io, redisClient, channel, consul, uuid)) return;
    const player = await getCurrentGamePlayer(redisClient, uuid);
    // Bots always play automatically; in debug mode human players do too.
    if (player && (player.type === 'Bot' || await isDebugGame(redisClient, uuid))) {
        const key = botRunningKey(uuid);
        if (!await redisClient.set(key, '1', {NX: true, EX: 120})) {
            const ttl = await redisClient.ttl(key);
            setTimeout(() => withGameLock(uuid, async () => {
                const current = await getCurrentGamePlayer(redisClient, uuid);
                if (await isActive(redisClient, uuid) && current &&
                    (current.type === 'Bot' || await isDebugGame(redisClient, uuid))) {
                    await continueTurn(io, redisClient, channel, consul, uuid);
                }
            }).catch(console.error), Math.max(1, ttl + 1) * 1000).unref();
            return;
        }
        await cancelTimer(redisClient, channel, uuid);
        await setPaused(redisClient, uuid, 'true');
        runBotTurns(io, redisClient, channel, consul, uuid);
        return;
    }
    if (player) {
        await setPaused(redisClient, uuid, 'false');
        await setTimer(channel, redisClient, uuid, 30);
        io.to(uuid).emit('timeRequested', 30);
    }
}

// Runs outside the game lock; hands the turn back to a human (or ends the game) when bots finish.
async function runBotTurns(io, redisClient, channel, consul, uuid) {
    let failed = false;
    try {
        await performBotActions(io, redisClient, consul, {gameUUID: uuid}, channel, action => withGameLock(uuid, action));
    } catch (error) {
        failed = true;
        console.error('Bot turn failed:', error);
    }
    await withGameLock(uuid, async () => {
        await redisClient.del(botRunningKey(uuid));
        if (!await isActive(redisClient, uuid)) return;
        // After a failure, leave the game paused on the bot; the next join retries the turn.
        if (failed) return;
        await continueTurn(io, redisClient, channel, consul, uuid);
    }).catch(error => console.error('Failed to finish bot turn:', error));
}

async function listenForExpiredMessages(io, redisClient, channel, consul) {
    await channel.consume('game_timer_fired', async message => {
        if (!message) return;
        channel.ack(message);
        try {
            const {gameUUID: uuid, turnId} = JSON.parse(message.content.toString());
            await withGameLock(uuid, async () => {
                if (!await isActive(redisClient, uuid)) return;
                if (await isDebugGame(redisClient, uuid)) return;
                if (Number(await redisClient.hGet(`Game:${uuid}`, 'TurnId')) !== turnId) return;
                if ((await getUserPaused(redisClient, uuid)).paused) return;
                const current = await getCurrentGamePlayer(redisClient, uuid);
                const pointer = await getActionPointer(redisClient, uuid);
                const action = await getAction(redisClient, uuid, pointer);
                if (!current || current.type === 'Bot' || !action) return;
                // End abandoned games: no human move for idleGameTimeoutMs.
                const lastMoveAt = Number(await redisClient.hGet(`Game:${uuid}`, 'LastMoveAt'));
                if (!lastMoveAt) {
                    await redisClient.hSet(`Game:${uuid}`, 'LastMoveAt', String(Date.now()));
                } else if (Date.now() - lastMoveAt >= idleGameTimeoutMs) {
                    await endGame(io, redisClient, channel, consul, uuid, 'abandoned');
                    return;
                }
                await setPaused(redisClient, uuid, 'true');
                try {
                    await recordEvent(redisClient, uuid, {type: 'timeout', player: playerRef(current)});
                    await cancelTimer(redisClient, channel, uuid);
                    action.requestedBubbles = {};
                    await setAction(redisClient, uuid, pointer + 1, action);
                    await setActionPointer(redisClient, uuid, pointer + 1);
                    const next = await setNextPlayer(redisClient, consul, uuid, {UserId: current.id});
                    io.to(uuid).emit('gameAction', action);
                    io.to(uuid).emit('currentPlayer', next);
                    await continueTurn(io, redisClient, channel, consul, uuid);
                } finally {
                    // A started bot turn keeps the game paused until it hands the turn back.
                    if (await isActive(redisClient, uuid) && !await redisClient.exists(botRunningKey(uuid))) {
                        await setPaused(redisClient, uuid, 'false');
                    }
                }
            });
        } catch (error) {
            console.error('Failed to process expired game timer:', error);
        }
    });
}

function joinServer(io, socket, consul, redisClient, channel) {
    socket.on('join', async (data = {}) => {
        if (!data.gameUUID) return;
        const uuid = data.gameUUID;
        try {
            await withGameLock(uuid, async () => {
                if (!await redisClient.exists(`Game:${uuid}`)) throw new Error('Game not found');
                await socket.join(uuid);
                await linkUserWithWebSocket(redisClient, uuid, socket.id);
                const players = await getGamePlayers(redisClient, consul, uuid);
                io.to(uuid).emit('playerList', players);
                const action = await getAction(redisClient, uuid, await getActionPointer(redisClient, uuid));
                if (action) io.to(socket.id).emit('gameAction', action);
                if (!await isActive(redisClient, uuid)) {
                    io.to(socket.id).emit('gameOver');
                    return;
                }
                const auth = data.token && await handleUserAuthentication(io, data, socket, redisClient, consul);
                if (!auth) await addGameWebSocketsGuestsToRedis(redisClient, uuid, socket.id);
                let current = await getCurrentGamePlayer(redisClient, uuid);
                const lobby = await getLobbyData(redisClient, uuid);
                const ready = await getReadyPlayers(redisClient, uuid);
                if (auth && ready === lobby.players.length) {
                    if (!current) {
                        current = players[0];
                        await setCurrentGamePlayer(redisClient, uuid, current);
                        await recordEvent(redisClient, uuid, {type: 'start', players: players.map(playerRef)});
                        await recordEvent(redisClient, uuid, {type: 'turn', player: playerRef(current)});
                        await continueTurn(io, redisClient, channel, consul, uuid);
                    } else if ((current.type === 'Bot' || await isDebugGame(redisClient, uuid)) &&
                        !(await getUserPaused(redisClient, uuid)).paused) {
                        await continueTurn(io, redisClient, channel, consul, uuid);
                        current = await getCurrentGamePlayer(redisClient, uuid);
                    }
                }
                if (current) io.to(uuid).emit('currentPlayer', current);
                io.to(socket.id).emit('timeRequested', await getRemainingTime(redisClient, uuid));
                io.to(socket.id).emit('isPaused', await getUserPaused(redisClient, uuid));
                if (await isDebugGame(redisClient, uuid)) io.to(socket.id).emit('debugMode', true);
            });
        } catch (error) {
            emitSystemMessage(io, socket, error.message);
        }
    });
}

function disconnectServer(io, socket, redisClient) {
    socket.on('disconnect', async () => {
        try {
            const uuid = await getGameUUIDByGameWS(redisClient, socket.id);
            if (!uuid) return;
            await redisClient.del(`GameWS:${socket.id}`);
            await withGameLock(uuid, async () => {
                if (!await redisClient.exists(`Game:${uuid}`)) return;
                for (const field of ['PlayersSockets', 'GuestsSockets']) {
                    const sockets = JSON.parse(await redisClient.hGet(`Game:${uuid}`, field) || '[]');
                    await redisClient.hSet(`Game:${uuid}`, field, JSON.stringify(sockets.filter(id => id !== socket.id)));
                }
            });
        } catch (error) {
            console.error('Failed to remove game socket:', error);
        }
    });
}

function openedBubble(io, socket, consul, redisClient, channel) {
    socket.on('sendOpenedBubble', async (data = {}) => {
        const uuid = data.gameUUID;
        if (!uuid || !data.token || data.bubbleId === undefined || data.bubbleId === null) return;
        try {
            await withGameLock(uuid, async () => {
                if (!await isActive(redisClient, uuid) || await getPaused(redisClient, uuid) ||
                    (await getUserPaused(redisClient, uuid)).paused || await isDebugGame(redisClient, uuid)) return;
                const auth = await handleUserAuthentication(io, data, socket, redisClient, consul);
                if (!auth) return;
                const current = await getCurrentGamePlayer(redisClient, uuid);
                if (!current || current.type === 'Bot' || Number(auth.UserId) !== current.id) return;
                const points = current.actionPoints ?? 2;
                const pointer = await getActionPointer(redisClient, uuid);
                const lastAction = await getAction(redisClient, uuid, pointer);
                if (!validateMove(data.bubbleId, lastAction, points)) return;
                const bubbleId = Number(data.bubbleId);
                const area = await getGameArea(redisClient, uuid);
                // openedBy is kept with matched pairs so clients can colour them after a reconnect.
                const bubble = {bubbleId, bubbleImg: area[bubbleId], openedBy: {type: current.type, id: current.id}};
                const action = {
                    openBubbles: [...lastAction.openBubbles], requestedBubbles: bubble,
                    sender: current, serverTime: new Date().toISOString()
                };
                await redisClient.hSet(`Game:${uuid}`, 'LastMoveAt', String(Date.now()));
                current.actionPoints = points - 1;
                let miss = false;
                if (points === 1) {
                    miss = lastAction.requestedBubbles.bubbleImg !== bubble.bubbleImg;
                    if (!miss) {
                        action.openBubbles.push(lastAction.requestedBubbles, bubble);
                        current.actionPoints = 2;
                    }
                    action.requestedBubbles = {};
                }
                await setCurrentGamePlayer(redisClient, uuid, current);
                await setAction(redisClient, uuid, pointer + 1, action);
                await setActionPointer(redisClient, uuid, pointer + 1);
                const player = playerRef(current);
                await recordEvent(redisClient, uuid, {type: 'open', player, bubbleId, bubbleImg: bubble.bubbleImg});
                if (points === 1) {
                    const bubbles = [Number(lastAction.requestedBubbles.bubbleId), bubbleId];
                    await recordEvent(redisClient, uuid, {type: miss ? 'miss' : 'match', player, bubbles});
                }
                await emitOpenBubbles(io, uuid, bubbleId, bubble.bubbleImg);
                if (miss) {
                    await setPaused(redisClient, uuid, 'true');
                    await cancelTimer(redisClient, channel, uuid);
                    const next = await setNextPlayer(redisClient, consul, uuid, auth);
                    io.to(uuid).emit('currentPlayer', next);
                    io.to(uuid).emit('timeRequested', 0);
                    // Closing a missed pair is also a state mutation and must use the same lock.
                    setTimeout(() => withGameLock(uuid, async () => {
                        if (!await isActive(redisClient, uuid)) return;
                        await emitCloseBubbles(io, uuid, Number(lastAction.requestedBubbles.bubbleId), bubbleId);
                        await setPaused(redisClient, uuid, 'false');
                        if (!(await getUserPaused(redisClient, uuid)).paused) {
                            await continueTurn(io, redisClient, channel, consul, uuid);
                        }
                    }).catch(error => console.error('Failed to close missed bubbles:', error)), 2000).unref();
                }
                io.to(uuid).emit('gameAction', action);
                await finishIfComplete(io, redisClient, channel, consul, uuid);
            });
        } catch (error) {
            emitSystemMessage(io, socket, error.message);
        }
    });
}

// "debug end" in the chat: no more turn timers and every turn (humans included) is played by the bot
// logic as fast as possible, so the game reaches a real end quickly; used to collect test replays.
const DEBUG_COMMAND = /^\s*debug\s+end\s*$/i;

async function enableDebugMode(io, redisClient, channel, consul, uuid, user) {
    await withGameLock(uuid, async () => {
        if (!await isActive(redisClient, uuid)) return;
        if (!await redisClient.hSetNX(`Game:${uuid}`, 'Debug', '1')) return;
        await recordEvent(redisClient, uuid, {type: 'debug', player: {type: 'Player', id: user.id}});
        await cancelTimer(redisClient, channel, uuid);
        await closeUnfinishedPick(io, redisClient, uuid);
        io.to(uuid).emit('debugMode', true);
        io.to(uuid).emit('timeRequested', 0);
        io.to(uuid).emit('receiveMessage', {
            message: `${user.username} turned on debug mode: every turn is played automatically until the end`,
            username: 'System'
        });
        await continueTurn(io, redisClient, channel, consul, uuid);
    });
}

// Closes a first pick that has no second one (the turn ends early: leaving, debug mode).
async function closeUnfinishedPick(io, redisClient, uuid) {
    const pointer = await getActionPointer(redisClient, uuid);
    const action = await getAction(redisClient, uuid, pointer);
    if (action?.requestedBubbles?.bubbleId == null) return;
    const bubbleId = Number(action.requestedBubbles.bubbleId);
    await emitCloseBubbles(io, uuid, bubbleId, bubbleId);
    action.requestedBubbles = {};
    await setAction(redisClient, uuid, pointer + 1, action);
    await setActionPointer(redisClient, uuid, pointer + 1);
    io.to(uuid).emit('gameAction', action);
}

function chatMessage(io, socket, consul, redisClient, channel) {
    socket.on('chatMessage', async (data = {}) => {
        if (!data.gameUUID || typeof data.message !== 'string' || data.message.length < 1 || data.message.length > 64) return;
        try {
            // Read-only, so it does not take the game lock and is not delayed by bot turns.
            if (!await isActive(redisClient, data.gameUUID)) return;
            const user = await extractAndVerifyJWT(data.token, consul, redisClient);
            const lobby = await getLobbyData(redisClient, data.gameUUID);
            if (!isActivePlayer(lobby, user.id)) {
                io.to(socket.id).emit('receiveMessage', {message: 'nobody hears you ;(', username: 'System'});
                return;
            }
            if (DEBUG_COMMAND.test(data.message)) {
                await enableDebugMode(io, redisClient, channel, consul, data.gameUUID, user);
                return;
            }
            await recordEvent(redisClient, data.gameUUID, {type: 'chat', player: {type: 'Player', id: user.id}, message: data.message});
            for (const id of await getGameWebSocketsFromRedis(redisClient, data.gameUUID)) {
                io.to(id).emit('receiveMessage', {message: data.message, username: user.username});
            }
        } catch (error) {
            emitSystemMessage(io, socket, error.message);
        }
    });
}

function ping(socket, io) {
    socket.on('ping', () => io.to(socket.id).emit('pong'));
}

function userPause(io, socket, consul, redisClient, channel) {
    socket.on('userPause', async (data = {}) => {
        if (!data.gameUUID || !data.token) return;
        const uuid = data.gameUUID;
        try {
            await withGameLock(uuid, async () => {
                if (!await isActive(redisClient, uuid) || await isDebugGame(redisClient, uuid)) return;
                const auth = await handleUserAuthentication(io, data, socket, redisClient, consul);
                if (!auth) return;
                const paused = (await getUserPaused(redisClient, uuid)).paused;
                const key = `Game:${uuid}`;
                if (!paused) {
                    // Wait until a turn is playable (initial readiness / pair animation / bot).
                    if (await getPaused(redisClient, uuid)) return;
                    const field = `GameUserPausedAt:${auth.UserId}`;
                    const last = Number(await redisClient.hGet(key, field));
                    if (last && Date.now() - last < 300000) throw new Error('Cannot pause again within 5 minutes');
                    await redisClient.hSet(key, 'PausedRemaining', String(await getRemainingTime(redisClient, uuid)));
                    await cancelTimer(redisClient, channel, uuid);
                    await redisClient.hSet(key, field, String(Date.now()));
                } else {
                    const remaining = Number(await redisClient.hGet(key, 'PausedRemaining'));
                    await setTimer(channel, redisClient, uuid, remaining);
                    io.to(uuid).emit('timeRequested', remaining);
                }
                await setUserPaused(redisClient, uuid, paused ? 'false' : 'true');
                await recordEvent(redisClient, uuid, {type: paused ? 'resume' : 'pause', player: {type: 'Player', id: Number(auth.UserId)}});
                io.to(uuid).emit('isPaused', await getUserPaused(redisClient, uuid));
            });
        } catch (error) {
            emitSystemMessage(io, socket, error.message);
        }
    });
}

// A player leaves the game early (GG, WP / Tab on the client). They keep their seat in the list but
// turns skip them; when no human player is left, the game ends.
function leaveGame(io, socket, consul, redisClient, channel) {
    socket.on('leaveGame', async (data = {}) => {
        const uuid = data.gameUUID;
        if (!uuid || !data.token) return;
        try {
            await withGameLock(uuid, async () => {
                if (!await isActive(redisClient, uuid)) return;
                const auth = await handleUserAuthentication(io, data, socket, redisClient, consul);
                if (!auth) return;
                const userId = Number(auth.UserId);
                await recordEvent(redisClient, uuid, {type: 'leave', player: {type: 'Player', id: userId}});
                const current = await getCurrentGamePlayer(redisClient, uuid);
                const wasCurrent = current && current.type !== 'Bot' && current.id === userId;

                const lobby = await getLobbyData(redisClient, uuid);
                lobby.leftPlayers = [...new Set([...(lobby.leftPlayers || []), userId])];
                lobby.userWS = (lobby.userWS || []).filter(user => user.id !== userId);
                await redisClient.hSet(`Game:${uuid}`, 'LobbyData', JSON.stringify(lobby));
                await setStatusById(userId, 'REGISTERED', await getPostgresConfig(consul));
                if (await redisClient.get(`UserGame:${userId}`) === uuid) await redisClient.del(`UserGame:${userId}`);

                const user = await getUserFromRedisByUserId(redisClient, consul, userId);
                io.to(socket.id).emit('leftGame');

                if (lobby.players.every(id => lobby.leftPlayers.includes(id))) {
                    io.to(uuid).emit('receiveMessage', {message: `${user.username} left the game`, username: 'System'});
                    await endGame(io, redisClient, channel, consul, uuid, 'abandoned');
                    return;
                }
                // Other people still play: a bot takes the seat so the game keeps all its players.
                const substitute = {
                    id: `bot-for-${userId}`, username: `${user.username} (bot)`, mmr: 0, type: 'Bot',
                    lastSuccessfulAttempt: 0, seatOf: userId, replaces: {type: 'Player', id: userId}
                };
                if (!lobby.bots.some(bot => bot.id === substitute.id)) lobby.bots.push(substitute);
                await redisClient.hSet(`Game:${uuid}`, 'LobbyData', JSON.stringify(lobby));
                await recordEvent(redisClient, uuid, {type: 'replace', player: {type: 'Player', id: userId}, by: {type: 'Bot', id: substitute.id}});
                io.to(uuid).emit('receiveMessage', {message: `${user.username} left the game, a bot takes the seat`, username: 'System'});
                const players = await getGamePlayers(redisClient, consul, uuid);
                io.to(uuid).emit('playerList', players);
                if (!wasCurrent) return;

                // Close the leaver's unfinished first pick; the bot plays the rest of the turn.
                await cancelTimer(redisClient, channel, uuid);
                await closeUnfinishedPick(io, redisClient, uuid);
                const next = players.find(player => player.type === 'Bot' && player.id === substitute.id);
                await setCurrentGamePlayer(redisClient, uuid, next);
                await recordEvent(redisClient, uuid, {type: 'turn', player: playerRef(next)});
                io.to(uuid).emit('currentPlayer', next);
                await continueTurn(io, redisClient, channel, consul, uuid);
            });
        } catch (error) {
            emitSystemMessage(io, socket, error.message);
        }
    });
}

module.exports = {joinServer, disconnectServer, openedBubble, chatMessage, ping, userPause, leaveGame, listenForExpiredMessages, setupRabbitMQ};
