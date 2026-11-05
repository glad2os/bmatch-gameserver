const {setPaused, cancelTimer, getLobbyData, getGamePlayers, getGameArea, getUserFromRedisByUserId} = require("./getGameData");
const {getPostgresConfig} = require("../config/config");
const {setStatusById} = require("./getPostgresqlUserData");
const {eventsKey, recordEvent, readEvents} = require("./gameEvents");
const {saveReplay, summarize} = require("./replayStore");

// reason: 'completed' (all pairs found) is saved as a replay and kept briefly for late clients;
// 'abandoned' (every human left, or nobody moved for too long) is not recorded and is deleted.
async function endGame(io, redisClient, rabbitMQChannel, consul, gameUUID, reason = 'completed') {
    const key = `Game:${gameUUID}`;
    if (!await redisClient.exists(key) || !await redisClient.hSetNX(key, 'GameOver', '1')) return;
    await setPaused(redisClient, gameUUID, 'true');
    await cancelTimer(redisClient, rabbitMQChannel, gameUUID);
    await recordEvent(redisClient, gameUUID, {type: 'end', reason});
    io.to(gameUUID).emit('gameOver', await gameOverSummary(redisClient, consul, gameUUID, reason));
    const lobby = await getLobbyData(redisClient, gameUUID);
    const cfg = await getPostgresConfig(consul);
    for (const id of lobby.players) {
        try {
            await setStatusById(id, "REGISTERED", cfg);
        } catch (error) {
            console.error(`Failed to reset status for user ${id}:`, error);
        }
        // Do not remove a mapping created by a subsequent game.
        if (await redisClient.get(`UserGame:${id}`) === gameUUID) {
            await redisClient.del(`UserGame:${id}`);
        }
    }

    if (reason !== 'completed') {
        await redisClient.del([key, `${key}:TimerStart`, `${key}:BotRunning`, eventsKey(gameUUID)]);
        return;
    }
    try {
        await saveReplay(cfg, await buildReplay(redisClient, consul, gameUUID, lobby));
    } catch (error) {
        console.error(`Failed to save the replay of game ${gameUUID}:`, error);
    }
    for (const gameKey of [key, `${key}:TimerStart`, eventsKey(gameUUID)]) {
        await redisClient.expire(gameKey, 600);
    }
}

// What clients show on the results screen: pairs per seat and the winners.
async function gameOverSummary(redisClient, consul, gameUUID, reason) {
    try {
        const players = await getGamePlayers(redisClient, consul, gameUUID);
        const {pairs, winners} = summarize(players, await readEvents(redisClient, gameUUID));
        const debug = await redisClient.hGet(`Game:${gameUUID}`, 'Debug') === '1';
        return {reason, debug, pairs, winners};
    } catch (error) {
        console.error(`Failed to summarize game ${gameUUID}:`, error);
        return {reason};
    }
}

async function buildReplay(redisClient, consul, gameUUID, lobby) {
    const players = (await getGamePlayers(redisClient, consul, gameUUID)).map(player => ({
        type: player.type, id: player.id, username: player.username, mmr: player.mmr, left: !!player.left,
        ...(player.replaces ? {replaces: player.replaces} : {})
    }));
    // People whose seat a bot took are no longer in the seat list but their moves are in the log.
    for (const id of lobby.leftPlayers || []) {
        if (players.some(player => player.type === 'Player' && player.id === id)) continue;
        const profile = await getUserFromRedisByUserId(redisClient, consul, id);
        players.push({type: 'Player', id, username: profile.username, mmr: profile.mmr, left: true});
    }
    const events = await readEvents(redisClient, gameUUID);
    const debug = await redisClient.hGet(`Game:${gameUUID}`, 'Debug') === '1';
    return {
        gameUUID,
        lobbyId: lobby.lobbyId,
        startedAt: events.find(event => event.type === 'start')?.t ?? events[0]?.t ?? Date.now(),
        finishedAt: Date.now(),
        players,
        board: await getGameArea(redisClient, gameUUID),
        events,
        summary: {...summarize(players, events), debug},
        debug
    };
}

module.exports = {endGame};
