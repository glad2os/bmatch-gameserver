const {getPostgresConfig} = require("../config/config");
const {fetchUserById} = require("./getPostgresqlUserData");
const consul = require("consul");
const {setNextPlayerId} = require("../gameLogic/queue");
const {findProgress} = require("../gameLogic/botChance");
const {recordEvent, playerRef} = require("./gameEvents");
const util = require("util");

const {getCachedUserProfile} = require("./userProfileCache");
const getUserFromRedisByUserId = getCachedUserProfile;

async function getGamePlayers(redisClient, consul, gameUUID) {
    let gameData = await getLobbyData(redisClient, gameUUID)
    let gamePlayers = [];

    if (gameData.players == null) {
        throw new Error("No players in the game");
    }

    // Players who left keep their seat (and colour on clients). While other people still play, a bot
    // takes the seat (LobbyData.bots entry with seatOf); otherwise turns skip it.
    const leftPlayers = gameData.leftPlayers || [];
    const substitutes = new Map(gameData.bots.filter(bot => bot.seatOf != null).map(bot => [bot.seatOf, bot]));
    for (let id of gameData.players) {
        if (leftPlayers.includes(id) && substitutes.has(id)) {
            const bot = substitutes.get(id);
            gamePlayers.push({...bot, findProgress: findProgress(bot.lastSuccessfulAttempt)});
            continue;
        }
        let userFromRedisByUserId = await getUserFromRedisByUserId(redisClient, consul, id);

        const {
            id: userId, username, mmr,
        } = userFromRedisByUserId;

        const transformedUser = {id: userId, username, mmr, type: "Player", left: leftPlayers.includes(userId)};
        gamePlayers.push(transformedUser);
    }

    gameData.bots.filter(bot => bot.seatOf == null).forEach(bot => {
        gamePlayers.push({...bot, findProgress: findProgress(bot.lastSuccessfulAttempt)});
    });

    return gamePlayers;
}

async function getLobbyData(redisClient, gameUUID) {
    let redisData = await redisClient.hGet(`Game:${gameUUID}`, 'LobbyData');
    return JSON.parse(redisData)
}

async function setUserJWT_UUID_Cache(redisClient, JWTToken, UUID, socketId, userId) {
    await redisClient.hSet(`GameJWT:${JWTToken}`, "UUID", UUID);
    await redisClient.hSet(`GameJWT:${JWTToken}`, "Socket", socketId);
    await redisClient.hSet(`GameJWT:${JWTToken}`, "UserId", userId);
    await redisClient.expire(`GameJWT:${JWTToken}`, 7200);
}

async function getUserJWTCache(redisClient, JWTToken) {
    const gameJWT = await redisClient.hGetAll(`GameJWT:${JWTToken}`);
    if (Object.keys(gameJWT).length === 0) return null;

    return gameJWT;
}

async function incrementReadyPlayers(redisClient, uuid) {
    const lobbyDataRaw = await redisClient.hGet(`Game:${uuid}`, "LobbyData");
    if (!lobbyDataRaw) return null;

    const lobbyData = JSON.parse(lobbyDataRaw);

    if (typeof lobbyData.readyPlayers === 'number' && lobbyData.readyPlayers < lobbyData.players.length) {
        lobbyData.readyPlayers++;
    }

    await redisClient.hSet(`Game:${uuid}`, "LobbyData", JSON.stringify(lobbyData));

    return lobbyData;
}

async function getReadyPlayers(redisClient, uuid) {
    const lobbyDataRaw = await redisClient.hGet(`Game:${uuid}`, "LobbyData");
    if (!lobbyDataRaw) return null;

    const lobbyData = JSON.parse(lobbyDataRaw);

    return lobbyData.readyPlayers;
}


async function getCurrentGamePlayer(redisClient, uuid) {
    const currentPlayerRaw = await redisClient.hGet(`Game:${uuid}`, "CurrentPlayer");

    if (!currentPlayerRaw) return null;
    return JSON.parse(currentPlayerRaw);
}

async function setCurrentGamePlayer(redisClient, uuid, currentPlayer) {
    await redisClient.hSet(`Game:${uuid}`, "CurrentPlayer", JSON.stringify(currentPlayer));
}

async function getActionPointer(redisClient, uuid) {
    const actionPointerRaw = await redisClient.hGet(`Game:${uuid}`, `LastActionId`,);

    if (!actionPointerRaw) return null;
    return JSON.parse(actionPointerRaw);
}

async function setActionPointer(redisClient, uuid, actionPointerId) {
    await redisClient.hSet(`Game:${uuid}`, `LastActionId`, actionPointerId);
}

async function setAction(redisClient, uuid, actionId, action) {
    await redisClient.hSet(`Game:${uuid}`, `Action:${actionId}`, JSON.stringify(action));
}

async function getAction(redisClient, uuid, actionId) {
    const lastActionDataRaw = await redisClient.hGet(`Game:${uuid}`, `Action:${actionId}`);

    if (!lastActionDataRaw) return null;
    return JSON.parse(lastActionDataRaw);
}

async function getGameArea(redisClient, uuid) {
    const gameArea = await redisClient.hGet(`Game:${uuid}`, `GameArea`);

    if (!gameArea) return null;
    return JSON.parse(gameArea);
}

async function getPaused(redisClient, uuid) {
    let isPausedRaw = await redisClient.hGet(`Game:${uuid}`, `GamePaused`);
    return isPausedRaw === 'true';
}

async function setPaused(redisClient, uuid, flag) {
    await redisClient.hSet(`Game:${uuid}`, `GamePaused`, flag);
}


async function getUserPaused(redisClient, uuid) {
    let dataRaw = await redisClient.hGet(`Game:${uuid}`, `GameUserPaused`);
    let data = JSON.parse(dataRaw);
    return {
        time: data ? data.time : null,
        paused: data ? data.paused === 'true' : false
    };
}

async function setUserPaused(redisClient, uuid, flag) {
    let data = {
        time: new Date().toISOString(),
        paused: flag
    };
    await redisClient.hSet(`Game:${uuid}`, `GameUserPaused`, JSON.stringify(data));
}

async function setBotLastSuccessfulAttempt(redisClient, gameUUID, botId, newValue) {
    const lobbyData = await getLobbyData(redisClient, gameUUID);

    const bot = lobbyData.bots.find(b => b.id === botId);
    if (bot) {
        bot.lastSuccessfulAttempt = newValue;
    } else {
        throw new Error(`Bot with ID ${botId} not found in the lobby.`);
    }

    await redisClient.hSet(`Game:${gameUUID}`, 'LobbyData', JSON.stringify(lobbyData));
}

async function setNextPlayer(redisClient, consul, gameUUID, currentUser) {
    let currentGamePlayers = await getGamePlayers(redisClient, consul, gameUUID);

    let currentIndex;
    if (currentUser.type === "Bot") {
        currentIndex = currentGamePlayers.findIndex(bot => bot.id === currentUser.id);
    } else {
        currentIndex = currentGamePlayers.findIndex(player => player.id == currentUser.UserId);
    }

    let nextPlayerIndex = currentIndex;
    do {
        nextPlayerIndex = setNextPlayerId(nextPlayerIndex, currentGamePlayers);
    } while (currentGamePlayers[nextPlayerIndex].left && nextPlayerIndex !== currentIndex);
    let nextPlayer = currentGamePlayers[nextPlayerIndex];

    await setCurrentGamePlayer(redisClient, gameUUID, nextPlayer);
    await recordEvent(redisClient, gameUUID, {type: 'turn', player: playerRef(nextPlayer)});

    return nextPlayer;
}

async function emitCloseBubbles(io, gameUUID, select1, select2) {
    io.to(gameUUID).emit('closeBubbles', {
        firstBubbleId: Number(select1), secondBubbleId: Number(select2)
    });
}

async function emitOpenBubbles(io, gameUUID, bubbleId, bubbleImg) {
    io.to(gameUUID).emit('openBubble', {
        bubbleId: bubbleId, bubbleImg: bubbleImg,
    });
}

async function addGameJWTsToRedis(redisClient, gameUUID, token) {
    let userJWTs = await getGameJWTFromRedis(redisClient, gameUUID)

    if (!userJWTs.includes(token)) {
        userJWTs.push(token);
        await redisClient.hSet(`Game:${gameUUID}`, `UserJWTs`, JSON.stringify(userJWTs));
    }
}

async function getGameJWTFromRedis(redisClient, gameUUID) {
    let userJWTsStr = await redisClient.hGet(`Game:${gameUUID}`, `UserJWTs`);
    return userJWTsStr ? JSON.parse(userJWTsStr) : [];
}

/**
 * Adds a game's web socket wsId to a Redis store
 * using for send OpenBubble and closeBubble actions
 */

async function addGameWebSocketsToRedis(redisClient, gameUUID, wsId) {
    let userWebSockets = await getGameWebSocketsFromRedis(redisClient, gameUUID)

    if (!userWebSockets.includes(wsId)) {
        userWebSockets.push(wsId);
        await redisClient.hSet(`Game:${gameUUID}`, `PlayersSockets`, JSON.stringify(userWebSockets));
    }
}

async function getGameWebSocketsFromRedis(redisClient, gameUUID) {
    let userWebSockets = await redisClient.hGet(`Game:${gameUUID}`, `PlayersSockets`);
    return userWebSockets ? JSON.parse(userWebSockets) : [];
}

/**
 * Adds a game's web socket wsId to a Redis store
 * using for send guests actions
 */

async function addGameWebSocketsGuestsToRedis(redisClient, gameUUID, wsId) {
    let userWebSockets = await getGameWebSocketsGuestsFromRedis(redisClient, gameUUID)

    if (!userWebSockets.includes(wsId)) {
        userWebSockets.push(wsId);
        await redisClient.hSet(`Game:${gameUUID}`, `GuestsSockets`, JSON.stringify(userWebSockets));
    }
}

async function getGameWebSocketsGuestsFromRedis(redisClient, gameUUID) {
    let userWebSockets = await redisClient.hGet(`Game:${gameUUID}`, `GuestsSockets`);
    return userWebSockets ? JSON.parse(userWebSockets) : [];
}

async function linkUserWithWebSocket(redisClient, gameUUID, wsId) {
    await redisClient.hSet(`GameWS:${wsId}`, "gameUUID", gameUUID);
    await redisClient.expire(`GameWS:${wsId}`, 6 * 60 * 60);
}

async function getGameUUIDByGameWS(redisClient, wsId) {
    return await redisClient.hGet(`GameWS:${wsId}`, `gameUUID`);
}

async function setTimer(rabbitMQChannel, redisClient, gameUUID, duration = 30) {
    const key = `Game:${gameUUID}`;
    if (!await redisClient.exists(key) || await redisClient.hGet(key, 'GameOver')) return;
    const turnId = await redisClient.hIncrBy(key, 'TurnId', 1);
    const timerKey = `${key}:TimerStart`;
    await redisClient.hSet(timerKey, {TimerStart: String(Date.now()), TimerDuration: String(duration)});
    // A timer must not outlive its game, including the end-of-game grace period.
    const ttl = await redisClient.ttl(key);
    await redisClient.expire(timerKey, ttl > 0 ? ttl : 6 * 60 * 60);
    rabbitMQChannel.publish('game_timer_delayed', 'fire', Buffer.from(JSON.stringify({gameUUID, turnId})), {
        persistent: true, headers: {'x-delay': Math.max(1, Math.ceil(duration * 1000))}
    });
}

async function getRemainingTime(redisClient, gameUUID) {
    const timer = await redisClient.hGetAll(`Game:${gameUUID}:TimerStart`);
    const start = Number(timer.TimerStart);
    const duration = Number(timer.TimerDuration);
    if (!start || !duration) return 0;
    return Math.max(0, duration - (Date.now() - start) / 1000);
}

async function cancelTimer(redisClient, rabbitMQChannel, gameUUID) {
    const key = `Game:${gameUUID}`;
    if (!await redisClient.exists(key)) return;
    await redisClient.hIncrBy(key, 'TurnId', 1);
    const timerKey = `${key}:TimerStart`;
    if (await redisClient.exists(timerKey)) {
        await redisClient.hSet(timerKey, 'TimerDuration', '0');
    }
}

async function updateUsersWSAtLobbyData(redisClient, lobbyData, userFromJWT, socketId, gameUUID) {
    lobbyData.userWS = lobbyData.userWS || [];
    const userIndex = lobbyData.userWS.findIndex(u => u.id === userFromJWT.id);

    if (userIndex !== -1) {
        lobbyData.userWS[userIndex].ws = socketId;
    } else {
        lobbyData.userWS.push({id: userFromJWT.id, ws: socketId});
    }

    lobbyData.readyPlayers = lobbyData.userWS.length;
    await redisClient.hSet(`Game:${gameUUID}`, 'LobbyData', JSON.stringify(lobbyData));
}

// Debug mode ("debug end" in the chat): no turn timers, every turn is played by the bot logic.
async function isDebugGame(redisClient, gameUUID) {
    return await redisClient.hGet(`Game:${gameUUID}`, 'Debug') === '1';
}

// Pseudo-random counter of an automatically played player: bots keep it in LobbyData.bots,
// humans (debug mode) in LobbyData.autoplayAttempts.
function getAutoplayCounter(lobbyData, player) {
    if (player.type === "Bot") return lobbyData.bots.find(bot => bot.id === player.id)?.lastSuccessfulAttempt ?? 0;
    return lobbyData.autoplayAttempts?.[player.id] ?? 0;
}

async function setAutoplayCounter(redisClient, gameUUID, player, value) {
    if (player.type === "Bot") {
        await setBotLastSuccessfulAttempt(redisClient, gameUUID, player.id, value);
        return;
    }
    const lobbyData = await getLobbyData(redisClient, gameUUID);
    lobbyData.autoplayAttempts = {...(lobbyData.autoplayAttempts || {}), [player.id]: value};
    await redisClient.hSet(`Game:${gameUUID}`, 'LobbyData', JSON.stringify(lobbyData));
}

// A player of this game who has not left it.
function isActivePlayer(lobbyData, userId) {
    return lobbyData.players.includes(userId) && !(lobbyData.leftPlayers || []).includes(userId);
}

module.exports = {
    isDebugGame,
    getAutoplayCounter,
    setAutoplayCounter,
    isActivePlayer,
    getGamePlayers,
    setUserJWT_UUID_Cache,
    getUserJWTCache,
    getLobbyData,
    incrementReadyPlayers,
    getCurrentGamePlayer,
    setCurrentGamePlayer,
    getReadyPlayers,
    setAction,
    getAction,
    getActionPointer,
    getUserFromRedisByUserId,
    setActionPointer,
    getGameArea,
    getPaused,
    setPaused,
    setNextPlayer,
    setBotLastSuccessfulAttempt,
    emitCloseBubbles,
    emitOpenBubbles,
    addGameJWTsToRedis,
    addGameWebSocketsToRedis,
    getGameWebSocketsFromRedis,
    addGameWebSocketsGuestsToRedis,
    linkUserWithWebSocket,
    updateUsersWSAtLobbyData,
    getRemainingTime,
    getGameUUIDByGameWS,
    setTimer,
    getUserPaused,
    setUserPaused,
    cancelTimer,
    getGameJWTFromRedis
}