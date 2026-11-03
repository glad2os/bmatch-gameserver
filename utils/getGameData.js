const {getPostgresConfig} = require("../config/config");
const {fetchUserById} = require("./getPostgresqlUserData");
const consul = require("consul");
const {setNextPlayerId} = require("../gameLogic/queue");
const util = require("util");

const {getCachedUserProfile} = require("./userProfileCache");
const getUserFromRedisByUserId = getCachedUserProfile;

async function getGamePlayers(redisClient, consul, gameUUID) {
    let gameData = await getLobbyData(redisClient, gameUUID)
    let gamePlayers = [];

    if (gameData.players == null) {
        throw new Error("No players in the game");
    }

    for (let id of gameData.players) {
        let userFromRedisByUserId = await getUserFromRedisByUserId(redisClient, consul, id);

        const {
            id: userId, username, mmr,
        } = userFromRedisByUserId;

        const transformedUser = {id: userId, username, mmr, type: "Player"};
        gamePlayers.push(transformedUser);
    }

    gameData.bots.forEach(bot => {
        gamePlayers.push(bot);
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
        currentIndex = currentGamePlayers.findIndex(bot => bot.username === currentUser.username);
    } else {
        currentIndex = currentGamePlayers.findIndex(player => player.id == currentUser.UserId);
    }

    let nextPlayerIndex = setNextPlayerId(currentIndex, currentGamePlayers);
    let nextPlayer = currentGamePlayers[nextPlayerIndex];

    await setCurrentGamePlayer(redisClient, gameUUID, nextPlayer);

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
}

async function getGameUUIDByGameWS(redisClient, wsId) {
    return await redisClient.hGet(`GameWS:${wsId}`, `gameUUID`);
}

async function removeTimer(redisClient, gameUUID) {
    await redisClient.del(`Game:${gameUUID}:Timer`, (err) => {
        if (err) console.error(`Failed to delete key Game:${gameUUID}:Timer`);
    });
}

async function setTimer(rabbitMQChannel, redisClient, gameUUID, duration = 30) {
    const exchangeName = "game_events";
    const message = JSON.stringify({
        gameUUID: gameUUID,
        ttl: duration * 1000
    });

    await rabbitMQChannel.publish(exchangeName, "game.timer.set", Buffer.from(message));
    await rabbitMQChannel.bindQueue("game_timer", exchangeName, "game.timer.set");

    await redisClient.hSet(`Game:${gameUUID}:TimerStart`, "Time", Date.now().toString());
}

async function getRemainingTime(redisClient, gameUUID, initialDuration = 30) {
    const timerStart = await redisClient.hGet(`Game:${gameUUID}:TimerStart`, "Time");

    if(timerStart === 0) return 0;

    if (!timerStart) return initialDuration;

    const elapsedTime = (Date.now() - timerStart) / 1000;
    const remainingTime = initialDuration - elapsedTime;

    return remainingTime > 0 ? remainingTime : 0;
}

async function cancelTimer(redisClient, rabbitMQChannel, gameUUID) {
    const message = await rabbitMQChannel.get("game_timer");

    if (message) {
        const content = JSON.parse(message.content.toString());

        if (content.gameUUID === gameUUID) {
            rabbitMQChannel.nack(message, false, true);
            await redisClient.hSet(`Game:${gameUUID}:TimerStart`, "Time", 0);
        } else {
            rabbitMQChannel.ack(message);
        }
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

    await redisClient.hSet(`Game:${gameUUID}`, 'LobbyData', JSON.stringify(lobbyData));
}

module.exports = {
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