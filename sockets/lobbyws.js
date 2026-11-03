const {verify} = require("jsonwebtoken");
const {JWTToken, getPostgresConfig} = require("../config/config");
const {fetchUserById, setStatusById, getStatusById} = require("../utils/getPostgresqlUserData");
const {v4: uuidv4} = require('uuid');
const {generateArea} = require('../gameLogic/areaGenerator')
const {
    extractAndVerifyJWT,
    emitSystemMessage,
    fetchLobbyDataAndPlayers,
    emitPlayerListToLobby,
    checkLobbyProperties,
    setLobbyPlayersData,
    setUserSocket,
    getLobbyFromWsSocket,
    getLobbyUUID
} = require("../utils/getLobbyData");


function join(io, socket, consul, redisClient) {
    socket.on('join', async (data) => {
        let lobbyId = data.lobbyID;

        if (lobbyId === undefined) {
            return;
        }

        socket.join(lobbyId);

        try {
            let cfg = await getPostgresConfig(consul);
            let lobbyData = await fetchLobbyDataAndPlayers(redisClient, lobbyId);

            let lobbyUUID = await getLobbyUUID(redisClient, lobbyId);

            if (lobbyUUID !== null) {
                io.to(lobbyId).emit("gameUUID", {
                    uuid: JSON.parse(lobbyUUID).uuid
                });
            }

            await setUserSocket(redisClient, socket.id, lobbyId)

            if (data.type === "Bot") {
                const admin = await extractAndVerifyJWT(data.token, consul, redisClient);
                if (!lobbyData.players.some(player => player.id === admin.id && player.type === "Admin")) {
                    throw new Error("Only the lobby admin can add bots");
                }
                if (lobbyData.players.length < 4) {
                    let lobbyProperties = checkLobbyProperties(lobbyData.players);
                    let botId = lobbyProperties.bots.length;

                    lobbyData.players.push({
                        id: `bot-${botId}`,
                        mmr: 0,
                        username: `Bot${botId}`,
                        type: "Bot",
                        lastSuccessfulAttempt: 0
                    })

                    await setLobbyPlayersData(redisClient, lobbyId, lobbyData.players)
                    await emitPlayerListToLobby(io, redisClient, lobbyId);
                    return;
                }
                throw new Error("Lobby is full");
            }

            let currentUser = await extractAndVerifyJWT(data.token, consul, redisClient);

            if (!currentUser.id) {
                throw new Error("Not able to parse user-id");
            }

            let currentStatus = await getStatusById(currentUser.id, cfg);

            if (currentStatus === "IN_GAME") {
                const uuid = await redisClient.get(`UserGame:${currentUser.id}`);
                if (uuid && await redisClient.exists(`Game:${uuid}`) &&
                    !await redisClient.hGet(`Game:${uuid}`, 'GameOver')) {
                    throw new Error(`The user is already in Game: ${uuid}`);
                }
                await setStatusById(currentUser.id, "REGISTERED", cfg);
            }

            const existingPlayerIndex = lobbyData.players.findIndex(player => player.id === currentUser.id);

            if (existingPlayerIndex === -1) {
                if (lobbyData.players.length >= 4) throw new Error("Lobby is full");
                let playerType = lobbyId.split('#')[0] === currentUser.username ? "Admin" : "Player";

                lobbyData.players.push({
                    id: currentUser.id,
                    mmr: currentUser.mmr,
                    username: currentUser.username,
                    type: playerType,
                    ws: socket.id
                });
            } else {
                let oldWebsocketId = lobbyData.players[existingPlayerIndex].ws;
                io.to(oldWebsocketId).emit("userExist");
                await redisClient.del(`Socket:${oldWebsocketId}`);
                lobbyData.players[existingPlayerIndex].ws = socket.id;
            }

            await setLobbyPlayersData(redisClient, lobbyId, lobbyData.players)
            await setUserSocket(redisClient, socket.id, lobbyId)
            await emitPlayerListToLobby(io, redisClient, lobbyId);
        } catch (ex) {
            emitSystemMessage(io, socket, ex.message);
        }
    });
}

function disconnectLobby(io, socket, redisClient) {
    socket.on('disconnect', async () => {
        try {
            let lobbyId = await getLobbyFromWsSocket(socket.id, redisClient);
            if (lobbyId == null) return;

            let lobbyData = await fetchLobbyDataAndPlayers(redisClient, lobbyId);
            let excludeCurrentSocketId = lobbyData.players.filter(player => player.ws !== socket.id);

            let lobbyProperties = checkLobbyProperties(excludeCurrentSocketId);

            if (!lobbyProperties.hasPlayers || !lobbyProperties.hasNoRealPlayersOrAdmin || lobbyProperties.hasNoAdmin) {
                io.to(lobbyId).emit('lobbyRemoved');
                await redisClient.del(`Lobby:${lobbyId}`);
            } else {
                await setLobbyPlayersData(redisClient, lobbyId, excludeCurrentSocketId)
                await emitPlayerListToLobby(io, redisClient, lobbyId);
            }

            await redisClient.del(`Socket:${socket.id}`);
        } catch (err) {
            console.error('Error handling disconnect:', err);
        }
    });
}

function generateGame(io, socket, redisClient, consul) {
    socket.on('generateGame', async (data) => {
        try {
            let currentUser = await extractAndVerifyJWT(data.token, consul, redisClient);
            let lobbyId = await getLobbyFromWsSocket(socket.id, redisClient);

            if (lobbyId == null) {
                throw new Error("Not able to parse lobby-id");
            }

            let lobbyData = await fetchLobbyDataAndPlayers(redisClient, lobbyId);

            let existingPlayerIndex = lobbyData.players.findIndex(player => player.id === currentUser.id);

            if (existingPlayerIndex === -1) {
                throw new Error("You are not a player of the lobby");
            }

            let lobbyProperties = checkLobbyProperties(lobbyData.players);

            if (!lobbyProperties.isFull) {
                throw new Error("In the lobby supposed to be only 4 players")
            }

            let lobbyUUID = await getLobbyUUID(redisClient, lobbyId);

            if (lobbyUUID !== null) {
                throw new Error("Game is already created");
            }

            const existingUUID = await redisClient.hGet(`Lobby:${lobbyId}`, 'UUID');

            if (existingUUID) {
                io.to(lobbyId).emit("gameUUID", {
                    uuid: existingUUID
                });

                return;
            }

            let uuid = uuidv4();

            await redisClient.hSet(`Lobby:${lobbyId}`, 'UUID', JSON.stringify({
                uuid: uuid,
            }));

            await redisClient.hSet(`Game:${uuid}`, "LobbyData", JSON.stringify({
                players: lobbyProperties.realPlayersOrAdminIds,
                bots: lobbyProperties.bots,
                readyPlayers: 0,
                lobbyId: lobbyId
            }));

            await redisClient.hSet(`Game:${uuid}`, "GameArea", JSON.stringify(generateArea()));
            await redisClient.hSet(`Game:${uuid}`, `LastActionId`, 0);
            await redisClient.hSet(`Game:${uuid}`, `Action:0`, JSON.stringify({
                "openBubbles": [],
                "requestedBubbles": {},
                "sender": {},
                "serverTime": new Date().toISOString()
            }));

            await redisClient.hSet(`Game:${uuid}`, `GamePaused`, 'true');

            await redisClient.expire(`Game:${uuid}`, 6 * 60 * 60);

            const cfg = await getPostgresConfig(consul);

            for (const it of lobbyProperties.realPlayersOrAdminIds) {
                await redisClient.set(`UserGame:${it}`, uuid, {EX: 6 * 60 * 60});
                await setStatusById(it, "IN_GAME", cfg);
            }
            io.to(lobbyId).emit("gameUUID", {uuid});
        } catch (ex) {
            let data = JSON.stringify({
                message: ex.message
            });
            io.to(socket.id).emit("systemMessage", data);
        }
    });
}

module.exports = {joinLobby: join, disconnectLobby, generateGame}