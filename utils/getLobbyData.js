const {getPostgresConfig, JWTToken} = require("../config/config");
const {getCachedUserProfile} = require("./userProfileCache");
const {verify} = require("jsonwebtoken");

async function extractAndVerifyJWT(jwt, consul, redisClient) {
    const verifyPlayer = verify(jwt, JWTToken);
    const userId = verifyPlayer.sub;

    return getCachedUserProfile(redisClient, consul, userId);
}

function checkLobbyProperties(players) {
    const hasPlayers = players.length > 0;
    const isFull = players.length === 4;
    const hasNoAdmin = players.every(player => player.type !== "Admin");
    const hasNoRealPlayersOrAdmin = players.every(player => player.type !== "Player" && player.type !== "Admin");
    const realPlayersOrAdminIds = players.filter(player => player.type === "Player" || player.type === "Admin").map(player => player.id);
    const bots = players.filter(player => player.type === "Bot");

    return {
        hasPlayers, bots, hasNoRealPlayersOrAdmin, hasNoAdmin, isFull, realPlayersOrAdminIds
    };
}

async function fetchLobbyDataAndPlayers(redisClient, lobbyID) {
    const lobbyData = await redisClient.hGetAll(`Lobby:${lobbyID}`);
    const players = lobbyData && lobbyData.Players ? JSON.parse(lobbyData.Players) : [];
    return {lobbyData, players};
}

async function setUserSocket(redisClient, socketId, lobbyID) {
    await redisClient.hSet(`Socket:${socketId}`, "lobbyID", lobbyID);
}

async function setLobbyPlayersData(redisClient, lobbyID, lobbyDataPlayers) {
    await redisClient.hSet(`Lobby:${lobbyID}`, "Players", JSON.stringify(lobbyDataPlayers));
    await redisClient.expire(`Lobby:${lobbyID}`, 3600);
}

async function getLobbyFromWsSocket(socketId, redisClient) {
    const lobbyID = await redisClient.hGet(`Socket:${socketId}`, "lobbyID");
    if (!lobbyID) return null;
    return lobbyID;
}

async function emitPlayerListToLobby(io, redisClient, lobbyID) {
    io.to(lobbyID).emit('playerList', await redisClient.hGetAll(`Lobby:${lobbyID}`));
}

function emitSystemMessage(io, socket, message) {
    const data = JSON.stringify({message});
    io.to(socket.id).emit("systemMessage", data);
}


async function getLobbyUUID(redisClient, lobbyId) {
    const existingUUID = await redisClient.hGet(`Lobby:${lobbyId}`, 'UUID');

    if (existingUUID) {
        return existingUUID;
    } else {
        return null;
    }
}

module.exports = {
    extractAndVerifyJWT,
    fetchLobbyDataAndPlayers,
    emitPlayerListToLobby,
    emitSystemMessage,
    checkLobbyProperties,
    setUserSocket,
    setLobbyPlayersData,
    getLobbyFromWsSocket,
    getLobbyUUID
}