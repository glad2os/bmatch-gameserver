const {setPaused, cancelTimer, getLobbyData} = require("./getGameData");
const {getPostgresConfig} = require("../config/config");
const {setStatusById} = require("./getPostgresqlUserData");

async function endGame(io, redisClient, rabbitMQChannel, consul, gameUUID) {
    const key = `Game:${gameUUID}`;
    if (!await redisClient.exists(key) || !await redisClient.hSetNX(key, 'GameOver', '1')) return;
    await setPaused(redisClient, gameUUID, 'true');
    await cancelTimer(redisClient, rabbitMQChannel, gameUUID);
    await redisClient.expire(key, 600);
    await redisClient.expire(`${key}:TimerStart`, 600);
    io.to(gameUUID).emit('gameOver');
    const lobby = await getLobbyData(redisClient, gameUUID);
    for (const id of lobby.players) {
        try {
            await setStatusById(id, "REGISTERED", await getPostgresConfig(consul));
        } catch (error) {
            console.error(`Failed to reset status for user ${id}:`, error);
        }
        // Do not remove a mapping created by a subsequent game.
        if (await redisClient.get(`UserGame:${id}`) === gameUUID) {
            await redisClient.del(`UserGame:${id}`);
        }
    }
}

module.exports = {endGame};
