const {getPostgresConfig} = require("../config/config");
const {fetchUserById} = require("./getPostgresqlUserData");

async function getCachedUserProfile(redisClient, consul, userId) {
    const key = `gameserver:user:${userId}`;
    const cached = await redisClient.get(key);
    if (cached) return JSON.parse(cached);
    const user = await fetchUserById(userId, await getPostgresConfig(consul));
    if (!user) throw new Error("User not found");
    const profile = {id: user.id, username: user.username, mmr: user.mmr};
    await redisClient.set(key, JSON.stringify(profile), {EX: 3600});
    return profile;
}

module.exports = {getCachedUserProfile};
