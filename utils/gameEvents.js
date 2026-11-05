// Event log of a game, kept in Redis while the game runs and saved as a replay when it is completed
// (see endGameHandler). Every entry has a type and a timestamp `t` (ms); players are referenced as
// {type: 'Player' | 'Bot', id}. Types:
//   start   {players}                       - seats in turn order
//   turn    {player}                        - the turn passes to player
//   open    {player, bubbleId, bubbleImg}   - a bubble is opened
//   match   {player, bubbles: [id, id]}     - the two open bubbles are a pair (player keeps the turn)
//   miss    {player, bubbles: [id, id]}     - not a pair; the bubbles close again
//   timeout {player}                        - the turn timer ran out
//   pause / resume {player}                 - a player paused or resumed the game
//   leave   {player}                        - a player left the game early
//   replace {player, by}                    - a bot takes the seat of a player who left
//   debug   {player}                        - "debug end": every following turn is played automatically
//   chat    {player, message}
//   end     {reason}                        - 'completed' (all pairs found) or 'abandoned'
const eventsKey = uuid => `Game:${uuid}:Events`;
const EVENTS_TTL_SECONDS = 6 * 60 * 60;

function playerRef(player) {
    return {type: player.type, id: player.id};
}

async function recordEvent(redisClient, uuid, event, t = Date.now()) {
    const length = await redisClient.rPush(eventsKey(uuid), JSON.stringify({...event, t}));
    // The log must not outlive an abandoned game; completed games shorten it in endGame.
    if (length === 1) await redisClient.expire(eventsKey(uuid), EVENTS_TTL_SECONDS);
}

async function readEvents(redisClient, uuid) {
    return (await redisClient.lRange(eventsKey(uuid), 0, -1)).map(event => JSON.parse(event));
}

module.exports = {eventsKey, playerRef, recordEvent, readEvents};
