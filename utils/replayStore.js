const {getPool} = require('./getPostgresqlUserData');

// Completed games, for replays and move analysis. `events` is the log described in gameEvents.js,
// `board` is the image number of each of the 100 bubbles (index = bubbleId).
const CREATE_REPLAYS_TABLE = `
    CREATE TABLE IF NOT EXISTS game_replays (
        game_uuid UUID PRIMARY KEY,
        lobby_id VARCHAR(100),
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ NOT NULL,
        players JSONB NOT NULL,
        board JSONB NOT NULL,
        events JSONB NOT NULL,
        summary JSONB NOT NULL,
        format_version SMALLINT NOT NULL DEFAULT 1
    )`;

// Debug games ("debug end": every turn played by the bot logic) are kept for statistics but can be
// filtered out of real-play analysis.
const ADD_DEBUG_COLUMN = `ALTER TABLE game_replays ADD COLUMN IF NOT EXISTS is_debug BOOLEAN NOT NULL DEFAULT false`;

// The table belongs to the game server; Postgres init scripts only run on an empty volume.
async function ensureReplayTable(config) {
    await getPool(config).query(CREATE_REPLAYS_TABLE);
    await getPool(config).query(ADD_DEBUG_COLUMN);
}

async function saveReplay(config, replay) {
    await getPool(config).query(
        `INSERT INTO game_replays (game_uuid, lobby_id, started_at, finished_at, players, board, events, summary, is_debug)
         VALUES ($1, $2, to_timestamp($3 / 1000.0), to_timestamp($4 / 1000.0), $5, $6, $7, $8, $9)
         ON CONFLICT (game_uuid) DO NOTHING`,
        [replay.gameUUID, replay.lobbyId, replay.startedAt, replay.finishedAt,
            JSON.stringify(replay.players), JSON.stringify(replay.board),
            JSON.stringify(replay.events), JSON.stringify(replay.summary), !!replay.debug]
    );
}

// Pairs found per player, from the event log; winners are the players with the most pairs.
function summarize(players, events) {
    const key = player => `${player.type}:${player.id}`;
    const pairs = Object.fromEntries(players.map(player => [key(player), 0]));
    for (const event of events) {
        if (event.type === 'match') pairs[key(event.player)] = (pairs[key(event.player)] ?? 0) + 1;
    }
    const best = Math.max(0, ...Object.values(pairs));
    return {
        pairs: players.map(player => ({player: {type: player.type, id: player.id}, pairs: pairs[key(player)]})),
        winners: best > 0 ? players.filter(player => pairs[key(player)] === best).map(player => ({type: player.type, id: player.id})) : []
    };
}

module.exports = {ensureReplayTable, saveReplay, summarize};
