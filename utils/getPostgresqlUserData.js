const {Pool} = require('pg');
let pool;
function getPool(config) {
    if (!pool) {
        pool = new Pool(config);
        pool.on('error', error => console.error('PostgreSQL pool error:', error));
    }
    return pool;
}
async function fetchUserById(userId, config) {
    const result = await getPool(config).query('SELECT id, username, mmr FROM users WHERE id = $1', [userId]);
    return result.rows[0];
}
async function setStatusById(userId, status, config) {
    await getPool(config).query('UPDATE users SET status = $1 WHERE id = $2', [status, userId]);
}
async function getStatusById(userId, config) {
    const result = await getPool(config).query('SELECT status FROM users WHERE id = $1', [userId]);
    return result.rows[0]?.status ?? null;
}
module.exports = {getPool, fetchUserById, setStatusById, getStatusById};
