const { Client } = require('pg');

const fetchUserById = async (userId, config) => {
    const client = new Client(config);
    try {
        await client.connect();
        const res = await client.query('SELECT id, username, mmr FROM users WHERE id = $1', [userId]);
        return res.rows[0];
    } catch (err) {
        throw err;
    } finally {
        await client.end();
    }
};


const setStatusById = async (userId, status, config) => {
    const client = new Client(config);
    try {
        await client.connect();
        const queryText = 'UPDATE users SET status = $1 WHERE id = $2';
        await client.query(queryText, [status, userId]);
    } catch (err) {
        throw err;
    } finally {
        await client.end();
    }
};


const getStatusById = async (userId, config) => {
    const client = new Client(config);
    try {
        await client.connect();
        const queryText = 'SELECT status FROM users WHERE id = $1';
        const res = await client.query(queryText, [userId]);
        return res.rows[0].status;
    } catch (err) {
        throw err;
    } finally {
        await client.end();
    }
};

module.exports = {
    fetchUserById,
    setStatusById,
    getStatusById
};
