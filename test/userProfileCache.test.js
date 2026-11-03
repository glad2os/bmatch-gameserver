const {test} = require('node:test');
const assert = require('node:assert/strict');
const {Pool} = require('pg');
const {getCachedUserProfile} = require('../utils/userProfileCache');
test('cache stores only safe profile fields under its own key with one-hour expiry', async () => {
    const consul = {catalog: {service: {nodes: async () => [{ServiceAddress: 'postgres', ServicePort: 5432}]}}};
    const saved = new Map();
    let calls = 0;
    Pool.prototype.query = async (sql, args) => {
        calls++;
        assert.equal(sql, 'SELECT id, username, mmr FROM users WHERE id = $1');
        return {rows: args[0] === 1 ? [{id: 1, username: 'user', mmr: 2000, password: 'must-not-be-cached'}] : []};
    };
    const redis = {
        get: async key => saved.get(key),
        set: async (key, value, options) => {
            assert.equal(key, 'gameserver:user:1');
            assert.deepEqual(options, {EX: 3600});
            saved.set(key, value);
        }
    };
    assert.deepEqual(await getCachedUserProfile(redis, consul, 1), {id: 1, username: 'user', mmr: 2000});
    assert.deepEqual(await getCachedUserProfile(redis, consul, 1), {id: 1, username: 'user', mmr: 2000});
    assert.equal(calls, 1);
    assert.equal(saved.get('gameserver:user:1').includes('password'), false);
    await assert.rejects(getCachedUserProfile(redis, consul, 2), /User not found/);
});
